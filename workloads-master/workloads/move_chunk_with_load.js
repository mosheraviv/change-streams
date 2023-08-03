/**
 * @file
 * Test performance of chunk migrations under load
 * <br>
 *
 * ### *Test*
 *
 *   Move one chunk, containing 25k documents, from one shard to another.
 *
 *   While one chunk migrating, benchRun is used to apply a mixed load onto the documents in the
 *   non-migrating chunks.
 *
 *   Throughput of the background load is also reported in metrics starting with "bg_". The goal
 *   of this test is basically for the migration speed to be as high as possible, while impacting
 *   the background load as little as possible.
 *
 *   Unless user specifies otherwise, uses default value for
 *   {@link https://docs.mongodb.com/manual/core/sharding-balancer-administration/index.html#chunk-migration-and-replication|_secondaryThrottle}
 *   (false for WiredTiger and true for MMAPv1), and
 *   {@link https://docs.mongodb.com/manual/tutorial/manage-sharded-cluster-balancer/#wait-for-delete|_waitForDelete}
 *   is set to true.
 *
 *   The test takes an optional 'thread_levels' parameters: an array specifying the number of threads to use for load.
 *
 *   Uses
 *   {@link https://docs.mongodb.com/manual/core/sharding-balancer-administration/#chunk-migration-procedure|moveChunk}
 *   command
 *
 *   Results are reported as docs moved / sec.
 *
 * ### *Setup*
 *
 *   Requires a sharded cluster.
 *
 *   Test inserts 100k documents, split evenly into one more chunk than there are shards. One chunk each is permanently moved to each of the
 *   shards. The last chunk is the one the test moves around. (Note that the test maxes out at five moves to avoid a large number of moves
 *   if the cluster has many shards.)
 *
 *   Field a is indexed, containing uniformly random numbers.
 *
 * ### *Notes*
 *
 *   The load that is here used only as background load was subsequently extracted into its own
 *   test in {@link module:workloads/mix}.
 *
 * ### *Owning-team*
 * mongodb/sharding
 *
 * ### *Keywords*
 *
 * @module workloads/move_chunk_with_load
 */

/* global db sharded sh printjson assert reportThroughput sleep */

var default_val = "default";

/**
 * The value for the _secondaryThrottle parameter to the moveChunk command.
 */
var secondary_throttle = secondary_throttle === undefined ? default_val : secondary_throttle;

/**
 * The value for the _waitForDelete parameter to the moveChunk command.
 */
var wait_for_delete = wait_for_delete === undefined ? default_val : wait_for_delete;


/**
 * The number of threads to run in parallel. The default is [0, 4, 64, 128, 256].
 * **Note:** Thread count should be divisible by 4.
 */
var thread_levels = thread_levels || [0, 4, 64, 128, 256];

(function () {

    if (!sharded()) {
        print("move_chunk_with_load will only run in a sharded cluster.\nSkip...");
        return;
    }

    sh.stopBalancer();

    var dbName = "mctest";
    var _db = db.getSiblingDB(dbName);
    var config = _db.getSiblingDB("config");
    var shards = config.shards.find().toArray();
    var numShards = shards.length;
    var shard_ids = [];
    shards.forEach(function (shard) {
        shard_ids.push(shard._id);
    });

    Random.setRandomSeed(0);

    var chunkSize = numShards < 5 ? 25000 : 1000; // 25K docs per shard, 1K if the number of shards is large
    var numberDoc = chunkSize * (numShards + 1);  // one chunk remains on each shard and one extra is moved around

    var longStr = "";
    for (var i = 0; i < 100; i++) {
        longStr += "a";
    }

    assert.commandWorked(_db.adminCommand({
        enableSharding: _db.toString()
    }));

    _db.adminCommand({
        movePrimary: _db.toString(),
        to: shard_ids[0]
    });

    function initColl(coll) {
        var shardConfig = {
            shardCollection: coll.getFullName(),
            key: {
                id: 1
            }
        };

        while (!_db.adminCommand(shardConfig).ok) {
            // wait for collection shard to finish
        }

        // Create n + 1 chunks (n = number of shards)
        for (i = 1; i <= numShards; i++) {
            sh.splitAt(coll.getFullName(), {
                id: chunkSize * i
            });
        }

        // Leave 2 chunks on shard 0 and move one each to the remaining shards.
        for (i = 1; i < numShards; i++) {
            idToMove = (chunkSize * (i + 1));
            assert.commandWorked(_db.adminCommand({
                moveChunk: coll.getFullName(),
                find: { id: idToMove },
                to: shard_ids[i]
            }));
        }

        // Insert a lot of documents evenly into the chunks.
        var bulk = coll.initializeUnorderedBulkOp();
        for (i = 0; i < numberDoc; i++) {
            bulk.insert({
                id: i,
                a: Random.randInt(1000000), // indexed
                c: longStr // not indexed
            });
        }
        bulk.execute();

        // Index a non-id key.
        coll.createIndex({
            a: 1
        });
    }

    // Move the chunk with id: 0 from its shard to the destination shard.
    function moveChunkTo(ns, toShard) {
        var d1 = Date.now();
        moveChunkCommand = {
            moveChunk: ns,
            find: { id: 0},
            to: toShard,
            _waitForDelete: true
        };
        // Use default secondaryThrottle unless it was explicitly specified in the config.
        if (secondary_throttle !== default_val) {
            moveChunkCommand["_secondaryThrottle"] = secondary_throttle;
        }

        var res = _db.adminCommand(moveChunkCommand);
        var d2 = Date.now();
        print("moveChunk result: " + tojson(res));
        assert(res.ok);
        return d2 - d1;
    }

    // Creates background load with BenchRun while the moveChunk is happening.
    function startLoad(ns, nThreads) {
        // We don't want to insert or remove from the chunk we're moving since that will change the
        // number of documents to move. We don't want to update the chunk we're moving either
        // because that makes the catchup phase never finish.
        var minWriteId = chunkSize;
        var benchRuns = [];

        var batchSize = 250; // number of documents per vectored insert
        var docSize = 100; // Document size + _id field

        // Make a document of slightly more than the given size.
        function makeDocument(docSize) {
            for (var i = 0; i < docSize; i++) {
                var doc = { id: minWriteId + Random.randInt(numberDoc - minWriteId),
                            a: Random.randInt(1000000),
                            c: "" };
                while(Object.bsonsize(doc) < docSize) {
                    doc.c += "x";
                }
                return doc;
            }
        }

        // Make the document array to insert
        var docs = [];
        for (var i = 0; i < batchSize; i++) {
            docs.push(makeDocument(docSize));
        }

        var ops = [{ op: "insert",
                      writeCmd: true,
                      ns: ns,
                      doc: docs },
                    { op: "remove",
                      writeCmd: true,
                      multi: true,
                      ns: ns,
                      query: { id: { "#RAND_INT" : [ minWriteId, numberDoc ]}}},
                    { op: "update",
                      writeCmd: true,
                      ns: ns,
                      query: { id: { "#RAND_INT" : [ minWriteId, numberDoc ]}},
                      update: { $inc: { a: 1 },
                                $set: { c: { "#RAND_STRING": [ { "#RAND_INT": [ 1, 100 ]}]}}}},
                    { op: "findOne",
                      readCmd: true,
                      ns: ns,
                      query: { id: { "#RAND_INT" : [ 0, numberDoc ]}}}];

        ops.forEach(function(op) {
            benchRuns.push(benchStart({ "ops": [op], "parallel": nThreads, "host": server, "username": username, "password": password }));
        });
        return benchRuns;
    }

    // Calls serverStatus with the appropriate arguments
    function getServerStatus(db) {
        return db.serverStatus({ rangeDeleter: 1,
                                 metrics: 0,
                                 tcmalloc: 0,
                                 sharding: 0,
                                 network: 0,
                                 connections: 0,
                                 asserts: 0,
                                 extra_info: 0 });
    }

    // Collects the serverStatus every 2 seconds until the provided CountDownLatch hits 0.
    function collectServerStatus(stopCounter) {
        var output = [];

        while (stopCounter.getCount() > 0) {
            output.push(db.serverStatus({ rangeDeleter: 1,
                                 metrics: 0,
                                 tcmalloc: 0,
                                 sharding: 0,
                                 network: 0,
                                 connections: 0,
                                 asserts: 0,
                                 extra_info: 0 }));
            sleep(2000);
        }
        return output;
    }

    function getTestName() {
        return "moveChunkWithLoad_secondaryThrottle_" + secondary_throttle;
    }

    // Takes 2 server status responses and makes an object with relevant throughputs between them.
    function getStatusDiffThroughput(status1, status2){
        var secDiff = (status2.localTime - status1.localTime) / 1000;
        var throughput = {
            time: status2.localTime,
            insert: (status2.opcounters.insert - status1.opcounters.insert) / secDiff,
            query: (status2.opcounters.query - status1.opcounters.query) / secDiff,
            update: (status2.opcounters.update - status1.opcounters.update) / secDiff,
            delete: (status2.opcounters.delete - status1.opcounters.delete) / secDiff,
            command: (status2.opcounters.command - status1.opcounters.command) / secDiff
        };
        return throughput;
    }

    /**
     * Converts results of background load serverStatus monitoring into two forms.
     * It creates a list of the throughputs at each time interval and then puts those into
     * a map from operation type to a list of throughputs.
     */
    function convertStatusResultsToThroughput(results) {
        var throughputs = [];
        var timeline = { time: [], insert: [], query: [], update: [], delete: [], command: [] };
        for(var i = 1; i < results.length; i++) {
            var throughput =  getStatusDiffThroughput(results[i-1], results[i]);
            throughputs.push(throughput);
            timeline.time.push(throughput.time);
            timeline.insert.push(throughput.insert);
            timeline.query.push(throughput.query);
            timeline.update.push(throughput.update);
            timeline.delete.push(throughput.delete);
            timeline.command.push(throughput.command);
        }
        // TODO:  This log could be useful, but the parser currently does not handle it properly.
        // When the parse is fixed, uncomment this line.
        // print("Throughputs: " + tojsononeline(throughputs));
        print("Timeline: " + tojson(timeline));
        return timeline;
    }

    function findMedian(values, description) {
        var median;

        values.sort(function(a,b){return a-b;});
        var half = Math.floor(values.length/2);
        if(values.length % 2) {
            median = values[half];
        } else {
            median = (values[half-1] + values[half]) / 2.0;
        }

        print(description + ": " + tojson(values));
        return median;
    }

    function testMoveChunkWithLoad(collName, nThreads) {
        var coll = _db[collName];
        initColl(coll);
        quiesceSystem();
        sleep(500);
        sh.status();  // check shard status before test run
        var nLogThreads = nThreads * 4;

        jsTest.log("Test moveChunk with\t " +
            " secondaryThrottle=" +  secondary_throttle +
            " nThreads=" + nLogThreads);

        var moveChunkEndCounter = new CountDownLatch(1);
        var serverStatusThread = new ScopedThread(collectServerStatus, moveChunkEndCounter);
        serverStatusThread.start();

        var benchRunTasks;
        if (nThreads > 0) {
            print("Starting load... ");
            benchRunTasks = startLoad(coll.getFullName(), nThreads);
        }

        // Let load start before collecting baseline.
        sleep(2000);

        // Record baseline throughput with load but no moveChunk.
        var beforeBaseStatus = getServerStatus(db);
        sleep(5000);
        var afterBaseStatus = getServerStatus(db);
        var bgBaseThroughput = getStatusDiffThroughput(beforeBaseStatus, afterBaseStatus);

        var bgInsertThroughputs = [];
        var bgQueryThroughputs = [];
        var throughputs = [];

        // The last move in this loop moves the chunk back to the first shard (rs0).
        // We limit number of moves in the case that we have many shards.
        maxMoves = numShards < 5 ? numShards : 5;
        for(var i = 1; i <= maxMoves; i++) {
            toShard = shard_ids[i % maxMoves];

            // Let the system settle after the moveChunk, logging the background throughput.
            var beforeBetweenStatus = getServerStatus(db);
            sleep(6000);
            var afterBetweenStatus = getServerStatus(db);
            var bgBetweenThroughput = getStatusDiffThroughput(beforeBetweenStatus,
                                                              afterBetweenStatus);
            jsTest.log("Background Throughput Before Migration: " + tojson(bgBetweenThroughput));

            // Perform the actual moveChunk, recording background throughput before and after.
            var beforeStatus = getServerStatus(db);
            print("Starting moveChunk... ");
            var moveChunkTime = moveChunkTo(coll.getFullName(), toShard);
            var afterStatus = getServerStatus(db);
            print("Ending moveChunk... ");

            var bgThroughput = getStatusDiffThroughput(beforeStatus, afterStatus);
            bgInsertThroughputs.push(bgThroughput.insert);
            bgQueryThroughputs.push(bgThroughput.query);
            throughputs.push(chunkSize * 1000 / moveChunkTime);

            jsTest.log("moveChunk to shard " + i%maxMoves + " takes " + moveChunkTime +
                " ms with\t secondaryThrottle=" + secondary_throttle +
                " nThreads=" + nThreads * 4);

            jsTest.log("Background Throughput During Chunk Migration: " + tojson(bgThroughput));

        }

        // Record throughput while load is still running but moveChunk is done.
        print("Ending Chunk Migrations... ");
        if (nThreads > 0) {
            sleep(6000);
            print("Ending benchRun... ");
            benchRunTasks.forEach(function(benchRunTask) {
                var benchResults = benchFinish(benchRunTask);
                print("BenchRun Results:  " + tojson(benchResults));
            });
        }

        // Record throughput when load is finished and moveChunk is done.
        sleep(4000);
        moveChunkEndCounter.countDown();
        serverStatusThread.join();
        var serverStatusData = serverStatusThread.returnData();
        convertStatusResultsToThroughput(serverStatusData);

        reportThroughput(getTestName(),
            findMedian(throughputs, "moveChunk throughputs"), {
                nThread: nLogThreads,
            });

        // We report our own background throughput and don't rely on benchRun's own results
        // because benchRun counts the throughput before and after the moveChunk as well.
        reportThroughput("bg_baseline_insert_" + getTestName(),
            bgBaseThroughput.insert, {
                nThread: nLogThreads,
            });
        reportThroughput("bg_baseline_query_" + getTestName(),
            bgBaseThroughput.query, {
                nThread: nLogThreads,
            });
        reportThroughput("bg_insert_" + getTestName(),
            findMedian(bgInsertThroughputs, "Background insert throughputs"), {
                nThread: nLogThreads,
            });
        reportThroughput("bg_query_" + getTestName(),
            findMedian(bgQueryThroughputs, "Background query throughputs"), {
                nThread: nLogThreads,
            });

        coll.drop();
    }

    function increaseMigrationWaitTimeout() {
        var configDB = db.getSiblingDB("config");
        var shards = configDB.shards.find().sort({_id: 1}).toArray();
        for (var i = 0; i < shards.length; i++) {
            var shard = shards[i];

            // Connect to each shard check binary version and increase the timeout.
            if (authEnabled){
                // Slice off the leading rs*/ from the list of members in the replica set
                shardHostParse = shard.host.slice(4);
                shardHostAuth = 'mongodb://'.concat(username, ':', password, '@', shardHostParse);
            } else {
                shardHostAuth = shard.host;
            }
            var mongo = new Mongo(shardHostAuth);
            var shardDB = mongo.getDB("admin");
            var buildInfo = shardDB.adminCommand({buildInfo: 1});
            if (buildInfo.versionArray[0] >= 5 || (buildInfo.versionArray[0] === 4 && buildInfo.versionArray[1] === 4)) {
                shardDB.adminCommand({setParameter: 1, receiveChunkWaitForRangeDeleterTimeoutMS: 3600000});
            }
        }
    }

    increaseMigrationWaitTimeout();

    thread_levels.forEach(function(nThreads) {
        var nThreadsNormalized = Math.floor(nThreads / 4);
        testMoveChunkWithLoad("move_chunk_throttle_" + secondary_throttle + " " + nThreads, nThreadsNormalized);
    });
})();
