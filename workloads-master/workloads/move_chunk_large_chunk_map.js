/**
 * @file
 * Test performance of each section of chunk migrations under load
 * <br>
 *
 * ### *Test*
 *
 *   Move one chunk, containing 25k documents, from one shard to another.
 *
 *   While one chunk migrating, benchRun is used to apply a mixed load onto the documents in the
 *   migrating chunk.
 *
 *   The goal of this test is to determine the time spent in each section of chunk
 *   migration as well as the impact of different sections of chunk migration on other
 *   commands.
 *
 *   Unless user specifies otherwise, uses default value for
 *   {@link
 * https://docs.mongodb.com/manual/core/sharding-balancer-administration/index.html#chunk-migration-and-replication|_secondaryThrottle}
 *   (false for WiredTiger and true for MMAPv1)
 *
 *   The test takes an optional 'threadLevels' parameters: an array specifying the number of
 * threads to use for load.
 *
 *   Uses
 *   {@link
 * https://docs.mongodb.com/manual/core/sharding-balancer-administration/#chunk-migration-procedure|moveChunk}
 *   command
 *
 *   Results are reported by dsi/dsi/libanalysis/move_chunk_log_analysis.
 *
 * ### *Setup*
 *
 *   Requires a sharded cluster.
 *
 *   Test inserts 25k documents in the first chunk (that is moved). There are also 500,000 empty
 * chunks. The chunk is moved 5 times per thread level.
 *
 *   Field a is indexed, containing uniformly random numbers.
 *
 * ### *Owning-team*
 * mongodb/sharding
 *
 * ### *Keywords*
 *
 * @module workloads/move_chunk_large_chunk_map
 */

/* global db sharded sh printjson assert reportThroughput sleep */

/**
 * The _secondaryThrottle parameter to the moveChunk command.
 */
var secondaryThrottle;

/**
 * The number of threads to run in parallel. The default is [0, 2, 4].
 * **Note:** Thread count should be divisible by 2, and this test cannot handle thread levels
 * greater than 4. Higher thread levels risk exceeding the 500MB memory limit for the transfer mods
 * queue and causing the chunk migration to fail.
 */
var threadLevels = threadLevels || [0, 2, 4];

/**
 * The number of chunks to create. The default is 500,000.
 */
var numChunks = numChunks || 500000;

/**
 * The major version of the server required to infer the FCV in use.
 */
 var majorVersion = db.version().split(".")[0];

(function() {

    if (!sharded()) {
        print("move_chunk_large_chunk_map will only run in a sharded cluster.\nSkip...");
        return;
    }

    assert(Array.isArray(threadLevels), "threadLevels variable must be an array");
    threadLevels.forEach(function(nThreads) {
        assert.eq(0, nThreads % 2, "Thread level must be divisible by 2, but got " + tojson(nThreads));
    });

    sh.stopBalancer();

    var dbName = "mctest";
    var _db = db.getSiblingDB(dbName);
    var config = _db.getSiblingDB("config");
    var numDocs = 25000;
    var shards = config.shards.find().toArray();
    var numShards = shards.length;
    var shardIds = [];
    shards.forEach(function(shard) {
        shardIds.push(shard._id);
    });

    /**
     * Create numChunks chunk entries to imitate calling the splitChunk command numChunks times. Each
     * chunk has an _id range of 1 starting at numDocs.
     */
    function initChunks(ns, numChunks) {

        var collUUID = config.collections.findOne({_id: ns}).uuid;
        var chunksUpdateQuery = majorVersion < 5 ? {ns: ns, min: {_id: MinKey}, max: {_id: MaxKey}} : {uuid: collUUID, min: {_id: MinKey}, max: {_id: MaxKey}};
        res = assert.commandWorked(config.chunks.update(chunksUpdateQuery, {$set: {max: {_id: numDocs}}}));

        var chunksFindQuery = majorVersion < 5 ? {ns: ns, min: {_id: MinKey}, max: {_id: numDocs}} : {uuid: collUUID, min: {_id: MinKey}, max: {_id: numDocs}};
        var chunk = config.chunks.findOne(chunksFindQuery);

        var bulkChunks = config.chunks.initializeUnorderedBulkOp();
        for (var i = 0; i < numChunks; i++) {
            var minKey = i + numDocs;
            var maxKey = (i === numChunks - 1) ? MaxKey : minKey + 1;
            var chunkEntry = majorVersion < 5 ? {
                "ns": ns,
                "min": {"_id": minKey},
                "max": {"_id": maxKey},
                "shard": chunk.shard,
                "lastmod": Timestamp(minKey, 0),
                "lastmodEpoch": chunk.lastmodEpoch,
                "history": [{"validAfter": Timestamp(0, 0), "shard": chunk.shard}]
            } : {
                "uuid": collUUID,
                "min": {"_id": minKey},
                "max": {"_id": maxKey},
                "shard": chunk.shard,
                "lastmod": Timestamp(minKey, 0),
                "history": [{"validAfter": Timestamp(0, 0), "shard": chunk.shard}]
            };
            bulkChunks.insert(chunkEntry);
        }
        assert.commandWorked(bulkChunks.execute());
    }

    /**
     * Add documents to the chunk that will be used.
     */
    function initColl(coll) {
        var shardConfig = {shardCollection: coll.getFullName(), key: {_id: 1}};
        while (!_db.adminCommand(shardConfig).ok) {
            // wait for collection shard to finish
        }

        // Insert a lot of documents into the chunk to be moved.
        Random.setRandomSeed(0);
        var longStr = "a".repeat(100);

        var bulk = coll.initializeUnorderedBulkOp();
        for (var i = 0; i < numDocs; i++) {
            bulk.insert({
                _id: i,
                a: Random.randInt(1000000),  // indexed
                c: longStr                   // not indexed
            });
        }
        assert.commandWorked(bulk.execute());

        // Index a non-_id key.
        assert.commandWorked(coll.createIndex({a: 1}));
    }

    /**
     * Move the chunk with _id: 0 from its shard to the destination shard.
     */
    function moveChunkTo(ns, toShard) {
        var d1 = Date.now();
        moveChunkCommand = {moveChunk: ns, find: {_id: 0}, to: toShard, _waitForDelete: true};
        // Use default secondaryThrottle unless it was explicitly specified in the config.
        if (secondaryThrottle !== undefined) {
            moveChunkCommand["_secondaryThrottle"] = secondaryThrottle;
        }

        var res = assert.commandWorked(_db.adminCommand(moveChunkCommand));
        var d2 = Date.now();
        print("moveChunk result: " + tojson(res));
        return d2 - d1;
    }

    /**
     * Creates background load with BenchRun while the moveChunk is happening.
     */
    function startLoad(ns, nThreads) {
        // Only test updates and finds since insert and remove would change
        // the number of documents being moved in the chunk migration.
        // All docs are on the migrating chunk, so all writes will go to that
        // chunk.
        var benchRuns = [];

        var ops = [
            {
                op: "update",
                writeCmd: true,
                ns: ns,
                query: {_id: {"#RAND_INT": [0, numDocs]}},
                update: {$inc: {a: 1}, $set: {c: {"#RAND_STRING": [{"#RAND_INT": [1, 100]}]}}}
            },
            {op: "findOne", readCmd: true, ns: ns, query: {_id: {"#RAND_INT": [0, numDocs]}}}
        ];

        ops.forEach(function(op) {
            benchRuns.push(benchStart({"ops": [op], "parallel": nThreads, "host": server, "username": username, "password": password}));
        });
        return benchRuns;
    }

    /**
     * MigrationPerf logs are debug, so need to explicitly enable them.
     */
    function updateVerbosityLevels() {
        var cmd = {setParameter: 1, logComponentVerbosity: {sharding: {migrationPerf: 2}}};
        assert(_db.getMongo().isMongos() === true);
        assert.commandWorked(_db.adminCommand({multicast: cmd}));
        assert.commandWorked(_db.adminCommand(cmd));
    }

    /**
     * Test moving chunk with nThreads running the background load.
     */
    function testMoveChunkWithLoad(collName, nThreads) {
        var coll = _db[collName];
        initColl(coll);
        initChunks(coll.getFullName(), numChunks - 1);

        quiesceSystem();
        sleep(500);
        var nLogThreads = nThreads * 2;

        updateVerbosityLevels();

        jsTest.log("Begin test");

        var benchRunTasks;
        if (nThreads > 0) {
            print("Starting load... ");
            benchRunTasks = startLoad(coll.getFullName(), nThreads);
            // Let load start before starting move.
            sleep(2000);
        }

        var nMoves = 5;
        for (var i = 1; i <= nMoves; i++) {
            toShard = shardIds[i % numShards];

            // Let the system settle after the moveChunk, logging the background throughput.
            sleep(6000);

            // Perform the actual moveChunk, recording background throughput before and after.
            print("Starting moveChunk... ");
            var moveChunkTime = moveChunkTo(coll.getFullName(), toShard);
            print("Ending moveChunk... ");

            jsTest.log("moveChunk to shard " + toShard + " takes " + moveChunkTime +
                    " ms with secondaryThrottle=" + tojson(secondaryThrottle) +
                    " nThreads=" + nLogThreads);
        }

        // End load.
        print("Ending Chunk Migrations... ");
        if (nThreads > 0) {
            sleep(6000);
            print("Ending benchRun... ");
            benchRunTasks.forEach(function(benchRunTask) {
                benchFinish(benchRunTask);
            });
        }

        // This log is used by the result parser to distinguish tests. JSON.stringify() is used over
        // tojson() because a single line JSON is easier for python to parse.
        var testEndTime = new Date();
        var testData = {
            "numChunks": numChunks,
            "secondaryThrottle": tojson(secondaryThrottle),
            "nThreads": nLogThreads,
            "time": testEndTime.toISOString()
        };
        jsTest.log("Test moveChunk with data: \t" + JSON.stringify(testData));

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

    function runTests() {
        assert.commandWorked(_db.adminCommand({enableSharding: _db.getName()}));

        assert.commandWorked(_db.adminCommand({movePrimary: _db.getName(), to: shardIds[0]}));

        threadLevels.forEach(function(nThreads) {
            var nThreadsNormalized = Math.floor(nThreads / 2);
            testMoveChunkWithLoad("move_chunk_threads_" + nThreads, nThreadsNormalized);
        });
    }

    increaseMigrationWaitTimeout();

    runTests();
})();
