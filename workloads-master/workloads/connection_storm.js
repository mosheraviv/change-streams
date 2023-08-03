/**
 * connection_storm.js
 *
 * ### *Test*
 *   Spiky scatter gather reads on a sharded cluster.
 *   After initial base load, create a spike of new connections, and see if mongos releases them.
 *
 *   Origin as a repro for SERVER-28673.
 *
 *  Notes:
 * 13:38]
 * acm Am I correct that not all mongos to mongod communication flows through the ASIO connection pool?
 *
 * [13:39]
 * If so, what type of operations against mongos do flow through the ASIO connection pool, and which don't?
 *
 * [14:38]
 * jason.carey Just the read path
 *
 * [14:38]
 * Things that it asynchronous results merger
 *
 * [14:39]
 * acm So, scatter gather reads
 *
 * [14:39]
 * jason.carey Yep
 *
 * ### *Owning-team*
 *
 * ### *Keywords*
 *
 */

/* global db sharded sh printjson assert reportThroughput sleep toNumber */

(function () {

    var baseThreadCount = 100;  // Thread count during phase 1 and 3
    var spikeThreadCount = 1000; // Thread count (total) during phase 2
    var phase1Duration = 300; // seconds
    var phase2Duration = 120; // seconds
    var phase3Duration = 600; // seconds
    var docSize = 100; // Document size + _id field
    var numberDoc = 10000; // Number of documents to initialize the collection with
    var delay = 1; // seconds? Limits the rate of ops/sec, as we want a high nr of connections.

    var dbName = "storm";
    var collName = "stormColl";
    var _db = db.getSiblingDB(dbName);
    var coll = _db[collName];
    var ns = coll.getFullName();
    Random.setRandomSeed(42);

    // Make a document of slightly more than the given size.
    function makeDocument(docSize, minWriteId) {
        for (var i = 0; i < docSize; i++) {
            var doc = { id: minWriteId + Random.randInt(numberDoc - minWriteId),
                        a: Random.randInt(numberDoc/100),
                        c: "" };
            while(Object.bsonsize(doc) < docSize) {
                doc.c += "x";
            }
            return doc;
        }
    }


    // Run this once in the beginning
    if( sharded() ) {
        assert.commandWorked(_db.adminCommand({
            enableSharding: _db.toString()
        }));
    }

    // Run this in the beginning of each iteration of threads
    function initColl(coll) {
        coll.drop();
        // Index our id (note, not _id, not unique) and shard key
        coll.createIndex({
            id: 1
        });

        // Index a non-id key.
        coll.createIndex({
            a: 1
        });


        if( sharded() ) {
            print("Sharding collection.");
            // For hashed shard key, even a small number of chunks results in even distribution,
            // since docs will land randomly into them. (Maybe 3 or 6 would be enough, even?)
            var shardConfig = {
                shardCollection: coll.getFullName(),
                key: {
                    id: "hashed"
                },
                numInitialChunks: 96
            };
            while (!_db.adminCommand(shardConfig).ok) {
                print("Waiting for shardCollection to finish...");
                sleep(1000);
            }
            sh.status();
        }

        // Insert the initial set of numberDoc documents. Not measured for performance.
        var batchSize = 1000;
        for (i = 0; i < numberDoc/batchSize; i++) {
            var bulk = coll.initializeUnorderedBulkOp();
            for ( j = 0; j < batchSize && i*batchSize+j < numberDoc; j++ ) {
                bulk.insert(makeDocument(docSize, 0));
            }
            bulk.execute();
        }
    }

    function startLoad(nThreads, delay) {
        // benchRun limitation: it can only start 190 threads or so.
        // So we must start multiple instances

        var benchRunInstances = [];
        var benchRunOps = [{ op: "find",
                             readCmd: true,
                             ns: ns,
                             delay: delay,
                             query: { a: { "#RAND_INT" : [ 0, 100 ]}}}];

        var nInstances = Math.floor(nThreads / 100) + 1;
        for( var i = 0; i < nInstances-1; i++ ){
            benchRunInstances.push(benchStart({ "ops": benchRunOps,
                                                "parallel": 100,
                                                "host": server,
                                                "username": username,
                                                "password": password }));
        }
        // Last instance has the remainder of threads, to end up with an exact thread count
        if ( nThreads % 100 > 0 ) {
            benchRunInstances.push(benchStart({ "ops": benchRunOps,
                                                "parallel": nThreads%100,
                                                "host": server,
                                                "username": username,
                                                "password": password }));
        }
        return benchRunInstances;
    }

    function finishLoad(benchRunInstances) {
        var throughput = { findOne: 0, query:0, insert:0, update:0, delete:0, total:0, command:0};
        benchRunInstances.forEach(function(benchRunInstance) {
            var benchResults = benchFinish(benchRunInstance);
            throughput.findOne += toNumber(benchResults.findOne);
            throughput.query   += toNumber(benchResults.query);
            throughput.insert  += toNumber(benchResults.insert);
            throughput.update  += toNumber(benchResults.update);
            throughput.delete  += toNumber(benchResults.delete);
            throughput.command += toNumber(benchResults.command);
        });
        // Note that command is excluded from the total!
        throughput.total = throughput.findOne + throughput.query + throughput.insert
                         + throughput.update  + throughput.delete;
        return throughput;
    }

    function testConnectionStorm() {
        initColl(coll);
        quiesceSystem();
        sleep(500);
        if(sharded()){
            sh.status();  // check shard status before test run
        }

        jsTest.log("Connection Storm, phase 1: nThreads=" + baseThreadCount);
        var benchRunBaseLoadInstances = startLoad(baseThreadCount, delay);
        sleep(phase1Duration*1000);

        jsTest.log("Connection Storm, phase 2: nThreads=" + (spikeThreadCount));
        var benchRunSpikeLoadInstances = startLoad(spikeThreadCount, delay);
        sleep(phase2Duration*1000);
        var spikeResults = finishLoad(benchRunSpikeLoadInstances);
        print("BenchRun Results for spike load:  " + tojson(spikeResults));
        // FIXME: Queries/sec is completely uninteresting as for the purpose of this test.
        // To actually run in production, we would have to poll serverStatus to get nr of
        // connections, then report that.
        reportThroughput("storm_spike", spikeResults.query, {nThread: spikeThreadCount});

        jsTest.log("Connection Storm, phase 3: nThreads=" + (baseThreadCount));

        sleep(phase3Duration*1000);
        var baseResults = finishLoad(benchRunBaseLoadInstances);
        print("BenchRun Results for base load:  " + tojson(baseResults));
        reportThroughput("storm_base", baseResults.query, {nThread: baseThreadCount});
    }

    testConnectionStorm();
})();
