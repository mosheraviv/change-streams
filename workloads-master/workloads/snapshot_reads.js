/**
 * @file
 *
 * Test performance of snapshot reads with and without background write load.
 *
 * ### *Setup*
 *
 * Populate a collection.
 *
 * ### *Test*
 *
 * Each thread performs a query of the form {_id: {$gte: <rand>}} with limit 100, batchSize 2, and
 * read concern level 'snapshot'. This is executed as a find and 49 getMores. This exercises the
 * stashing of locks and storage engine resources between find and getMore for snapshot reads. The
 * throughput is measured as ops/sec*100, to account for the fact that each operation finds 100
 * documents.
 *
 * The test may be run with a 'background_writes' parameter. If this is true, then background write
 * load is generated in the form of updates to random documents in the collection. This tests the
 * limits of cache usage while transactions are pinning snapshot history in cache. The write
 * throughput is additionally measured in ops/sec.
 *
 * ### *Notes*
 *
 * ### *Owning-team*
 * mongodb/storage-execution
 *
 * ### *Keywords*
 *
 * @module workloads/snapshot_reads
 */

/*global
 print reportThroughput sleep
*/

/**
 * The actual values in use for the following parameters are injected by run_workloads.py, which
 * gets it from a config file.
 * see {@link https://github.com/10gen/dsi/blob/def465728682f60d7e8f4086d7bf967bd7383e4b/configurations/test_control/test_control.snapshot_reads.yml#L13-L28|test_control.snapshot_reads.yml}.
 *
 * Parameters: thread_levels, background_writes, background_currentOp, useSnapshotReads, nb_docs, test_duration_secs
 */

/**
 * The number of threads to run in parallel.
 */
var thread_levels = thread_levels || [1, 32, 64];

/**
 * Run the test with background writes on.
 */
var background_writes = background_writes || false;

/**
 * Run the test with the currentOp command running in the background.
 */
var background_currentOp = background_currentOp || false;

/**
 * If true, reads are performed using read concern level 'snapshot'. useSnapshotReads=false is used
 * as a control.
 */
var useSnapshotReads = useSnapshotReads && true;

/**
 * The number of documents to insert into the collection during setup.
 */
var nb_docs = nb_docs || 6400;

/**
 * The test duration in seconds per thread level.
 */
var test_duration_secs = test_duration_secs || 180;

(function() {
    var dbName = "snapshot_reads";
    var collName = "coll";
    var testDB = db.getSiblingDB(dbName);
    var coll = testDB.getCollection(collName);
    var batchSize = 2;
    var limit = 100;

    /**
     * Populates the collection.
     */
    function setup() {
        coll.drop();
        var docs = [];
        for (var i = 0; i < nb_docs; i++) {
            docs.push({_id: i, x: 0});
        }
        coll.insert(docs);
    }

    /**
     * Performs snapshot reads on the collection and reports throughput.
     */
    function runSnapshotReads(nThreads, useSnapshotReads) {
        var res = benchRun({
            ops: [{op: "find",
                   query: {_id: {$gte: {"#RAND_INT": [0, nb_docs - limit]}}},
                   ns: coll.getFullName(),
                   batchSize: batchSize,
                   limit: limit,
                   readCmd: true}],
            useSessions: true,
            useSnapshotReads: useSnapshotReads,
            seconds: test_duration_secs,
            host: server,
            parallel: nThreads,
            username: username,
            password: password
        });
        var controlSuffix = useSnapshotReads ? "" : "_control";
        var backgroundWritesSuffix = background_writes ? "_with_load" : "";
        var backgroundCurrentOpSuffix = background_currentOp ? "_with_currentOp" : "";
        reportThroughput("snapshot_reads" + controlSuffix + backgroundWritesSuffix + backgroundCurrentOpSuffix,
                         res["totalOps/s"] * limit,
                         {nThread: nThreads});
    }

    /**
     * Starts background writes on the collection. These occur outside of the session.
     */
    function startWriteLoad(nThreads) {
        return benchStart({ops: [{op: "update",
                                  query: {_id: {"#RAND_INT_PLUS_THREAD": [0, 100]}},
                                  update: {$inc: {x: 1}},
                                  ns: coll.getFullName(),
                                  writeCmd: true}],
                           host: server,
                           parallel: nThreads,
                           username: username,
                           password: password});
    }

    /**
     * Starts background currentOp on the collection. These occur outside of the session.
     */
    function startCurrentOpLoad(nThreads) {
        return benchStart({ops: [{op: "command",
                                  ns: "admin",
                                  command: {currentOp: 1}}],
                           host: server,
                           parallel: nThreads,
                           username: username,
                           password: password});
    }

    thread_levels.forEach(function(nThreads) {
        print("Running thread level " + nThreads + ", background_writes: " + background_writes
              + ", background_currentOp: " + background_currentOp);

        print("Populating collection...");
        setup();

        var nReaders = nThreads;
        var nWriters = 0;
        var nCurrentOp = background_currentOp ? 1 : 0;
        if (background_writes) {
            // Split threads between readers and writers, but with at least one of each.
            if (nThreads === 1) {
                nReaders = 1;
                nWriters = 1;
            } else {
                nReaders = nThreads / 2;
                nWriters = nThreads / 2;
            }
        }

        var backgroundWriteLoad;
        if (background_writes) {
            print("Starting " + nWriters + " background writers...");
            backgroundWriteLoad = startWriteLoad(nWriters);
        }

        var backgroundCurrentOpLoad;
        if (background_currentOp) {
            print("Starting " + nCurrentOp + " background currentOp thread...");
            backgroundCurrentOpLoad = startCurrentOpLoad(nCurrentOp);
        }

        if (background_writes || background_currentOp) {
            // Let background load start before running snapshot reads.
            sleep(2000);
        }

        print("Starting " + nReaders + " readers...");
        runSnapshotReads(nReaders, useSnapshotReads);

        var controlSuffix = useSnapshotReads ? "" : "_control";
        if (background_currentOp) {
            print("Stopping background currentOp...");
            var res = benchFinish(backgroundCurrentOpLoad);
            reportThroughput("snapshot_reads" + controlSuffix + "_currentOp_throughput",
                             res["totalOps/s"],
                             {nThread: nCurrentOp});
        }

        if (background_writes) {
            print("Stopping background writers...");
            var res = benchFinish(backgroundWriteLoad);
            reportThroughput("snapshot_reads" + controlSuffix + "_load_throughput",
                             res["totalOps/s"],
                             {nThread: nWriters});
        }
    });
})();
