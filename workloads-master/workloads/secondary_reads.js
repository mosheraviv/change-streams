/**
 * @file
 *
 * Test performance of secondary reads while applying oplogs
 *
 * ### *Setup*
 *
 * This workload should be run in a replica set of at least two nodes. The
 * IP addresses of the primary and one of the secondaries are specified by the
 * configuration.
 *
 * ### *Test*
 *
 * On an empty replica set, the workload performs the following actions:
 * - Setup stage:
 *   - Inserted *initial_nb_docs* documents into both the primary and the
 *   secondaries. This should be done before any writes and reads.
 * - Actual test stage:
 *   - *nWriters* background writers doing infinite writes on the primary which
 *   will replicate these writes to secondaries.
 *   - *nReaders* readers doing queries on a sepcified seconary. Each query has
 *   *batch_size* numbers of documents and only reads documents inserted in the
 *   setup stage in order to have more constant behavior all the time.
 *
 * Read and write throughputs are reported as docs / sec. Read latencies on
 * secondaries are reported as millis / ops. The workload also reports how
 * delayed (millis) the secondary is from the primary when the workload finishes
 * all the secondary reads.
 *
 * ### *Notes*
 *
 * - Default test time is 3 minutes.
 * - Default document size is 500 bytes.
 * - Default number of documents in each vectored insert is 1.
 * - Default number of initial documents is 10000.
 * - For production setting, the number of writer threads should be fixed and a
 *   varying number of reader threads is used.
 *
 * ### *Owning-team*
 * mongodb/replication
 *
 * ### *Keywords*
 *
 * @module workloads/secondary_reads.js
 */

/**
 * The actual values in use for the following parameters are injected by
 * run_workloads.py, which
 * gets it from config file.
 * see {@link
 * https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this
 * hello world example}.
 *
 * Parameters: reader_threads, writer_threads, primary, secondary,
 * test_duration_secs, batch_size,
 * document_size, prefix, initial_nb_docs
 */

/**
 * The number of threads to read from a specified secondary.
 */
var reader_threads = reader_threads || [1, 16, 32];

/**
 * The number of threads to write on primary.
 */
var writer_threads = writer_threads || [16];

/**
 * The primary IP address. The default is "10.2.0.100".
 *
 * The value can be changed as a parameter, see
 * {@link
 * https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this
 * hello world example}.
 */
var primary = primary || "10.2.0.100";

/**
 * The secondary IP address. The default is "10.2.0.101".
 *
 * The value can be changed as a parameter, see
 * {@link
 * https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this
 * hello world example}.
 */
var secondary = secondary || "10.2.0.101";

/**
 * The prefix to the name of the test.
 */
var prefix = prefix || "";

/**
 * The test duration in seconds per thread level.
 */
var test_duration_secs = test_duration_secs || 180;

/**
 * The number of documents for each vectored insert.
 */
var batch_size = batch_size || 1;

/**
 * The size of each document in bytes.
 */
var document_size = document_size || 500;

/**
 * The number of preexisting documents. Note readers will only query these
 * documents from the secondary.
 */
var initial_nb_docs = initial_nb_docs || 10000;

(function() {
    var dbName = "secondary_reads";
    var collName = "coll";
    var coll = db.getCollection(collName);
    var readBatchSize = 200;
    var readLimit = 100;
    var nWritersPrefix;
    // Create a vectored insert batch for benchStart
    var documentsToInsert = [];
    var nCharacters =
        document_size - Object.bsonsize({_id: ObjectId(), x: 0, str: "x"}) + 1;
    for (var i = 0; i < batch_size; i++) {
        documentsToInsert.push({
            x: {"#RAND_INT": [initial_nb_docs, 1000000000]},
            str: "x".repeat(nCharacters)
        });
    }

    /**
     * Insert *initial_nb_docs* documents into the database and make sure
     * secondaries have the documents.
     */
    function setup() {
        coll.drop();
        coll.createIndex({x: 1});

        print("Inserting " + initial_nb_docs + " documents into the database");
        var bulk = coll.initializeOrderedBulkOp();
        for (var i = 0; i < initial_nb_docs; i++) {
            bulk.insert({x: i, str: "x".repeat(nCharacters)});
        }
        var res = bulk.execute();
        assert.eq(res.nInserted, initial_nb_docs);
    }

    /**
     * Performs secondary reads on the collection and reports throughput.
     */
    function runSecondaryReads(nThreads) {
        var res = benchRun({
            ops: [{
                op: "find",
                readCmd: true,
                query: {
                    x: {$gte: {"#RAND_INT": [0, initial_nb_docs - readLimit]}}
                },
                ns: coll.getFullName(),
                batchSize: readBatchSize,
                limit: readLimit,
                readPrefMode: "secondaryPreferred",
            }],
            seconds: test_duration_secs,
            host: secondary,
            parallel: nThreads,
            username: username,
            password: password
        });
        reportThroughput(
            nWritersPrefix + prefix + "read_throughput",
            res["totalOps/s"] * readLimit, {nThread: nThreads});
        reportThroughput(
            nWritersPrefix + prefix + "reads_latency_ms",
            -res["queryLatencyAverageMicros"] / 1000.0, {nThread: nThreads});
    }

    /**
     * Starts infinite writes on the primary.
     */
    function startLoad(nThreads) {
        return benchStart({
            ops: [{
                op: "insert",
                ns: coll.getFullName(),
                doc: documentsToInsert,
                writeCmd: true
            }],
            host: primary,
            parallel: nThreads,
            username: username,
            password: password
        });
    }

    writer_threads.forEach(function(nWriters) {
        reader_threads.forEach(function(nReaders) {
            nWritersPrefix = nWriters + "writers:";
            setup();
            // Make sure all the initial documents are on secondaries.
            quiesceSystem();

            print("Starting " + nWriters + " background writers...");
            var writeLoad = startLoad(nWriters);

            // Make sure oplog appliers on secondaries are running.
            waitOplogCheck();

            print("Starting " + nReaders + " readers...");
            runSecondaryReads(nReaders);

            print("Stopping background writers...");
            var res = benchFinish(writeLoad);
            reportThroughput(
                nWritersPrefix + prefix + "write_throughput",
                res["totalOps/s"] * batch_size, {nThread: nReaders});

            // Also report how delayed the secondary is from the primary at this
            // point when workload finishes.
            var testFinishTs = new Date();
            waitOplogCheck();
            var replicationFinishTs = new Date();
            reportThroughput(
                nWritersPrefix + prefix + "secondary_lag_ms",
                -(replicationFinishTs - testFinishTs), {nThread: nReaders});
        });
    });
})();
