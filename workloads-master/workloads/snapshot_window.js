/**
 * @file
 *
 * Compare performance of a writes + PIT reads workload using a dynamically versus statically
 * maintained available snapshot history window size, and different storage engine cache sizes.
 *
 * ### *Setup*
 *
 * Creates a collection and inserts 'nb_docs' over a period of time of
 * 'snpashot_window_size_in_seconds' seconds in order to establish an available snapshot history
 * window of max size. THe data documents are very large, so cache pressure can be built quickly via
 * the updates workload that will be run.
 *
 * ### *Test*
 *
 * Do the following workloads in parallel:
 *
 * * Continuous updates to the existing documents. This is done to build cache pressure as well as
 *   move the snapshot window forward in time so that it will dynamically adjust. The stable
 *   timestamp must move forward for the window between it and oldest timestamp to grow or shrink.
 *
 * * Point-in-time reads using atClusterTime between the latest timestamp and
 *   'maxTargetSnapshotHistoryWindowInSeconds'. These reads will remain open for a time, pinning the
 *   snapshot in cache until it finishes. This attempts to simulate snapshot transaction user
 *   requests.
 *
 * The test may run with a 'static_window' parameter. If this is true, then a failpoint will be
 * activated to prevent changes to 'targetSnapshotHistoryWindowInSeconds', which is the size of the
 * available snapshot history window that the system tries to maintain. Normally, the window size
 * adjusts dymanically in response to cache pressure and PIT reads failing with SnapshotTooOld
 * errors.
 *
 * The throughput measurements will be
 *     - successful PIT reads/sec
 *     - total successful PIT reads
 *     - total failed PIT reads
 *     - updates/sec
 *     - total updates
 *
 * ### *Owning-team*
 * mongodb/storage-execution
 *
 * ### *Keywords*
 *
 * @module workloads/snapshot_window
 */

/*global
 print reportThroughput sleep
*/

/**
 * The actual values in use for the following parameters are injected by run_workloads.py, which
 * gets it from a config file, dsi/configurations/test_control/test_control.snapshot_window.yml.
 *
 * Parameters:
 *    - reader_threads
 *    - writer_threads
 *    - static_window
 *    - nb_docs
 *    - server_cache_size_gb
 *    - snapshot_held_open_for_seconds
 *    - test_duration_secs
 */

/**
 * The number of parallel readers and writers, for the simultaneously running read and write
 * workloads.
 */
var writer_threads = writer_threads || 3;
var reader_threads = reader_threads || 20;

/**
 * Dictates whether the test is run with a statically or dynamically sized available snapshot
 * window.
 */
var static_window = static_window || false;

/**
 * The number of documents to insert into the collection during setup.
 */
var nb_docs = nb_docs || 20;

/**
 * Specifies the storage engine's cache size in GB.
 */
var server_cache_size_gb = server_cache_size_gb || 1;

/**
 * Each successful find operation will hold a snapshot open between 0 and
 * snapshot_held_open_for_seconds seconds by delaying the getMore. The specific value is randomized
 * since transcation will be held open for varying lengths of time.
 */
var snapshot_held_open_for_seconds = snapshot_held_open_for_seconds || 1;

/**
 * How long to run the workloads.
 */
var test_duration_secs = test_duration_secs || 60;

(function() {
    var dbName = "snapshot_window";
    var collName = "testcoll";
    var testDB = db.getSiblingDB(dbName);
    var coll = testDB.getCollection(collName);

    // We will use a batchSize of 2 and limit of 3 to provoke a single getMore in a find request.
    var batchSize = 2;
    var limit = 3;

    // We will generate large random strings to use in document inserts/updates.
    var listCharsString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var largeStringLength = 10000;

    // TODO: this can become configurable with PERF-1571.
    var maxSnapshotWindowSizeInSeconds = getParameter("maxTargetSnapshotHistoryWindowInSeconds");

    /**
     * Takes a server parameter 'field' and calls getParameter to retrieve the current setting of
     * that server parameter.
     */
    function getParameter(field) {
        var q = {getParameter: 1};
        q[field] = 1;

        var ret = assert.commandWorked(db.getSiblingDB("admin").runCommand(q));
        return ret[field];
    }

    /**
     * Calls setParameter to set server parameter 'field' to 'value'.
     */
    function setParameter(field, value) {
        var cmd = {setParameter: 1};
        cmd[field] = value;
        assert.commandWorked(db.getSiblingDB("admin").runCommand(cmd));
    }

    function genLargeString(size) {
        var large = "";
        for (var i = 0; i < size; ++i) {
            large += listCharsString.charAt(Math.floor(Math.random() *
                                            (listCharsString.length - 1)));
        }
        return large;
    }

    /**
     * Set up a collection and populate it with large documents over a time of
     * maxTargetSnapshotHistoryWindowInSeconds to fully populate the snapshot window. And adjust
     * runtime parameters according to the control settings -- e.g. server_cache_size_gb.
     */
    function setup() {
        coll.drop();
        quiesceSystem();

        setParameter("wiredTigerEngineRuntimeConfig", "cache_size=" + server_cache_size_gb + "GB");

        // Give the system a little more time to respond to cache pressure before allowing a dynamic
        // window size decrease.
        setParameter("minMillisBetweenSnapshotWindowDec", 1000);

        // Choose whether to set a failpoint to prevent dynamic target snapshot window adjustments.
        if (static_window) {
            testDB.adminCommand({
                configureFailPoint: "preventDynamicSnapshotHistoryWindowTargetAdjustments",
                mode: "alwaysOn"
            });
        } else {
            testDB.adminCommand({
                configureFailPoint: "preventDynamicSnapshotHistoryWindowTargetAdjustments",
                mode: "off"
            });
        }

        // Perform document inserts over a period of time 'maxSnapshotWindowSizeInSeconds' such that
        // the snapshot window can be fully established with a range of timestamps.
        var maxSnapshotWindowSizeMilliseconds = maxSnapshotWindowSizeInSeconds * 1000;
        var intervalMillis = maxSnapshotWindowSizeMilliseconds / nb_docs;

        for (var i = 0; i < nb_docs; ++i) {
            var t1 = new Date();
            var bigString = genLargeString(largeStringLength);
            var doc = {"_id": i, "a": bigString};
            var res = coll.insert(doc);
            assert.writeOK(res);
            var t2 = new Date();
            if (intervalMillis > (t2 - t1)) {
                sleep(intervalMillis - (t2 - t1));
            }
        }
    }

    /**
     * Starts a background atClusterTime snapshot reads workload on the collection.
     */
    function startAtClusterTimeReads(nThreads) {
        return benchStart({
            ops: [{op: "find",
                   query: {_id: {$gte: {"#RAND_INT": [0, nb_docs - 1 - limit]}}},
                   ns: coll.getFullName(),
                   batchSize: batchSize,
                   limit: limit,
                   readCmd: true,
                   handleError: true,  // SnapshotTooOld errors
                   // Use an atClusterTime somewhere in the max available snapshot history range.
                   // The actual window size can be smaller if the server is under heavy write load.
                   useAClusterTimeWithinPastSeconds: maxSnapshotWindowSizeInSeconds,
                   maxRandomMillisecondDelayBeforeGetMore: (snapshot_held_open_for_seconds*1000)}],
            useSessions: true,
            useSnapshotReads: true,
            // this will help space out find requests and prevent the server from getting hammered
            // with requests if the window gets too small.
            delayMillisOnFailedOperation: (snapshot_held_open_for_seconds * 1000),
            host: server,
            parallel: nThreads,
            username: username,
            password: password,
        });
    }

    /**
     * Starts background update workload on the collection. These occur outside of the session and
     * any transaction.
     */
    function startUpdates(nThreads) {
        return benchStart({
            ops: [{op: "update",
                   query: {"_id": {"#RAND_INT": [ 0, nb_docs - 1 ]}},
                   update: {"$set": {"a": {"#RAND_STRING": [largeStringLength]} }},
                   writeCmd: true,
                   ns: coll.getFullName(),
                 }],
            host: server,
            parallel: nThreads,
            username: username,
            password: password
        });
    }

    // Don't quiesce the system after setup() because we set up the cache in setup().
    setup();

    print("Running '" + reader_threads + "' reader threads and '" + writer_threads +
          "' writer threads, with a " + (static_window ? "statically" : "dynamically") +
          " sized available snapshot history window and cache size of '" + server_cache_size_gb +
          "' GB.");

    print("Starting background atClusterTime readers...");
    // Starting reads first since they will impact the system less than writes.
    var backgroundAtClusterTimeReadLoad = startAtClusterTimeReads(reader_threads,
                                                                  maxSnapshotWindowSizeInSeconds);

    print("Starting background update writers...");
    var backgroundUpdatesLoad = startUpdates(writer_threads);

    sleep(test_duration_secs * 1000);

    print("Stopping background atClusterTime readers...");
    var readRes = benchFinish(backgroundAtClusterTimeReadLoad);

    print("Stopping background update writers...");
    var writeRes = benchFinish(backgroundUpdatesLoad);

    reportThroughput("atCluster_reads_per_sec",
                     readRes["totalOps/s"],
                     {nThread: reader_threads});
    reportThroughput("total_atCluster_reads_succeeded",
                     readRes["queries"].toNumber(),
                     {nThread: reader_threads});
    reportThroughput("total_atCluster_reads_failed",
                     readRes["errCount"].toNumber(),
                     {nThread: reader_threads});
    reportThroughput("updates_per_sec", writeRes["totalOps/s"], {nThread: writer_threads});
    reportThroughput("total_updates", writeRes["updates"].toNumber(), {nThread: writer_threads});
})();
