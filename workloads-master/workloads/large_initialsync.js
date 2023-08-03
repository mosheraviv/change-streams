/**
 *
 * @file
 *
 *   Test
 *   {@link https://docs.mongodb.com/manual/core/replica-set-sync/#initial-sync|initial sync}
 *   performance.
 *
 *   This test begins with a single node replica set (with a
 *   pre-seeded ebs volume for data path) and an empty data bearing node which
 *   has not yet been added to the replica set.
 *
 *   Once the empty node is added to the replicaset, the test tracks how long it
 *   takes for a full initial sync to complete.
 *
 *   Periodically, the initial sync progress is printed.
 *
 *   A simultaneous mongoreplay payload is applied to the replicaset in order to
 *   simulate a real world environment.
 *
 * ### *Test*
 *
 *   On start, the primary has been populated with an EBS volume (which has also
 *   been pre-warmed). An empty secondary is added to the replica set and the
 *   test is started.
 *
 *   The state of the secondary is tracked through the
 *   {@link https://docs.mongodb.com/manual/reference/command/replSetGetStatus/|replSetGetStatus}
 *   command.
 *
 *   The initial sync of the secondary will have completed (either successfully or not)
 *   when the stateStr of the syncing member is no longer
 *   {@link https://docs.mongodb.com/manual/reference/replica-states/#replstate.STARTUP2|STARTUP2}.
 *
 * Results are reported as duration of the initial sync (in milliseconds). It is reported as 
 *   a negative value (latency). As a result, higher values are better.
 *
 * ### *Setup*
 *
 * The starting point for this test is a single node replica set with a pre-seeded dbpath.
 *   - For {@link https://github.com/mongodb/mongo/blob/master/etc/system_perf.yml|initialsync-logkeeper-short} task, refer
 * {@link https://github.com/10gen/dsi/blob/master/configurations/infrastructure_provisioning/infrastructure_provisioning.initialsync-logkeeper-short.yml|infrastructure_provisioning.initialsync-logkeeper-short.yml}
 * for the ebs volume details.
 *
 *   - For {@link https://github.com/mongodb/mongo/blob/master/etc/system_perf.yml|initialsync-logkeeper} task, refer
 * {@link https://github.com/10gen/dsi/blob/master/configurations/infrastructure_provisioning/infrastructure_provisioning.initialsync-logkeeper.yml|infrastructure_provisioning.initialsync-logkeeper.yml}
 * for the ebs volume details.
 *
 * Before the test is started the ebs volume is warmed. See the *'WITH_EBS'* section
 * of
 * {@link https://github.com/10gen/dsi/blob/master/clusters/remote-scripts/system-setup.sh|system-setup.sh}
 *
 * Once the node has been populated and warmed, an additional (empty) data bearing node is
 * added to the replica set. This additional node must be configured the same
 * replSetName, if it is to be successfully added with
 * {@link https://docs.mongodb.com/manual/reference/method/rs.add/#rs.add|rs.add()},
 * as it was configured with .
 *
 * See the
 * {@link https://docs.mongodb.com/manual/tutorial/expand-replica-set/#add-a-member-to-an-existing-replica-set|Add Members to a Replica Set}
 * tutorial for  more details.
 *
 * {@link https://docs.mongodb.com/manual/reference/program/mongoreplay/|mongoreplay}
 * is used to apply a load to the primary while the initial sync is ongoing.
 *
 * ### *Notes*
 *
 *   * {@link https://github.com/mongodb/mongo/blob/master/etc/system_perf.yml|initialsync-logkeeper} task
 *     - Dataset contains one database named "buildlogs" and three collections.
 *     - "tests" collection has 355 million documents. Avgerage document size is 613 KB. Storage size 25 GB. 2 Indexes - "_id", "build_id_1_started_1".
 *     - "builds" collection has 4 million documents. Avgerage document size is 216 KB. Storage size 182 MB. 2 Indexes - "_id", "buildnum_1_builder_1".
 *     - "logs" collection has 71 million documents. Avgerage document size is 57 KB. Storage size 607 GB. 3 Indexes - "_id", "build_id_1_test_id_1_seq_1", "build_id_1_started_1".
 *   * {@link https://github.com/mongodb/mongo/blob/master/etc/system_perf.yml|initialsync-logkeeper-short} task
 *     - Uses pre-compressed data files (of a node that got restored from snapshot "snap-09d40a2412085bc5a") to load primary data.
 *     - The snapshot was taken out of {@link https://s3-us-west-2.amazon/usr/bin/aws.com/dsi-donot-remove/InitialSyncLogKeeper/logkeeper_slice.json.gz|this} dataset.
 *     - Dataset contains one database named "buildlogs" and one collection named "logs".
 *     - Collection has 1 million documents. Document size can range from few hundred bytes to few hundred kilobytes.
 *     - Total collection storage size is ~10GB. And, the collection has one index built out of "_id" field.
 *   * population of the replica set is performed with a pre-seeded EBS volume
 *   {@link https://github.com/10gen/dsi/blob/master/configurations/test_control/test_control.initialsync-logkeeper.yml|test_contrl yml file}.
 *   See the *on_workload_client* *pre_task* sections of this file.
 *   * {@link https://docs.mongodb.com/manual/reference/program/mongoreplay/|mongoreplay}
 *   is used to apply a pre-recorded workload on the primary while the initial sync is ongoing. Mongoreplay process is currently commented out
 *   and unlikely to ever return.
 *
 * ### *Owning-team*
 * mongodb/replication
 * 
 * ### *Keywords*
 * 
 * @module workloads/large_initialsync
 */
/* global db sharded Random */
/* global rs print printjson tojson assert  */
/* global reportThroughput sleep jsTest jsTestLog */
/* global ScopedThread load Mongo getPhaseData transposePhaseLogs */
/* global waitForStates waitOplogCheck CountDownLatch reportDurations ReplSetTest isReplSet */

load("libs/initialsync_sync_types_impl.js");

// Set defaults for externally declared parameters.
/**
 * The IP address of the primary node. The default is *10.2.0.190*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var primary_addr = primary_addr || "10.2.0.190";

/**
 * The port on which mongod process runs. The default is *27017*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var port = port || "27017";

/**
 * The IP address of an empty data bearing node that needs to sync data from the primary. The default is *10.2.0.200*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var empty_node_addr = empty_node_addr || "10.2.0.200";

/**
 * This value represents how an empty node should sync data from the primary. The default is *initialSync*.
 * Valid sync_type values are 'initialSync', 'rsync'.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var sync_type = sync_type || "initialSync";

/**
 * This tells how the mongod process has to be restarted in a workload. The default is *mongod  --config /tmp/mongo_port_27017.conf*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var start_mongod = start_mongod || "mongod  --config /tmp/mongo_port_27017.conf";

/**
 * This value represents the directory where the mongod instance stores its data. The default is *data/dbs*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var db_path = db_path || "data/dbs";
var test_name = "initialsync-logkeeper";
var keep_alive_needed = (sync_type != "initialSync");

var waitForInitialSyncFinish = function(empty_node_admin_db) {
    var initsync_done = false;
    var last_print_time = Date.now() - 60 * 10 * 1000 - 100;

    print("Waiting for secondary.");
    sleep(10 * 1000 ); // Wait for 10 seconds before we check status.

    while ( ! initsync_done ) {
        var status = undefined;
        try{
            status = empty_node_admin_db.adminCommand({replSetGetStatus: 1, initialSync: 1});
        }
        catch(err) {
            print("large_initialsync: replSetGetStatus error with [" +
                  err +
                  "]. This is informational only.");
        }

        if ( status ) {
            initsync_done = true;
            // Check if there is still member in STARTUP2.
            for (var m in status.members) {
                if (status.members[m].stateStr == "STARTUP2") {
                    initsync_done = false;
                }
            }

            var now = Date.now();
            if ( (now - last_print_time) > 60 * 10 * 1000) {
                // Print initialSyncStatus every 10 minutes
                if ( status.initialSyncStatus !== undefined ) {
                    printjson(status.initialSyncStatus);
                }

                last_print_time = now;
            }

            if ( initsync_done ) {
                printjson(status);
            }
        }

        if ( ! initsync_done ) {
            sleep(10 * 1000);  // check every 10 seconds
        }
    }
};

/**
 * Prints rsync progress.
 */
var printRsyncProgress = function(node_addr, db_path) {
    load('utils/mongoshell.js');
    var listDataDir =  "ls -la " + db_path;
    jsTestLog(Date() + ' : Listing data directory ' + db_path);
    runCmdOnTarget(node_addr, listDataDir);
};

var run_initialsync = function() {
    var empty_node_admin_db = getAdminDB(hostWithPort(empty_node_addr, port));

    jsTestLog(test_name + " - using " + sync_type);
    // Sanity check to make sure the current status is NotYetInitialized (94).
    assert.commandFailedWithCode(empty_node_admin_db.adminCommand("replSetGetStatus"),
                                 ErrorCodes.NotYetInitialized,
                                 "Node should be in the uninitialized state");

    quiesceSystem();
    // we need to call quiesce for empty_node_admin_db as it has not yet been added to the
    // replica set
    quiesceSystem(empty_node_admin_db);

    // Regular logkeeper workload takes around 8 hrs to sync data via rsync (w/ compression).
    // But {test_control.timeouts.no_output_ms} is set to 90 mins. So, in order to avoid the
    // test from getting timed-out, we start a keep-alive thread which prints rsync progress
    // for every 10 mins.
    // Note: When rsync is executed with -p (progress) via mongoshell, the progress is not
    // flushed to stdout properly. To overcome that, we are adding keep-alive thread to montior
    // rsync progress.
    var path_to_monitor = (sync_type == "rsync") ? getDbPathBkp(db_path) : db_path;
    var res = keepAliveStartifNeeded(keep_alive_needed, /* Is keep-alive thread needed? */
                                        printRsyncProgress, /* call-back func that prints keep-alive message */
                                        10 * 60 * 1000, /* Execute call-back function for every 10 mins */
                                        empty_node_addr, path_to_monitor); /* call-back args */
    var keep_alive_thread = res[0];
    var keep_alive_end_counter = res[1];

    var start_time = Date.now();

    if (sync_type == "rsync") {
        var start_time_post_rsync = doRsync(primary_addr, empty_node_addr, port, db_path, start_mongod);
        // Reconnect to empty node to survive node restart during rsync.
        empty_node_admin_db = getAdminDB(hostWithPort(empty_node_addr, port));
    }

    // Reconfigure the cluster.
    jsTestLog("Adding last node");
    assert.commandWorked(rs.add({host: hostWithPort(empty_node_addr, port), votes: 0, priority: 0}));

    var start_time_prime = Date.now();
    print(tojson(rs.status()));

    // If sync_type is not "rsync", then empty node copies data
    // from the primary node via initial sync after rs.add cmd.
    waitForInitialSyncFinish(empty_node_admin_db);
    var end_time = Date.now();

    // Stop keep-alive thread.
    keepAliveStop(keep_alive_thread, keep_alive_end_counter);

    var elapsed_time = end_time - start_time;

    var elapsed_time_prime;
    if (sync_type == "rsync")
        elapsed_time_prime = start_time_post_rsync - start_time;
    else
        elapsed_time_prime = end_time - start_time_prime;

    // "effectiveElapsedTimeSecs" includes only the data transfer time.
    // "totalElapsedTimeSecs" includes both the data transfer time and
    // time taken for pre/post data-transfer phase.
    jsTestLog("Test Result: " + tojson({
                testName: test_name,
                syncType: sync_type,
                totalElapsedTimeSecs: elapsed_time / 1000,
                effectiveElapsedTimeSecs: elapsed_time_prime / 1000
             }));

    printjson(empty_node_admin_db.adminCommand({replSetGetStatus: 1,
                                         initialSync: 1}).initialSyncStatus);
    reportThroughput(test_name, -1 * elapsed_time, {nThread: 1});

    // Test complete / has passed, wait to allow for primary to notice
    // the second node state.
    // Note: We run PSA (3-node replica) configuration for logkeeper-short task
    // and PS configuration (2-node replica) for logkeeper task.
    waitForStates(db, 60 * 1000, "PRIMARY", "SECONDARY", "ARBITER");
};


var run_tests = function() {
    if (sharded()) {
        print("Workload 'large_initialsync' is not configured to run in " +
              "a sharded environment.\n");
        return;
    } else if (isReplSet()) {
        db.adminCommand({fsync: 1});
        run_initialsync();
    } else {
        print("Workload 'large_initialsync' is not configured to run in a " +
              "standalone environment.\n");
        return;
    }
};

run_tests();
