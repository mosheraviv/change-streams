/**
 *
 * @file
 * Test and measure
 * {@link https://docs.mongodb.com/manual/core/replica-set-sync/#initial-sync|initial sync}
 * performance.
 *
 * ### *Test*
 *
 * This workload performs the following actions:
 *   - creates *num_dbs* databases. (We use 1 or 32.).
 *   - creates *num_collections* / database. (We use 1 or 32.).
 *   - inserts a fixed number of documents. (5,000,000 docs in total).
 *   - creates a single compound index for each of the collections.
 *   - apply a write work load while syncing if *write_load* is true.
 *
 * On completion of the steps, above, a third member is added to the replica set.
 *
 * The initialsync metric is the number of (docs synced) / second for the
 * duration of the intial sync. The sync is defined to have completed
 * when the third node reports that it has transitioned to 'SECONDARY' state.
 *
 * The other metrics (e.g cloneDBs) represent the phases of the initial sync.
 * These actual phases vary, depending on the MongoDB version, and are generally
 * only useful for debugging purposes. They are reported as negative values
 * (latency) and higher is considered better.
 *
 * Results are reported as docs synced per second.
 *
 * ### *Setup*
 *
 * The starting point for this test is a 2 node replica set:
 *   - one primary
 *   - one secondary.
 *
 * Once these 2 nodes are populated, an additional (empty) third data bearing node is
 * added to the replica set. This additional node is added with
 * {@link https://docs.mongodb.com/manual/reference/method/rs.add/#rs.add|rs.add()},
 * as it was configured with the same replSetName.
 *
 * See the
 * {@link https://docs.mongodb.com/manual/tutorial/expand-replica-set/#add-a-member-to-an-existing-replica-set|Add Members to a Replica Set}
 * tutorial for more details.
 *
 * ### *Notes*
 *
 *   * Insert 5M (*num_docs*) documents across *num_dbs* databases and *num_collections*
 *     collections.
 *   * Each document contains the following fields:
 *     - *_id*: an ObjectId
 *     - *name*: a string "Wile E. Coyote"
 *     - *age*: an int between 0 and 120
 *     - *i*: an int, from 0 to num_docs / ( num_dbs * num_collections)
 *     - *address.street*: the string "443 W 43rd St",
 *     - *address.zip_code*: a rand Int between 0 and 100000
 *     - *address.city*: the string "New York"
 *     - *random*: a rand Int between 0 and 10000000
 *     - *phone_no*: a string comprised of the concatenation of a rand Int between
 *       0 and 1000 with  "-" and  a rand Int between 0 and 10000
 *     - *long_string*: a string created by concatenating a rand Int between 0 and
 *        100000000 and the number of 'a' characters equal to the min value of string_field_size and
 *        1000.
 *     - *other_long_string*: an unindexed string created by concatenating a rand Int
 *        between 0 and the number of 'a' characters used as overflow for when
 *        string_field_size > 1024.
 *     - *str*: a string of 1K 'a's
 *     - *numericField*: a random integer numeric field in the range 0 to 100M -1.
 *
 * ### *Owning-team*
 * mongodb/replication
 *
 * ### *Keywords*
 *
 * @module workloads/initialsync
 */
/* global db sharded Random */
/* global rs print tojson assert  */
/* global reportThroughput sleep jsTest */
/* global ScopedThread load Mongo getPhaseData transposePhaseLogs */
/* global waitForStates waitOplogCheck CountDownLatch reportDurations ReplSetTest isReplSet */
load('utils/log_analysis.js');
load("libs/initialsync_sync_types_impl.js");

// Set defaults for externally declared parameters.
/**
 * The number of databases to create. The default is 1.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var num_dbs = num_dbs || 1;

/**
 * The number of collections to create. The default is 1.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var num_collections = num_collections || 1;

/**
 * This value controls whether a write load is applied while running the workload.
 * If set to true then 100,000 docs are inserted while the initial sync is in progress.
 * The default is false.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var write_load = write_load || false;

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

/**
 * This value tells whether or not to build indexes other than the _id index. The default is *true*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var build_user_indexes = typeof build_user_indexes === 'undefined' ? true : build_user_indexes;

/**
 * This value represents the number of documents inserted. The default is *5 million*.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var num_docs = num_docs || 5 * 1024 * 1024;

/**
 * This value represents the size of long_string to be created in each doc. The default is *1KB*.
 * When string_field_size > 1000, overflow is added onto other_long_string instead of long_string.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var string_field_size = string_field_size || 1024;

var db_name = "initsync";
var num_inserts_for_write_load = 100000;
var num_threads = 16;
var test_name =
    "initialsync_dbs_" + num_dbs + "_colls_" + num_collections + "_writeload_" +
    write_load;

Random.srand(341215145);

var long_string = {str: "a"};
var other_long_string = {str: "a"};
// Enforce field size maximum so indexes can be created on long_string.
while (Object.bsonsize(long_string) < Math.min(string_field_size, 1000)) {
    long_string.str += "a";
}
// Add overflow to unindexed field other_long_string to grow the sum of the sizes of long_string
// and other_long_string to string_field_size.
while ((Object.bsonsize(long_string) + Object.bsonsize(other_long_string)) < string_field_size) {
    other_long_string.str += "a";
}

// This function returns a document with randomized fields, and one field, 'i',
// that identifies the document.
var create_doc = function(i, long_string, other_long_string) {
    var doc = {
        "name": "Wile E. Coyote",
        "age": Random.randInt(120),
        "i": i,
        "address": {
            "street": "443 W 43rd St",
            "zip_code": Random.randInt(100000),
            "city": "New York",
        },
        "random": Random.randInt(10000000),
        "phone_no": Random.randInt(1000) + "-" + Random.randInt(10000),
        "long_string": Random.randInt(100000000) + long_string.str,
        "other_long_string": Random.randInt(100000000) + other_long_string.str
    };

    return doc;
};

// This function inserts documents into the specified database and collection.
// It inserts 'num_docs' documents starting at 'min_doc_idx' for the document's
// identifier. 'create_doc' is a function that takes a 'long_string' and an
// index and returns a document.
function insert_docs(db_name, num_colls, num_docs, min_doc_idx, create_doc,
                     long_string, other_long_string, tid) {
    var currDB = db.getSiblingDB(db_name);
    var max_doc_idx = min_doc_idx + num_docs;
    Random.srand(341215145 + tid);

    // This function creates a list of documents from 'min_doc_idx' to 'max_doc_idx'.
    var create_doc_list = function(min_doc_idx, max_doc_idx) {
        var docs = [];
        for (var j = min_doc_idx; j < max_doc_idx; j++) {
            docs.push(create_doc(j, long_string, other_long_string));
        }
        return docs;
    };

    for (var j = 0; j < num_colls; j++) {
        var coll_name = "coll" + j;

        // Go one batch past the last even 1000 to ensure we get all documents.
        for (var i = min_doc_idx; i < max_doc_idx + 1000; i += 1000) {
            // Make sure to never go over the actual maximum doc index.
            var docs = create_doc_list(i, Math.min(max_doc_idx, i + 1000));
            currDB[coll_name].insert(docs, {ordered: false});
        }
    }
}

// check that the number of docs and indexes are correct
var checkContents = function(_db) {
    var num_indexes = (build_user_indexes && string_field_size <= 1024) ? 6 : 1;
    var docs_per_collection = num_docs / (num_dbs * num_collections);
    for (var i = 0; i < num_dbs; i++) {
        var currDB_name = db_name + i;
        var currDB = _db.getSiblingDB(currDB_name);
        for (var j = 0; j < num_collections; j++) {
            var coll_name = "coll" + j;

            // Assert that there are the right number of documents.
            assert.eq(currDB[coll_name].count(), docs_per_collection);

            // Assert all indexes were created.
            assert.eq(currDB[coll_name].getIndexes().length, num_indexes);
        }
    }
};

var load_data = function() {
    // Adjust num_docs to account for roundoff
    var docs_per_collection = num_docs / (num_dbs * num_collections);
    var docs_per_thread = Math.floor(docs_per_collection / num_threads);
    docs_per_thread = (docs_per_thread == 0) ? 1 : docs_per_thread;
    num_docs = docs_per_thread * num_threads * num_collections * num_dbs;
    print("Adding data with ",
          num_dbs,
          " dbs, ",
          num_collections,
          " collections, ",
          num_docs,
          " total docs, each with string fields of size ",
          string_field_size);
    print(docs_per_collection + " docs per collection");

    for (var i = 0; i < num_dbs; i++) {
        var currDB_name = db_name + i;
        var currDB = db.getSiblingDB(currDB_name);
        currDB.dropDatabase();
        print("Loading data to database: " + currDB_name);

        var threads = [];
        for (var t = 0; t < num_threads; t++) {
            // Start threads for each block of doc ids.
            var new_thread = new ScopedThread(insert_docs,
                                              currDB_name,
                                              num_collections,
                                              docs_per_thread,
                                              docs_per_thread * t,
                                              create_doc,
                                              long_string,
                                              other_long_string,
                                              t);
            new_thread.start();
            threads.push(new_thread);
        }

        for (var t = 0; t < num_threads; t++) {
            threads[t].join();
        }

        if (build_user_indexes && string_field_size <= 1024) {
            print("Creating indexes");
            for (var j = 0; j < num_collections; j++) {
                var coll_name = "coll" + j;

                assert.commandWorked(
                    currDB.runCommand({
                        createIndexes: coll_name,
                        indexes: [
                            {key: {phone_no: 1}, name: "phone_no"},
                            {key: {i: 1}, name: "i"},
                            {key: {random: 1}, name: "random"},
                            {key: {age: 1, long_string: 1}, name: "age_long_string"},
                            {key: {"address.zip_code": 1}, name: "address_zip_code"}
                        ]
                    })
                );
            }
            print("Done creating indexes.");
        }
    }
    print("Done adding documents");
    checkContents(db);
    print("Done checking contents");
};

var writeLoad = function(inserts, dbname, collname, create_doc, long_string, other_long_string) {
    print("Write load started");
    // writeLoad is in a thread, so the random number generator must be
    // seeded.
    Random.srand(341215145 + inserts);
    var coll = db.getSiblingDB(dbname).getCollection(collname);
    for (var i = 0; i < inserts; i++) {
        assert.writeOK(coll.insert(create_doc(i, long_string, other_long_string)));
    }
    print("Write load finished: " + inserts + " inserts.");
    return true;
};

var startPhaseLogCollection = function(phaseLogs) {
    var logCollectionThread = null;
    var initialSyncEndCounter = new CountDownLatch(1);
    logCollectionThread =
        new ScopedThread(collectLogData, empty_node_addr, port, phaseLogs,
                         initialSyncEndCounter);
    logCollectionThread.start();
    return [logCollectionThread, initialSyncEndCounter];
};

var stopPhaseLogCollection = function(logCollectionThread, initialSyncEndCounter) {
    if (logCollectionThread) {
        initialSyncEndCounter.countDown();
        logCollectionThread.join();
        var phaseData = logCollectionThread.returnData();

        // Data comes back read-only so we need to copy it.
        var phaseDataCopy = JSON.parse(JSON.stringify(phaseData));
        reportDurations(phaseDataCopy, test_name);
    }
};

var collectLogData = function(empty_node_addr, port, phaseLogs, stop_counter) {
    load('utils/log_analysis.js');
    load('utils/mongoshell.js');

    var startPhaseLogs = transposePhaseLogs(phaseLogs, "start");
    var endPhaseLogs = transposePhaseLogs(phaseLogs, "end");
    var needs_refresh = true;
    var phaseData = {};

    while (stop_counter.getCount() > 0) {
        sleep(10 * 1000); // Sleep for 10 secs.
        try {
            // Reconnect to empty node to survive node restart during rsync.
            if (needs_refresh) {
                var _db = getAdminDB(hostWithPort(empty_node_addr, port));
            }
            var logs = _db.adminCommand({getLog: "global"}).log;
            needs_refresh = false;
        } catch (e) {
            needs_refresh = true;
            continue;
        }
        getPhaseData(startPhaseLogs, endPhaseLogs, logs, phaseData);
    }
    return phaseData;
};

var run_initialsync = function() {
    var empty_node_admin_db = getAdminDB(hostWithPort(empty_node_addr, port));
    var uses_3dot2 =
        empty_node_admin_db.adminCommand({getParameter: 1, use3dot2InitialSync: 1}).use3dot2InitialSync;

    var phaseLogs = {};
    if (uses_3dot2) {
        phaseLogs = {
            start: {start: 'initial sync pending',
                    end: 'initial sync drop all databases'},
            dropDBs: {start: 'initial sync drop all databases',
                      end: 'initial sync clone all databases'},
            cloneDBs: {start: 'initial sync clone all databases',
                       end: 'oplog sync 1 of 3'},
            oplog1: {start: 'oplog sync 1 of 3', end: 'oplog sync 2 of 3'},
            oplog2: {start: 'oplog sync 2 of 3',
                     end: 'initial sync building indexes'},
            buildIndexes: {start: 'initial sync building indexes',
                           end: 'oplog sync 3 of 3'},
            oplog3: {start: 'oplog sync 3 of 3',
                     end: 'initial sync finishing up'},
            finishing: {start: 'initial sync finishing up',
                        end: 'initial sync done'},
            compact: {start: 'initial sync done',
                      end: 'transition to SECONDARY'},
        };
    } else {
        empty_node_admin_db.adminCommand({setParameter: 1,
                          logComponentVerbosity: {verbosity: 0,
                                                  replication: {verbosity: 2}}});
        phaseLogs = {
            start: {start: 'drop all user databases',
                    end: 'Dropping the existing oplog'},
            dropDBs: {start: 'Dropping the existing oplog',
                      end: 'Creating the oplog'},
            cloneDBs: {start: 'Creating the oplog',
                       end: 'Finished cloning data'},
            oplog1: {
                // The missing 'a' is deliberate.
                start: ['pplying operations until', 'No need to apply operations.'],
                end: 'Initial sync attempt finishing up.'},
            finishing: {start: 'Initial sync attempt finishing up.',
                        end: 'initial sync done'},
            compact: {start: 'initial sync done',
                      end: 'transition to SECONDARY'},
            cloneOneDB: {start: 'Scheduling listCollections call for database',
                         end: '    database:'},
            createIndex: {start: 'Creating indexes for',
                          end: 'Done creating indexes for'},
        };
    }

    jsTestLog(test_name + " - using " + sync_type);
    // Sanity check to make sure the current status is NotYetInitialized (94).
    assert.commandFailedWithCode(empty_node_admin_db.adminCommand("replSetGetStatus"),
                                 ErrorCodes.NotYetInitialized,
                                 "Node should be in the uninitialized state");

    // If there is no write load expected, turn off no-op oplog writes also.
    if (!write_load) {
        assert.commandWorked(db.adminCommand({setParameter: 1, writePeriodicNoops: false}));
    }

    load_data();
    var writeLoadDB = db.getSiblingDB('writeLoad');

    var coll = writeLoadDB.insert_empty;
    quiesceSystem();
    // we need to call quiesce for empty_node_admin_db as it has not yet been added to the
    // replica set
    quiesceSystem(empty_node_admin_db);

    // Start phase log collection.
    var res = startPhaseLogCollection(phaseLogs);
    var logCollectionThread = res[0];
    var initialSyncEndCounter = res[1];

    // Start the write workload.
    var insertThread = null;
    if (write_load) {
        insertThread = new ScopedThread(writeLoad,
                                        num_inserts_for_write_load,
                                        writeLoadDB.getName(),
                                        coll.getName(),
                                        create_doc,
                                        long_string,
                                        other_long_string);
        insertThread.start();
    }

    // Reconfigure the cluster.
    var elapsed_time = addNodeAndWaitForInitialSyncFinish(empty_node_admin_db);

    // Returning throughput in documents per second.
    reportThroughput(test_name, (num_docs * 1000) / elapsed_time, {nThread: 1});
    if(insertThread) {
        insertThread.join();
        assert(insertThread.returnData(), "insertThread failed.");
    }

    // Stop phase log collection.
    stopPhaseLogCollection(logCollectionThread, initialSyncEndCounter);

    writeLoadDB.dropDatabase();

    // test complete / has passed, wait to allow for primary to notice
    // the third node (empty node) state.
    waitForStates(db, 60 * 1000, "PRIMARY", "SECONDARY");
};

// This method:
// 1. confirms that the third node(empty node) reports that it is not initialized
// 2. adds it to the set (on the primary)
// 3. blocks until the third node reports that it is in a secondary state
// 4. validates the content of the third node after it transition to SECONDARY state.
//
// returns the total time taken to complete initial sync.
var addNodeAndWaitForInitialSyncFinish = function(empty_node_admin_db) {
    var start_time = Date.now();

	if (sync_type == "rsync") {
        var start_time_post_rsync = doRsync(primary_addr, empty_node_addr, port, db_path, start_mongod);
        // Reconnect to empty node to survive node restart during rsync.
        empty_node_admin_db = getAdminDB(hostWithPort(empty_node_addr, port));
    }

    print("Adding last node");
    assert.commandWorked(rs.add(hostWithPort(empty_node_addr, port)));

    var start_time_prime = Date.now();
    print(tojson(rs.status()));

    // Initial sync is over when the server goes into a secondary state.
    // Wait up to 1 hour for this to happen.
    assert.soonNoExcept(function () {
        assert.commandWorked(empty_node_admin_db.adminCommand({
            replSetTest: 1,
            waitForMemberState: ReplSetTest.State.SECONDARY,
            timeoutMillis: 60 * 60 * 1000
        }));
        return true;
    }, "Initial sync failed to complete", 60 * 60 * 1000);

    var end_time = Date.now();
    print("Empty data node now successfully in SECONDARY state");

    print("Checking secondary contents");
    empty_node_admin_db.getMongo().setSlaveOk();
    checkContents(empty_node_admin_db);

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
    return elapsed_time;
};

var run_tests = function() {
    if (sharded()) {
        print("Workload 'initialsync' " +
              "is not configured to run in a sharded environment.\n");
        return;
    } else if (isReplSet()) {
        db.adminCommand({fsync: 1});
        run_initialsync();
    } else {
        print("Workload 'initialsync' " +
              "is not configured to run in a standalone environment.\n");
        return;
    }

    for (var i = 0; i < num_dbs; i++) {
        var currDB = db.getSiblingDB(db_name + i);
        currDB.dropDatabase();
    }
};

run_tests();
