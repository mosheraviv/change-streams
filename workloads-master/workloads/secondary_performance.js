/**
 *
 * @file
 *
 * This workload is designed to capture replication throughput and provide
 * a comparison to the primary throughput. It is solely converned with the pace of
 * replication of a secondary process. As a result, it specifically uses a 2 node PS
 * (**1 Primary**, **1 Secondary**) replica set configuration.
 *
 * ### *Test*
 *
 * On an empty 2 node PS replica set, this workload performs the following actions:
 * - Pause replication.
 * - Insert a fixed number of documents (on the primary only as replication is paused).
 * - Note the duration required to insert all the documents (in seconds)
 *   as the primary duration.
 * - Perform a blocking write in a separate thread.
 * - Unpause replication.
 * - Wait for the blocking write thread to complete.
 * - Note the duration of the blocking write as the secondary duration (in seconds).
 *
 * Results are the primary and secondary throughputs. They are calculated from
 * *count* / *duration* docs per second.
 *
 * ### *Setup*
 *
 * The starting point for this test is a 2 node replica set:
 *   - one primary
 *   - one secondary.
 *
 * To begin the test:
 *   - a single document is inserted to ensure the collection exists on all processes.
 *   - all the replica set members are quiesced.
 *   - replication is stopped on the secondary.
 *
 * The {@link https://docs.mongodb.com/manual/reference/command/fsync/|fsync} command is used
 * to flush all writes on the primary.
 *
 * An insertion workload is written to the primary using each of the *thread_levels* as the num
 * insert threads.
 * Each thread inserts a total of  *count* / *num insert threads* documents in a
 * {@link https://docs.mongodb.com/manual/reference/method/Bulk/#unordered-operations|UnorderedBulkOp}
 * of up to a max of 1000 documents per batch.
 *
 * The documents consists of:
 *   - *_id*: an {@link https://docs.mongodb.com/manual/reference/method/ObjectId/|ObjectId}
 *   - *s*: a string of of length *size* + 1 (*size* varies from 1 to 10000)
 *
 * Next a write is performed with a
 * {@link https://docs.mongodb.com/manual/reference/write-concern/index.html#w-option|write concern}
 * of \{ w: all \} is performed in a separate thread.
 * Replication is resumed and the duration of the write thread is timed.
 *
 * ### *Notes*
 *
 * Currently, the externally supplied thread_levels are [32] for wiredTiger and [10] for MMAPv1.

 * For each thread level as num insert threads:
 *   * Insert *count* documents using *num insert threads* threads.
 *   * Each document contains the following fields:
 *     - *_id*: an ObjectId
 *     - *s*: a string of *size* + 1 bytes
 *   * Roughly speaking each document will be 30 + *size* + 1 bytes in length.
 *   * The value of 30 comes from the result of the following javascript
 *     ~Object.bsonsize({_id:ObjectId() , s:''})~
 *
 * ### *Owning-team*
 * mongodb/replication
 *
 * ### *Keywords*
 *
 * @module workloads/secondary_performance
 */
/* global db sharded Random */
/* global rs print tojson assert Thread humanReadableNumber */
/* global stopServerReplication reportThroughput sleep jsTest quiesceSystem */
/* global ScopedThread load Mongo getPhaseData transposePhaseLogs */
/* global waitOplogCheck CountDownLatch reportDurations ReplSetTest isReplSet */

/**
 * The primary IP address. The default is "10.2.0.100".
 *
 * The value can be changed as a parameter, see
 * {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var primary = primary || "10.2.0.100";

/**
 * The secondary IP address. The default is "10.2.0.101".
 *
 * The value can be changed as a parameter, see
 * {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var secondary = secondary || "10.2.0.101";

/**
 * The size of string to use. It can be any value (less than ~16GB).
 * The external configutation currently uses 1, 100, 1000 and 10000. The default is 1.
 *
 * The value can be changed as a parameter, see
 * {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var size = size || 1; // Size of the string used in the docs inserted

/**
 * The total number of documents to insert. One of 10M and 1M (1M is for size
 * 10000). The default is 10M.
 *
 * The value can be changed as a parameter, see
 * {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var count = count || 10*1000*1000; // Number of documents to insert

/**
 * The number of threads used to insert documents. The default is [32].
 *
 * *Note*: this value is only used as the number of threads to insert data.
 * Unlike in other tests, it is not specifically configuring a measurable metric for this
 * test. As a result, only a single value is expected.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var thread_levels = thread_levels || [ 32 ];

function runBenchmark(primary, secondary, benchmark, username, password, authEnabled) {
    "use strict";
    var privateCollection = "private_db.private_collection";

    var num_members = primary.getDB("test")._adminCommand("replSetGetStatus").members.length;

    // Preinsert a document so the collection exists on the secondary.
    assert.writeOK(primary.getCollection(privateCollection).insert({}, {writeConcern:{w:num_members}}));

    quiesceSystem();

    // Stop application on the secondary.
    stopServerReplication(secondary);

    var primaryTime = Date.timeFunc(function(){ return benchmark(primary); }) / 1000;

    primary.adminCommand('fsync');

    // Spawn a thread to do a w:all write. It will block until the secondary finishes replicating.
    var thread = new ScopedThread(function(host, privateCollection, num_members, username, password, authEnabled) {
        if (authEnabled){
            var hostAuth = 'mongodb://'.concat(username, ':', password, '@', host);
        } else {
            hostAuth = host;
        }
        assert.writeOK(new Mongo(hostAuth).getCollection(privateCollection)
                                      .insert({}, {writeConcern:{w:num_members}}));
    }, primary.host, privateCollection, num_members, username, password, authEnabled);
    thread.start();
    // Restart application on the secondary.
    assert.commandWorked(secondary.adminCommand({configureFailPoint: 'stopReplProducer',
                                                 mode: 'off'}));
    var secondaryTime = Date.timeFunc(function(){ return thread.join(); }) / 1000;
    primary.getDB('bench').dropDatabase({w: 2});
    secondary.adminCommand('fsync');

    return {primaryTime: primaryTime, secondaryTime: secondaryTime};
}


function insertDocumentsThread(mongo, size, count, username, password, authEnabled) {
    if (typeof(mongo) == 'string') {
        if (authEnabled){
            var mongoAuth = 'mongodb://'.concat(username, ':', password, '@', mongo);
        } else {
            mongoAuth = mongo;
        }
        mongo = new Mongo(mongoAuth);
    }
    var collection = mongo.getCollection('bench.collection');
    var str = Array(size+1).toString(); // TODO uncompressible?
    while (count > 0) {
        // Send batches of up to 1000 to prevent unlimited client-side buffering.
        var batch = collection.initializeUnorderedBulkOp();
        var batchSize = Math.min(count, 1000);
        for (var i = 0; i< batchSize; i++){
            batch.insert({s:str}); // _id set automatically.
        }
        assert.writeOK(batch.execute());
        count -= batchSize;
    }
}

function insertDocuments(mongo, size, count, num_insert_threads, username, password) {
    if (typeof num_insert_threads === 'undefined') {
        num_insert_threads = 1;
    }
    var threads = [];
    for (i = 0; i < num_insert_threads; i++) {
        threads.push(new Thread(insertDocumentsThread, mongo.host, size, count/num_insert_threads, username, password, authEnabled));
        threads[threads.length-1].start();
    }
    threads.forEach(function(thread){ thread.join(); });
    return;

}

function getDBIfString(mongo, username, password, authEnabled) {
    if (typeof(mongo) == 'string') {
        print ("mongo is a string and is " + mongo);
        if (authEnabled){
            var mongoAuth = 'mongodb://'.concat(username, ':', password, '@', mongo);
        } else {
            mongoAuth = mongo;
        }
        mongo = new Mongo(mongoAuth);
    }
    return mongo;
}


function runInsertTestAndReport(size, count) {
}

(function() {
    "use strict";
    if (isReplSet()) {
        // This likely can be handled better without variable reassignment, feel free to update
        var usernameFunc = username;
        var passwordFunc = password;
        var authEnabledFunc = authEnabled;
        primary = getDBIfString(primary, usernameFunc, passwordFunc, authEnabledFunc);
        secondary = getDBIfString(secondary, usernameFunc, passwordFunc, authEnabledFunc);
        thread_levels.forEach(function(num_insert_threads){
            var suffix = "size_" + humanReadableNumber(size) + "_count_" + humanReadableNumber(count);
            print ("Running tests for " + suffix);
            var results = runBenchmark(primary, secondary,
                                       function(mongo){ return insertDocuments(mongo, size, count, num_insert_threads, usernameFunc, passwordFunc, authEnabledFunc); }, usernameFunc, passwordFunc, authEnabledFunc);
            print("Primary time: " + results["primaryTime"]);
            print("Secondary time: " + results["secondaryTime"]);
            reportThroughput("Primary_" + suffix, count /results["primaryTime"], {nThread: num_insert_threads});
            reportThroughput("Secondary_" + suffix, count /results["secondaryTime"], {nThread: num_insert_threads});
        });
    } else {
        print("Workload 'secondary_performance' is not configured to run in a standalone or sharded environment.\n");
        return;
    }
}());
