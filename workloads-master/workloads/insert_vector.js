/**
 * @file
 * Batched inserts
 *
 * Insert a batch of documents at a time. In particular, using the old style
 * `insert([ {}, {}, ... ])` batch inserts.
 *
 * ### *Test*
 *
 * Insert 250 documents `{ fieldName: "xxx..." }` where bsonsize < 512 bytes.
 *
 * Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 * Nothing. Inserts into an empty collection.
 *
 * ### *Notes*
 *
 * BenchRun does not support bulkWrite() and in particular cannot do unordered bulk inserts.
 * For a sharded cluster therefore this isn't really testing what we want. We compensate with
 * higher thread count.
 *
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 *
 * @module workloads/insert_vector
 */

////////////////////////////////////////////////////////////////////////////////
// Start of global scope variables

/**
 * The number of threads to run in parallel. The default is [1, 8, 16].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var thread_levels = thread_levels || [1, 8, 16];

/**
 * Whether to run this test with retryable writes. Defaults to false.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var retryable_writes = retryable_writes || false;

// End of global scope variables
////////////////////////////////////////////////////////////////////////////////

var db_name = "VectoredInsertTest";
var batchSize = 250; // number of documents per vectored insert
var docSize = 512; // Document size + _id field
var testTime = 180; // benchRun test time in seconds

// Threshold for pass fail comparing secondary throughput to primary
var threshold = 0.95;

var testDB = db.getSiblingDB(db_name);

// Make a document of the given size. Actually plus _id size.
function makeDocument(docSize) {
    for (var i=0; i<docSize; i++) {
        var doc = { "fieldName":"" };
        while(Object.bsonsize(doc) < docSize) {
            doc.fieldName += "x";
        }
        return doc;
    }
}
doc = makeDocument(docSize);

// Make the document array to insert
var docs = [];
for (var i = 0; i < batchSize; i++) {
    docs.push(doc);
}


// We wan't to drop all data, but simultaneously we wan't to avoid dropDatabase() because it is
// known to have been flaky to then immediately call enableSharding() again. So we simply drop all
// collections inside the db.
var cleanup = function(d) {
    var colls = d.getCollectionNames();
    while( colls.length ){
        var c = colls.pop();
        if ( c.substring(0, 7) == "system." )
            continue;

        assert(d.getCollection(c).drop(), "Failed to drop collection between tests.");
    }
};

// Test of oplog with vectored inserts.
// Insert a vector of documents using benchRun.
// Measure the  throughput of benchRun on the primary and secondary.
// Wait for all secondaries to completely catch up, and compute the aggregate throughput.
// The Test passes if the aggregate throughput is within threshold
// percent of the primary's throughput.
function testInsert(d, docs, thread, runTime, retryableWrites) {
    waitOplogCheck();
    cleanup(d);
    coll = d.vectored_insert;

    if (sharded()) {
        shardCollection(d, coll);
    }
    if (!quiesceSystem()) {
        print("Error in quiesceSystem, exiting test insert_vector.js");
        return false;
    }

    // Make sure that the oplog is caught up
    if (!waitOplogCheck()) {
        // error with replSet, quit the test early
        return;
    }
    // timestamp for the start of the test
    var start = new Date();
    // TODO: For sharded clusters, this kind of insert test should really use unordered bulkWrite().
    // But benchRun doesn't support it. Probably need to compensate by running a high nr of threads.
    var benchArgs = { ops : [ { ns : coll.getFullName() ,
                                op : "insert" ,
                                doc : docs,
                                writeCmd : true} ],
                      parallel : thread,
                      seconds : runTime,
                      host : server,
                      username: username,
                      password: password};

    if (retryableWrites) {
        benchArgs.useSessions = true;
        benchArgs.useIdempotentWrites = true;
    }

    res = benchRun(benchArgs);
    // timestamp for benchRun complete, but before waiting for secondaries
    var middle = new Date();

    // if replSet, get secondary stats and compute
    var members = rs.status().members;

    // Get the number of documents on the primary. This assumes we're
    // talking to the primary. Can explicitely get primary if needed,
    // but the benchRun call should have been done against the primary
    var primDocs = d.stats().objects;

    // Find the minimum number of documents processed by any of the
    // secondaries
    var minDocs = primDocs;
    // Go through all members
    for (i in members) {
        // Skip the primary
        if (members[i].state != 1) {
            // Get the number of objects
            // Connect to the secondary and call db.sats()
            if (authEnabled){
                var membersAuth = 'mongodb://'.concat(username, ':', password, '@', members[i].name);
            } else {
                membersAuth = members[i].name;
            }
            var x = new Mongo(membersAuth);
            var mydb = x.getDB(d);
            var numDocs = mydb.stats().objects;
            if (numDocs < minDocs) {
                minDocs = numDocs;
            }
        }
    }

    // for each secondary, wait until it is caught up.
    if (!waitOplogCheck()) {
        // error with replSet, quit the test early
        return;
    }
    // All secondaries are caught up now. Test is done. Get the time
    var end = new Date();
    if(!sharded()) {
        // print out to get lags
        rs.printSlaveReplicationInfo();
    }

    printjson(res);

    //var thpt = res["totalOps/s"]*batchSize; // This throughput isn't comparable to the secondary ones, so not going to use.
    // All following throughputs needs to be scaled by time. The timestamps are in milliseconds.
    var secThpt = primDocs * 1000 / (end - start);
    // Minimum Throughput of a secondary during the benchRun call
    var secThptPrimary = minDocs * 1000 / (middle - start);
    // Throughput of the primary as measured by this script (not benchRun)
    var primThpt = primDocs * 1000 / (middle - start);
    //reportThroughput("insert_vector_primary_benchrun", thpt, {nThread: thread});

    // Did the secondary have 95% of the throughput?
    // During the first phase?
    var second_pass_first_phase = (minDocs > primDocs * threshold);
    // overall?
    var second_pass_overall = ((threshold * (end - start)) < (middle - start));

    var reportNameSuffix = (retryableWrites ? "_retry" : "");
    reportThroughput("insert_vector_primary" + reportNameSuffix, primThpt, {nThread: thread});
    reportThroughput("insert_vector_secondary_load_phase" + reportNameSuffix, secThptPrimary, {nThread : thread, pass : second_pass_first_phase, errMsg : "Sec. falling behind"});
    reportThroughput("insert_vector_secondary_overall" + reportNameSuffix, secThpt, {nThread: thread, pass : second_pass_overall, errMsg : "Sec. slower than primary"});
    print("thread = " + thread + ",   thpt = " + primThpt.toFixed(2));
}

function run_test(d) {
    if (sharded()) {
        enableSharding(d);
    }

    for (var i=0; i < thread_levels.length; i++) {
        print("Running thread level " + thread_levels[i] + ", retryableWrites: " + retryable_writes);
        var threads = thread_levels[i];
        testInsert(d, docs, threads, testTime, retryable_writes);
    }

    d.dropDatabase();
}

run_test(testDB);
