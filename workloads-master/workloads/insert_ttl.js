/**
 * @file
 * Test inserting into a collection with a TTL index
 *
 * ### *Test*
 *
 * Batches of 1000 documents (with the current time) are inserted into a collection with a
 * {@link https://docs.mongodb.com/manual/core/index-ttl/|TTL index}
 *
 * Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 * 1. an index with a TTL index is created with expireAfterSeconds is set to 0.
 * +  ttlMonitorSleepSecs is set to 5 seconds.
 *
 * ### *Notes*
 *
 * 1. For all the tests, a varying number of threads is used.
 * +  The runtime for each test is 3 minutes.
 * +  BenchRun does not support bulkWrite() and in particular cannot do unordered bulk inserts.
 *    We compensate with higher thread count.
 *
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 *
 * @module workloads/insert_ttl
 */
/* global db sharded Random enableSharding shardCollection quiesceSystem */
/* global benchRun benchStart benchFinish sh print printjson assert  */
/* global reportThroughput sleep server jsTest version emit */
/* global createJob runJobsInPool WORDS */
var db_name = "ttlTest";
var expTime = 0; // 0 seconds expireAfterSeconds
var monitorSleep = 5; // 5 second ttlMonitorSleepSecs
var batchSize = 1000;
var docSize = 1024;
var testTime = 180;

/**
 * The number of threads to run in parallel. The default is [1, 16, 32].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var thread_levels = thread_levels || [1, 16, 32];

var testDB = db.getSiblingDB(db_name);
testDB.adminCommand({setParameter: 1, ttlMonitorSleepSecs: monitorSleep});
print("Setting ttlMonitorSleepSecs to " + monitorSleep);
var docs = [];
for (var i = 0; i < batchSize; i++) {
    docs.push( {ttl: {"#CUR_DATE": 0},
                s : docSize > 41 ? new Array(docSize - 41).join('x') : ""} );
    // Make the size of all documents equal to docSize
    // 41 is the size of a document with empty string
}

function cleanup(d) {
    db.adminCommand({setParameter: 1, ttlMonitorSleepSecs: 60});
    print("Setting ttlMonitorSleepSecs to " + 60);
    d.dropDatabase();
}

function testInsert(d, docs, thread, runTime) {
    coll = d.ttl_coll;
    coll.drop();
    coll.createIndex({ttl: 1}, {expireAfterSeconds: expTime});

    if( sharded() ) {
        shardCollection(d, coll);
    }

    // TODO: For sharded clusters, this kind of insert test should really use unordered bulkWrite().
    // But benchRun doesn't support it. Probably need to compensate by running a high nr of threads.
    var benchArgs = { ops : [ { ns : coll.getFullName() ,
                                op : "insert" ,
                                writeCmd : true,
                                doc : docs} ],
                      parallel : thread,
                      seconds : runTime,
                      host : server,
                      username: username,
                      password: password };
    res = benchRun(benchArgs);
    var oldest = coll.find().sort({ttl:1}).limit(1).next().ttl;
    var now = new Date();
    var age = now - oldest;
    var thpt = res["totalOps/s"]*batchSize;
    reportThroughput("insert_ttl", thpt, {nThread: thread});
    print("thread = " + thread + ",   thpt = " + thpt.toFixed(2) + ",   oldest_doc_age = " +age);
}

function run_test(d) {
    if (sharded()) {
        enableSharding(d);
    }

    for (var i=0; i < thread_levels.length; i++) {
        quiesceSystem();
        var threads = thread_levels[i];
        testInsert(d, docs, threads, testTime);
    }
    cleanup(d);
}

run_test(testDB);

