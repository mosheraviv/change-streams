/**
 * @file
 * Test inserting into a capped collection
 *
 * ### *Tests*
 *
 * Batches of 1000 documents are inserted into a
 * {@link https://docs.mongodb.com/manual/core/capped-collections/|capped collection}
 *
 * Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 * 1. collection is capped at 1GB.
 * +  collection count, not enabled by default, is capped at 1024*1024. The document size is 1KB,
 *    so this also results in a size of 1GB.
 *
 * ### *Notes*
 *
 * 1. For all the tests, a varying number of threads is used.
 * +  Capped collection are not supported in a sharded configurations.
 * +  Test time is 3 minutes.
 *
 * ### *Owning-team*
 * mongodb/storage-execution
 *
 * ### *Keywords*
 *
 * @module workloads/insert_capped
 */
/* global db sharded Random enableSharding shardCollection quiesceSystem */
/* global benchRun benchStart benchFinish sh print printjson assert  */
/* global reportThroughput sleep server jsTest version emit */
/* global createJob runJobsInPool WORDS */
var db_name = "cappedTest";
var d = db.getSiblingDB(db_name);
var capSize = 1024*1024*1024; // 1GB capped collection size
var capCount = 1024*1024; // max doc count that's roughly 1GB
var useCount = false; // default to set the cap by size only
var batchSize = 1000;
var docSize = 1024;
var testTime = 180;

/**
 * The number of threads to run in parallel. The default is [1, 2].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var thread_levels = thread_levels || [1, 2];

doc_content = '';
for (var i=0; i<docSize; i++) {
    doc_content += 'x';
}

var docs = [];
for (var i = 0; i < batchSize; i++) {
    docs.push( {x: doc_content} );
}

function setup(use_count) {
    d.dropDatabase();
    coll = d.capped_coll;
    if (use_count)
        d.createCollection("capped_coll", {capped: true, size: capSize, max: capCount});
    else
        d.createCollection("capped_coll", {capped: true, size: capSize});
    // Fill the collection with a little over the cap size to force deletes
    // Ignores capCount, but this is ok. In the worst case, we did some extra work.
    var start_time = Date.now();
    for (var i=0; i < 1.1*capSize/(batchSize*docSize); i++) {
        var bulk = coll.initializeUnorderedBulkOp();
        for (var j=0; j<batchSize; j++)
            bulk.insert( {x: doc_content} );
        bulk.execute();
    }
    var end_time = Date.now();
    elapsed_time = end_time - start_time;
    thpt = 1000*(i*batchSize)/elapsed_time;
    print("Total time to prime the collection: " + elapsed_time/1000 + " seconds");
    print("Throughput: " + thpt + " ops/sec");
    if (use_count)
        reportThroughput("initial_load_cappedWithCount", thpt, {nThread: 1});
    else
        reportThroughput("initial_load_capped", thpt, {nThread: 1});
}

function cleanup() {
    d.dropDatabase();
}

function testInsert(docs, thread, tTime) {
    var benchArgs = { ops : [ { ns : coll.getFullName() ,
                                op : "insert" ,
                                writeCmd: true,
                                doc : docs} ],
                      parallel : thread,
                      seconds : tTime,
                      host : server,
                      username: username,
                      password: password };
    res = benchRun(benchArgs);
    return res;
}

function run_test() {
    if (sharded()) {
        print("Workload 'insert_capped' is not configured to run in a sharded environment.\n");
        return;
    } else {
        setup(useCount);
        for (var i=0; i < thread_levels.length; i++) {
            quiesceSystem();
            var threads = thread_levels[i];
            res = testInsert(docs, threads, testTime);
            if (useCount)
                reportThroughput("insert_cappedWithCount", res["totalOps/s"]*batchSize, {nThread: threads});
            else
                reportThroughput("insert_capped", res["totalOps/s"]*batchSize, {nThread: threads});
        }
        cleanup();
    }
}

run_test();
