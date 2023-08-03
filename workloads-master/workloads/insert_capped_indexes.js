/**
 * @file
 * Test inserting into a capped collection with multiple secondary indexes
 *
 * ### *Test*
 *
 * Batches of 1000 documents are inserted into a
 * {@link https://docs.mongodb.com/manual/core/capped-collections/|capped collection}
 * with multiple secondary indexes.
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
 * +  This is based on insert processes that MMS/Cloud Manager uses. Inserts like this caused a lot
 *    of headaches in the 3.0 timeframe.
 * +  Typical results are unusual: Throughput over thread levels is flat for MMAPv1 (makes sense)
 *    and actually decreases with WT with more thread levels. This makes sense: the deletion is
 *    single threaded, so you cannot insert new stuff faster than that. (It's throttled on purpose.)
 *
 * ### *Owning-team*
 * mongodb/storage-execution
 *
 * ### *Keywords*
 *
 * @module workloads/insert_capped_indexes
 */
/* global db sharded Random enableSharding shardCollection quiesceSystem */
/* global benchRun benchStart benchFinish sh print printjson assert  */
/* global reportThroughput sleep server jsTest version emit */
/* global createJob runJobsInPool WORDS */

var db_name = "cappedTestIndexes";
var d = db.getSiblingDB(db_name);
var capSize = 1024*1024*1024; // 1GB capped collection size
var useCount = false; // default to set the cap by size only
var batchSize = 1000;
var testTime = 180;

/**
 * The number of threads to run in parallel. The default is [1, 2].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var thread_levels = thread_levels || [1, 2];

// This document is patterned off of MMS log documents, with some
// minor changes and uses benchRun expansions
var doc = {
	"_id" : {
		"#OID" : 0
	},
	"groupID" : {
		"#RAND_INT" : [
			0,
			100
		]
	},
	"timestamp" : {
		"#CUR_DATE" : 0
	},
	"level" : {
		"#RAND_INT" : [
			0,
			5
		]
	},
	"thread" : {
		"#RAND_STRING" : [
			20
		]
	},
	"message" : {
		"#RAND_STRING" : [
			40
		]
	},
	"hostname" : {
		"#RAND_STRING" : [
			10
		]
	},
	"process" : {
		"#RAND_STRING" : [
			10
		]
	}
};
var docSize = 201; // This is from measurement
var capCount = capSize/docSize; // max doc count that's roughly 1GB


var docs = [];
for (var i = 0; i < batchSize; i++) {
    docs.push( doc );
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

// Create the collection with the appropriate indexes
function setup(use_count) {
    d.dropDatabase();
    coll = d.capped_coll_indexes;
    if (use_count)
        d.createCollection("capped_coll_indexes", {capped: true, size: capSize, max: capCount});
    else
        d.createCollection("capped_coll_indexes", {capped: true, size: capSize});

    var start_time = Date.now();
    // fill the collection with a little over the cap size to force deletes

    // Use benchRun to load data, so that all the expansions are converted to (random) data
    // Run it long enough to ensure we filled the capped size.
    // Ignores capCount, but this is ok. In the worst case, we did some extra work.
    var totalops = 0;
    while (totalops < capSize / (batchSize * docSize)) {
        res = testInsert(docs, 8, 10);
        totalops += res.totalOps;
        print("Inserted " + (totalops * batchSize) + " docs as part of setup.");
    }
    print ("Finished inserting documents. Indexing now.");
    // Add the indexes
    coll.createIndex({"groupID" : 1, "level" : 1, "timestamp" : 1});
    coll.createIndex({"groupID" : 1, "process" : 1, "timestamp" : 1});
    coll.createIndex({"groupID" : 1, "hostname" : 1, "timestamp" : 1});
    coll.createIndex({"groupID" : 1, "timestamp" : -1});
    print("Indexed collection as part of setup.");
    var end_time = Date.now();
    elapsed_time = end_time - start_time;
    thpt = batchSize*(totalops)/elapsed_time;
    print("Total time to prime the collection: " + elapsed_time/1000 + " seconds");
    print("Throughput: " + thpt + " ops/sec");
}

function run_test() {
    if (sharded()) {
        print("Workload 'insert_capped_indexes' is not configured to run in a sharded environment.\n");
        return;
    } else {
        setup(useCount);
        for (var i=0; i < thread_levels.length; i++) {
            quiesceSystem();
            var threads = thread_levels[i];
            res = testInsert(docs, threads, testTime);
            if (useCount)
                reportThroughput("insert_capped_indexes_WithCount", res["totalOps/s"]*batchSize, {nThread: threads});
            else
                reportThroughput("insert_capped_indexes", res["totalOps/s"]*batchSize, {nThread: threads});
        }
        cleanup();
    }
}

run_test();
