/**
 * @file
 * Various simple tests with different Write Concern options.
 *
 * Write operations on documents with
 * {@link https://docs.mongodb.com/manual/reference/write-concern/#write-concern|Write Concern}. The supported
 * operations currently are:
 * 1. insert
 * +  update
 * +  remove
 *
 * ### *Test*
 *
 * The following operations are tested:
 * * Insert empty documents.
 * * Update a set of ranged _ids by increment a field by 1.
 * * Remove a multiple docs matching a range, then insert / upsert new documents
 *   to replace the removed documents.
 *
 * Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 * Inserts: Nothing, inserts are on an empty collection.
 * Updates: 4800 docs are created as part of the test initialization.
 * Removes: The collection used for updates is reused for removes.
 *
 * ### *Notes*
 * 1. For all the tests, a varying number of threads is used.
 * +  The runtime for all the tests listed above defaults to 60 seconds.
 * +  BenchRun does not support bulkWrite() and in particular cannot do unordered bulk inserts. For
 *    a sharded cluster therefore this isn't really testing what we want. *In future*, we may need
 *    to compensate with a higher thread count or more benchrun clients (see
 *    {@link https://jira.mongodb.org/browse/SERVER-29764|SERVER-29764}).
 * +  It is likely that threads levels are not yet at peak values (but we are currently limited
 *    in the max num of configurable threads, see
 *    {@link https://jira.mongodb.org/browse/SERVER-29764|SERVER-29764}), but even in this case
 *    the results look like they are more stable.
 *
 * ### *Owning-team*
 * mongodb/product-perf
 *
 * ### *Keywords*
 *
 * @module workloads/crud
 */
/* global db sharded Random enableSharding shardCollection quiesceSystem */
/* global benchRun benchStart benchFinish sh print printjson assert  */
/* global reportThroughput sleep server jsTest version emit */
/* global createJob runJobsInPool WORDS */


var db_name = "crudTest";
var testDB = db.getSiblingDB(db_name);
var testTime = 60;

/**
 * The number of threads to run in parallel. The default is [1, 64, 128].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var thread_levels = thread_levels || [1, 64, 128];
/**
 * The w option of writeConcern to use during tests. The default is 1.
 */
var w_value = typeof w_value === 'undefined' ? "majority" : w_value;
/**
 * The j option of writeConcern to use during tests. The default is true.
 */
var j_value = typeof j_value === 'undefined' ? true : j_value;

var test_suffix = "_j" + j_value + "_w" + w_value;

var writeConcern = { "w": w_value, "j": j_value };

var numDocs = 4800; // numDocs should be >= 100*max threads

var setupTest = function( collection ) {
    collection.drop();
    var docs = [];
    for ( var i = 0; i < numDocs; i++ ) {
        docs.push( { _id : i , x : 0 } );
    }
    collection.insert(docs, {writeConcern: {w: "majority"}});
};

var run_insert = function(coll, thread, runTime, wc, suffix){
    quiesceSystem();
    // Do insert test first
    // Same workload as Insert.Empty
    // TODO: For sharded clusters, this kind of insert test should really use unordered bulkWrite().
    // But benchRun doesn't support it. Probably need to compensate by running a high nr of threads.
    res = benchRun( {
        ops : [{"op":"insert",
                "doc":{},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true}],
        seconds : runTime,
        host : server,
        parallel : thread
    });
    reportThroughput("insert" + suffix, res["totalOps/s"], {nThread: thread});
};

var run_update_multi = function(coll, thread, runTime, wc, suffix) {
    quiesceSystem();
    res = benchRun( {
        ops : [{"op":"update",
                "multi":true,
                "query":{"_id":{"$in":[{"#RAND_INT_PLUS_THREAD":[0,100]},
                                       {"#RAND_INT_PLUS_THREAD":[0,100]}]}},
                "update":{"$inc":{"x":1}},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true}],
        seconds : runTime,
        host : server,
        parallel : thread
    });
    // Updating 2 documents each time (although ~1% of the time it will be 1)
    reportThroughput("updatemulti" + suffix, res["totalOps/s"]*2, {nThread: thread});
};

var run_update_single = function(coll, thread, runTime, wc, suffix) {
    quiesceSystem();
    // update single workload
    // modifying previous workload to only select one document and use multi:false
    res = benchRun( {
        ops : [{"op":"update",
                "multi":false,
                "query":{"_id":{"#RAND_INT_PLUS_THREAD":[0,100]}},
                "update":{"$inc":{"x":1}},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true}],
        seconds : runTime,
        host : server,
        parallel : thread,
        username: username,
        password: password
    });
    reportThroughput("updatesingle" + suffix, res["totalOps/s"], {nThread: thread});
};

var run_remove_multi = function(coll, thread, runTime, wc, suffix) {
    quiesceSystem();
    // remove multi workload
    // Using same corpus
    res = benchRun( {
        ops : [{ "op": "let",
                 "target": "x",
                 "value": {"#RAND_INT_PLUS_THREAD": [0,100]},
                 "ns":coll.getFullName()},
               { "op": "let",
                 "target": "y",
                 "value": {"#RAND_INT_PLUS_THREAD": [0,100]},
                 "ns":coll.getFullName()},
               {"op":"remove",
                "multi":true,
                "query":{"_id":{"$in":[{"#VARIABLE":"x"},{"#VARIABLE":"y"}]}},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true},
               {"op":"insert",
                "doc":{"_id" : {"#VARIABLE" : "x"}, x : 0},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true},
               {"op":"update",
                "upsert" : true, // Use upsert in case x equals y
                "query":{_id : {"#VARIABLE" : "y"}},
                "update":{$inc : {x : 1}},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true}],
        seconds : runTime,
        host : server,
        parallel : thread,
        username: username,
        password: password
    });
    // benchRun counts 5 operations per iteration above. We're removing
    // 2 documents, and inserting 2 documents, so 4 documents that we
    // care about. Therefore scaling throughput by 4/5 = 0.8
    reportThroughput("removemulti" + suffix, res["totalOps/s"]*0.8, {nThread: thread});
};

var run_remove_single = function(coll, thread, runTime, wc, suffix) {
    quiesceSystem();
    // single remove
    res = benchRun( {
        ops : [{ "op": "let",
                 "target": "x",
                 "value": {"#RAND_INT_PLUS_THREAD": [0,100]},
                 "ns":coll.getFullName()},
               {"op":"remove",
                "multi":false,
                "query":{"_id":{"#VARIABLE":"x"}},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true},
               {"op":"insert",
                "doc":{"_id" : {"#VARIABLE" : "x"}, x : 0},
                "ns":coll.getFullName(),
                "writeConcern":wc,
                "writeCmd":true}],
        seconds : runTime,
        host : server,
        parallel : thread,
        username: username,
        password: password
    });
    // benchRun counts 3 operations per iteration above. We're
    // removing 1 documents, and inserting 1 documents, so 2 documents
    // that we care about. Therefore scaling throughput by 2/3 = 0.667
    reportThroughput("removesingle" + suffix, res["totalOps/s"]*0.667, {nThread: thread});
};

// We want to drop all data, but simultaneously we want to avoid dropDatabase() because it is
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

var run = function(d, thread, runTime, wc, suffix) {
    cleanup(d);
    var coll = d.insert_empty;
    if (sharded()) {
        shardCollection( d, coll );
    }
    run_insert(coll, thread, runTime, wc, suffix);

    coll = d.update;
    if (sharded()) {
        shardCollection( d, coll );
    }
    setupTest(coll);
    run_update_multi(coll, thread, runTime, wc, suffix);
    // Don't cleanup, following tests use the same collection
    run_remove_multi(coll, thread, runTime, wc, suffix);
};

var run_tests = function(d, wc, suffix) {
    if (sharded()) {
        enableSharding(d);
    }

    for (var i=0; i < thread_levels.length; i++) {
        print("Running thread level " + thread_levels[i]);
        var threads = thread_levels[i];
        run(d, threads, testTime, wc, suffix);
    }
    d.dropDatabase();
};

run_tests(testDB, writeConcern, test_suffix);
