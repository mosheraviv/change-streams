/**
 * @file
 *
 * This file tests that routing operations through a mongos does not impose an overhead greater
 * than 30% (direct v passthrough and passthrough v sharded). That is, determine the penalty
 * imposed by {@link https://docs.mongodb.com/manual/sharding/|sharding}.
 *
 * ### *Test*
 *
 * Compare the throughput of a fixed workload for various configurations.
 * This workload alternates single reads
 * ({@link https://docs.mongodb.com/manual/reference/method/db.collection.findOne/|findOne})
 * and writes
 * ({@link https://docs.mongodb.com/manual/reference/method/db.collection.insert/|insert}).
 *
 * The routes tested are:
 * 1. **direct**: bypass the mongos and send operations directly to a replica set primary.
 * 1. **pass through**: route operations through a mongos to an unsharded collection.
 * 1. **sharded**: route operations through a mongos to a shard collection.
 *
 * The pass / fail ratios are:
 * 1. **passthrough** / **direct** > 0.7
 * 1. **sharded** / **direct** > 0.7
 *
 * Results are reported as the following ratios:
 * 1. **passthrough** / **direct**
 * 1. **sharded** / **direct**
 *
 * ### *Setup*
 *   non-mongos configuration versus the configuration behind a mongos and
 *   config server
 *
 * ### *Notes*
 * * The collection namespace is *mongos_50read_50write.test*.
 * * The collection is sharded using a hashed shard key.
 * * This test uses a varying number of threads.
 * * The document being inserted is padded with a string of 'x's to ensure it is 512B in size.
 * * The test is run for a fixed period of time, 3 minutes in this case.
 *
 * ### *Owning-team*
 * mongodb/sharding
 *
 * ### *Keywords*
 *
 * @module workloads/mongos_50read_50write
 */
/*global
  db sharded Random enableSharding shardCollection quiesceSystem
  benchRun benchStart benchFinish sh print printjson assert tojson
  reportThroughput sleep server jsTest version findPrimaryShard
*/

// Full namespace is 'mongos_50read_50write.test'
var db_name = "mongos_50read_50write";
var d = db.getSiblingDB(db_name);
var namespace = db_name + ".test";

// Tunable configuration options
var is_trial_run = false;           // Trial run - don't report results in core workloads
var doc_size = 512;                 // Size of document plus _id field
var test_time = 180;                // The benchRun test time in seconds

/**
 * The number of threads to run in parallel. The default is [1, 16, 32].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var thread_levels = thread_levels || [1, 16, 32]; // The number of threads to run in parallel
var threshold = 0.7;                // Required mongos throughput ratio to pass

// Make a document of the desired size
var document = {
    "x": 1,
    "padding": "x"
};
while (Object.bsonsize(document) < doc_size) {
    document.padding += "x";
}

var setup = function(d) {
    // Drop the database and recreate the index
    d.dropDatabase();
    d.createCollection("test");
};

var teardown = function(d) {
    d.dropDatabase();
};

var testInsertAndRead = function(target, threads, use_sharding) {
    // Do we target a sharded collection?
    if (use_sharding) {
        sh.enableSharding(db_name);
        sh.shardCollection(namespace, {"x": "hashed"});
    }
    quiesceSystem();
    var bench_args = {
        "ops": [{op: "insert", writeCmd: true, ns: namespace, doc: document},
                {op: "findOne", readCmd: true, ns: namespace, query: {"x": 1}}],
        "parallel": threads,
        "seconds": test_time,
        "host": target,
        "username": username,
        "password": password
    };

    var result = benchRun(bench_args);
    printjson(result);
    return result;
};

var runTest = function() {
    if (!sharded()) {
        print("Workload 'mongos_50read_50write' must be run in a sharded environment.");
    }
    else {
        // Run the tests on various thread counts
        for (var i = 0; i < thread_levels.length; i++) {
            var thread_count = thread_levels[i];

            // Run the test three times against different targets
            setup(d);
            var result_direct_shard = testInsertAndRead(findPrimaryShard(db_name), thread_count, false);
            teardown(d);
            setup(d);
            var result_passthrough = testInsertAndRead(server, thread_count, false);
            teardown(d);
            setup(d);
            var result_sharded = testInsertAndRead(server, thread_count, true);
            teardown(d);

            // Calculate ratios and pass/fail results
            var passthrough_ratio = 100 * result_passthrough["totalOps/s"] / result_direct_shard["totalOps/s"];
            var passthrough_result = {
                "nThread": thread_count,
                "pass": (passthrough_ratio > threshold),
                "errMsg": ""
            };
            var sharded_ratio = 100 * result_sharded["totalOps/s"] / result_direct_shard["totalOps/s"];
            var sharded_result = {
                "nThread": thread_count,
                "pass": (sharded_ratio > threshold),
                "errMsg": ""
            };

            // Report throughput for the two mongos variants
            reportThroughput("mongos_50read_50write_passthrough_ratio", passthrough_ratio, passthrough_result, is_trial_run);
            reportThroughput("mongos_50read_50write_sharded_ratio", sharded_ratio, sharded_result, is_trial_run);
            reportThroughput("mongos_50read_50write_passthrough_raw", result_passthrough["totalOps/s"], {nThread: thread_count}, is_trial_run);
            reportThroughput("mongos_50read_50write_sharded_raw", result_sharded["totalOps/s"], {nThread: thread_count}, is_trial_run);
            reportThroughput("mongos_50read_50write_direct_shard", result_direct_shard["totalOps/s"], {nThread: thread_count}, is_trial_run);
        }
    }
};

runTest();
