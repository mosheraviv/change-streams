/**
 *
 * @file
 * Measure performance of aggregation and corresponding find/count/distinct commands against the
 * {@link https://bestbuyapis.github.io/api-documentation/#overview|BestBuy Developer API data}.
 *
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 * - bestbuy
 * - aggregate
 * - throughput
 * - distinct
 * - find
 * - count
 *
 * ### *Pre-requisite*
 * The dataset must be installed on the target cluster before running the test. The data can be
 *  downloaded  from
 * {@link
 * https://s3-us-west-2.amazonaws.com/dsi-donot-remove/AggPerformance/bestbuyproducts.bson.gz|here}
 *  and installed using mongorestore (mongorestore --gzip --archive=bestbuyproducts.bson.gz)
 *
 * ### *Setup*
 * None
 *
 * ### *Test*
 *
 * There are two types of tests: those that attempt to measure performance of an aggregate vs. other
 * equivalent read commands, and those which are simply testing the throughput of aggregation.
 *
 * ## Agg/Query Comparison Tests
 *
 * Each test has an aggregation (useAgg) and non-aggregation (noAgg) variant. The tests cover:
 * distinct, find, count commands against the Best Buy dataset.
 *
 * Results are reported as ops / second.
 *
 * The test takes an optional 'thread_levels' parameters: an array specifying the number of threads
 * to use.
 *
 * ## Exclusively Aggregation Tests
 *
 * These tests use a simple for loop to repeatedly run a specific pipeline, reporting both the
 * throughput in documents processed per second and the performance variability, measured as the
 * percentage difference in latency between the 99th percentile and the 50th percentile.
 *
 * ### *Notes*
 * 1. The tests vary greatly in execution time. Tests that nominally take less than 50 ms are run
 *  + using benchRun, while those that take more than 50 ms are run in a simple loop for at least
 *  +  TEST_SECONDS.
 *
 * @module workloads/bestbuy_agg_query_comparison
 */

/**
 * The number of threads to run in parallel. The default is [1, 8].
 */
var thread_levels = thread_levels || [1, 8];

/**
 * Measure performance of aggregation and find/count/distinct commands against a customer's retail
 * products collection.
 */
(function() {
    "use strict";

    load("libs/bestbuy_products_full_projection.js");  // For 'retailerProductsFullProjection'.
    load("utils/exec_helpers.js");                     // For 'ExecHelpers'.

    var testDb = db.getSiblingDB(testDbName);
    var testColl = testDb[testCollName];

    // The number of seconds to use when measuring throughput, either with benchRun() or with
    // 'ExecHelpers.measurePerformance().
    var TEST_SECONDS = 2 * 60;

    // Queries whose latency is below this threshold will be run through benchrun, while queries
    // above that threshold will be run in a loop to guarantee that at least one query finishes, and
    // we wait for the last query to finish. Queries run in a loop will still report a throughput
    // based on the number completed and total execution time.
    var LATENCY_THRESHOLD_MILLIS = 50;

    var throughput;

    // Make sure the system has reached a stable state before starting the tests
    quiesceSystem();

    /**
     * Given the find command spec 'findCmd', converts into a benchRun "find" op suitable for
     * executing in benchRun().
     */
    function findOpFromCmd(findCmd) {
        var op = {op: "find", ns: testColl.getFullName(), readCmd: true};

        if (findCmd.filter) {
            op.query = findCmd.filter;
        }

        if (findCmd.projection) {
            op.filter = findCmd.projection;
        }

        if (findCmd.sort) {
            op.sort = findCmd.sort;
        }

        if (findCmd.skip) {
            op.skip = findCmd.skip;
        }

        if (findCmd.limit) {
            op.limit = findCmd.limit;
        }

        return op;
    }

    /**
     * Runs 'command' using benchRun() for TEST_SECONDS with threads. Returns the
     * resulting throughput measured in operations per second.
     */
    function getThroughput(command, threads) {
        var benchOp;

        if (command.hasOwnProperty("find")) {
            benchOp = findOpFromCmd(command);
        } else {
            benchOp = {
                op: "command",
                ns: testDb.getName(),
                command: command,
            };
        }
        benchOp.writeCmd = true;

        var benchArgs = {ops: [benchOp], parallel: threads, seconds: TEST_SECONDS, host: server, username: username, password: password};
        // printjson(benchArgs);
        var benchResult = benchRun(benchArgs);
        return benchResult["totalOps/s"];
    }

    /**
     * Returns a unique test name by appending '-useAgg' in cases that the operation was executed as
     * an aggregation or '-noAgg' if not.
     */
    function makeTestName(originalName) {
        return originalName + (useAgg ? "-useAgg" : "-noAgg");
    }

    /**
     * Given a pipeline, returns the full aggregation command suitable for executing the pipeline in
     * benchRun().
     */
    function aggCmdFromParts(pipeline) {
        return {aggregate: testColl.getName(), pipeline: pipeline, cursor: {}};
    }

    /**
     * Given a field name 'distinctKey' to find the distinct values of and a query predicate,
     * returns the full corresponding distinct command suitable for executing in benchRun().
     */
    function distinctCmdFromParts(distinctKey, predicate) {
        return {distinct: testColl.getName(), key: distinctKey, query: predicate};
    }

    /**
     * Given a predicate, returns the corresponding count command suitable for executing in
     * benchRun().
     */
    function countCmdFromParts(predicate) {
        return {count: testColl.getName(), query: predicate};
    }

    //
    // Distinct command vs. agg.
    //

    function runDistinct(distinctKey, predicate, useBenchrun, testname) {
        var pipeline = [{$match: predicate}, {$group: {_id: "$" + distinctKey}}];
        var aggCmd = aggCmdFromParts(pipeline);
        var distinctCmd = distinctCmdFromParts(distinctKey, predicate);
        if (useBenchrun) {
            print("useBenchrun " + testname + " thread_levels  " + thread_levels);
            thread_levels.forEach(function(nThread) {
                reportThroughput(testname,
                                 getThroughput(useAgg ? aggCmd : distinctCmd, nThread),
                                 {nThread: nThread});
            });
        } else {
            var perfInfo = ExecHelpers.measurePerformance(function() {
                if (useAgg) {
                    assert.gt(testColl.aggregate(pipeline).itcount(), 0);
                } else {
                    assert.gt(testColl.distinct(distinctKey, predicate).length, 0);
                }

            }, TEST_SECONDS);
            reportThroughput(testname, perfInfo.meanThroughput, {nThread: 1});
        }
    }

    runDistinct("type", {}, !useAgg, makeTestName("distinct_types_no_predicate"));
    runDistinct("type",
                {type: {$in: ["Game", "Movie", "Music", "HardGood"]}},
                !useAgg,
                makeTestName("distinct_types_with_in_predicate"));
    //
    // Count command vs. agg.
    //

    function runCount(predicate, useBenchrun, testname) {
        var pipeline = [{$match: predicate}, {$count: "count"}];
        var aggCmd = aggCmdFromParts(pipeline);
        var countCmd = countCmdFromParts(predicate);
        if (useBenchrun) {
            print("useBenchrun " + testname + " thread_levels  " + thread_levels);
            thread_levels.forEach(function(nThread) {
                reportThroughput(testname,
                                 getThroughput(useAgg ? aggCmd : countCmd, nThread),
                                 {nThread: nThread});
            });
        } else {
            var perfInfo = ExecHelpers.measurePerformance(function() {
                if (useAgg) {
                    assert.gt(testColl.aggregate(pipeline).itcount(), 0);
                } else {
                    assert.gt(testColl.count(predicate), 0);
                }
            }, TEST_SECONDS);
            reportThroughput(testname, perfInfo.meanThroughput, {nThread: 1});
        }
    }

    runCount({}, !useAgg, makeTestName("count_no_predicate"));
    runCount({type: "Music"}, false, makeTestName("count_with_type_predicate"));
    runCount(
        {type: "Movie", mpaaRating: {$in: ["PG", "PG-13"]}, theatricalReleaseDate: {$ne: null}},
        false,
        makeTestName("count_with_and_predicate"));

    //
    // Find command vs. agg.
    //

    function runFind(findCmd, useBenchrun, testname) {
        // Construct the aggregation command corresponding to 'findCmd'.
        var pipeline = [];
        if (findCmd.filter) {
            pipeline.push({$match: findCmd.filter});
        }
        if (findCmd.projection) {
            pipeline.push({$project: findCmd.projection});
        }
        if (findCmd.sort) {
            pipeline.push({$sort: findCmd.sort});
        }
        if (findCmd.skip) {
            pipeline.push({$skip: findCmd.skip});
        }
        if (findCmd.limit) {
            pipeline.push({$limit: findCmd.limit});
        }
        var aggCmd = aggCmdFromParts(pipeline);
        if (useBenchrun) {
            print("useBenchrun " + testname + " thread_levels  " + thread_levels);
            thread_levels.forEach(function(nThread) {
                reportThroughput(testname,
                                 getThroughput(useAgg ? aggCmd : findCmd, nThread),
                                 {nThread: nThread});
            });
        } else {
            var perfInfo = ExecHelpers.measurePerformance(function() {
                if (useAgg) {
                    assert.gt(testColl.aggregate(pipeline).itcount(), 0);
                } else {
                    var cursor = testColl.find(findCmd.filter, findCmd.projection);
                    if (findCmd.sort) {
                        cursor.sort(findCmd.sort);
                    }
                    if (findCmd.skip) {
                        cursor.skip(findCmd.skip);
                    }
                    if (findCmd.limit) {
                        cursor.limit(findCmd.limit);
                    }
                    assert.gt(cursor.itcount(), 0);
                }
            }, TEST_SECONDS);
            reportThroughput(testname, perfInfo.meanThroughput, {nThread: 1});
        }
    }

    runFind({find: testColl.getName(), filter: {type: "Music"}}, false, makeTestName("find"));

    runFind({
        find: testColl.getName(),
        filter: {type: "Music"},
        projection: retailerProductsFullProjection
    },
            false,
            makeTestName("find_project"));

    runFind({find: testColl.getName(), filter: {type: "Music"}, limit: 1000},
            true,
            makeTestName("find_limit"));

    runFind({find: testColl.getName(), filter: {type: "Music"}, skip: 1000, limit: 1},
            true,
            makeTestName("find_skip_limit_1"));

    runFind({
        find: testColl.getName(),
        filter: {type: "Music"},
        projection: retailerProductsFullProjection,
        skip: 1000,
        limit: 1000
    },
            false,
            makeTestName("find_project_skip_limit"));

    runFind({
        find: testColl.getName(),
        filter: {type: "Movie"},
        projection: {
            _id: 0,
            type: 1,
            mpaaRating: 1,
            format: 1,
            genre: 1,
            theatricalReleaseDate: 1,
            percentSavings: 1
        },
        sort: {mpaaRating: 1},
        skip: 500,
        limit: 1
    },
            !useAgg,
            makeTestName("find_project_6_sort_indexed_skip_limit_1"));

    runFind({
        find: testColl.getName(),
        filter: {type: "Movie"},
        projection: {
            _id: 0,
            type: 1,
            mpaaRating: 1,
            format: 1,
            genre: 1,
            theatricalReleaseDate: 1,
            percentSavings: 1
        },
        sort: {percentSavings: 1},
        skip: 500,
        limit: 1
    },
            false,
            makeTestName("find_project_6_sort_unindexed_skip_limit_1"));

}());
