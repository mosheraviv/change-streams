/**
 * @file
 * Measure performance of aggregation's $merge stage against the
 * {@link https://bestbuyapis.github.io/api-documentation/#overview|BestBuy Developer API data}.
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
 * The tests use a simple for loop to repeatedly run a specific pipeline ending in $merge, reporting
 * the throughput in documents processed per second.
 *
 * ### *Owning-team*
 * mongodb/product-query
 * 
 * ### *Keywords*
 * 
 * @module workloads/agg_out
 */
(function() {
    "use strict";

    load("utils/exec_helpers.js");  // For 'ExecHelpers'.

    var serverVersion = db.version().split(".");
    if (serverVersion[0] < 4 || (serverVersion[0] == 4 && serverVersion[1] < 1)) {
        jsTestLog("New $merge features are not available on version " + tojson(serverVersion) +
                  ". Skipping test.");
        return;
    }

    var testDb = db.getSiblingDB(testDbName);
    var testColl = testDb[testCollName];

    // Function which repeatedly runs a single test configuration for a set duration, recording the
    // average latency across all executions.
    function runMerge(nDocsPerMerge, matchRatio, spec) {
        // If the combination of parameters guarantees that this test will always fail, skip it.
        if ((spec.whenMatched === "fail" && matchRatio > 0) ||
            (spec.whenNotMatched === "fail" && matchRatio < 1.0)) {
            return;
        }
        // Only run the single-document test once, and skip all subsequent variations.
        if (nDocsPerMerge == 1 && (matchRatio < 1 || spec.whenMatched !== "merge")) {
            return;
        }
        // Generate a name for this test which captures all the parameters used to execute it.
        var testName = "merge_into_" + spec.into.db + "." + spec.into.coll + "_" +
            (Array.isArray(spec.whenMatched) ? "pipeline" : spec.whenMatched) + "_" +
            spec.whenNotMatched + "_" + matchRatio + "_" + nDocsPerMerge;

        // The target collection is defined by the $merge spec's 'into' parameter.
        var targetColl = db.getSiblingDB(spec.into.db)[spec.into.coll];

        // Clears out the target collection, then copies (nDocsPerMerge*matchRatio) docs from the
        // source collection into the target collection. This ensures that the percentage of
        // documents which trigger the 'whenMatched' condition during the test reflects the
        // 'matchRatio' parameter.
        function populateTargetCollection(numTrialsSoFar) {
            // Drop and recreate the collection so that each time we run the workload we start with
            // a clean slate. We can't simply delete the contents of the collection after each
            // iteration, because that can lead to WT cache issues and excessively noisy results.
            ExecHelpers.resetCollection(targetColl);
            if (matchRatio === 0) {
                return true;
            }
            // In order to ensure predictable results, we need to have a sort order to ensure the
            // same documents make it through the $limit each time. We further have to filter out
            // those which have a null productId, since they will not make it through the $merge.
            assert.doesNotThrow(function() {
                testColl.aggregate([
                    {$match: {productId: {$ne: null}}},
                    {$sort: {_id: 1}},
                    {$limit: Math.round(nDocsPerMerge * matchRatio)},
                    {$merge: {into: spec.into, whenMatched: "fail", whenNotMatched: "insert"}}
                ]);
            });
        }

        // Run a single iteration of the test case. This function is called repeatedly by
        // ExecHelpers.measurePerformance to calculate an average latency for the test.
        function runTrial(numTrialsSoFar) {
            // In order to ensure predictable results, we need to have a sort order to ensure the
            // same documents make it through the $limit each time. We further have to filter out
            // those which have a null productId, since they will not make it through the $merge.
            var pipeline = [
                {$sort: {_id: 1}},
                {$match: {productId: {$ne: null}}},
                {$limit: nDocsPerMerge},
                {$addFields: {fromMerge: true}},
                {$merge: spec}
            ];

            // Pipelines ending with $merge should return 0 results.
            assert.eq(testColl.aggregate(pipeline).itcount(), 0);

            // Test that our $merge worked and produced the right number of documents.
            var totalDocsAfterMerge = targetColl.find({}, {_id: 1}).sort({_id: 1}).itcount();
            assert.eq(totalDocsAfterMerge,
                      nDocsPerMerge,
                      "Expected " + nDocsPerMerge + " documents in '" + targetColl.getFullName() +
                          "' after $merge, but found " + totalDocsAfterMerge);
        }

        // Run the current test configuration for 90 seconds. The 'populateTargetCollection' setup
        // function will be run before each trial but will not be included in the latency stats.
        // In the case of one single document per merge limit the run time to 45 seconds. This is
        // needed because in that case the overhead of setting up the sharded collection and
        // recreating its chunk distribution is much higher than the test's run time. This would
        // lead to very long overall execution times.
        var testSeconds = nDocsPerMerge == 1 ? 45 : 90;
        var perfInfo =
            ExecHelpers.measurePerformance(runTrial, testSeconds, populateTargetCollection);

        // Report the throughput in documents per second to allow easier comparisons between
        // pipelines. This is simply the ops per second multiplied by the number of documents we
        // expect inserted per $merge stage.
        reportThroughput(testName, nDocsPerMerge * perfInfo.meanThroughput, {nThread: 1});
    }

    // We cannot use $-prefixed fieldnames in DSI's configuration files. If 'whenMatched' is a
    // pipeline, then we must prefix the name of each stage in the pipeline with '$'.
    modes.forEach(function(mode) {
        if (Array.isArray(mode.whenMatched)) {
            mode.whenMatched.forEach(function(stageObj) {
                var stageName = Object.keys(stageObj)[0];
                var stageSpec = stageObj[stageName];
                stageObj["$" + stageName] = stageSpec;
                delete stageObj[stageName];
            });
        }
    });

    for (var n = 0; n < nDocsPerMerge.length; n++) {
        for (var r = 0; r < matchRatio.length; r++) {
            for (var m = 0; m < modes.length; m++) {
                for (var t = 0; t < targets.length; t++) {
                    if (!sharded() && targets[t] === "hashed_id") {
                        // On unsharded deployments there are no sharded collections, so skip this
                        // namespace which is meant to be identical to another test but with a
                        // different distribution.
                        continue;
                    }
                    runMerge(nDocsPerMerge[n], matchRatio[r], {
                        into: {db: targetDB, coll: targets[t]},
                        whenMatched: modes[m].whenMatched,
                        whenNotMatched: modes[m].whenNotMatched
                    });
                }
            }
        }
    }
}());
