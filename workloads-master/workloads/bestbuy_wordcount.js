/**
 *
 * @file
 * Measure performance of aggregation's $merge stage against the
 * {@link https://bestbuyapis.github.io/api-documentation/#overview|BestBuy Developer API data},
 * specifically stressing the exchange logic and comparing the performance against the mapReduce
 * command for older branches less than version 4.4 and earlier, for newer branches >=4.5 just
 * aggregation pipelines will be exercised. Each of the operations will compute the same thing:
 * a histogram of words in the 'name' field of each game or software product in the database,
 * something like: {_id: "word", count: 32}. The results will be spilled to a collection using
 * either $merge or the 'output' option to mapReduce. To stress the exchange optimization in a
 * sharded deployment, that collection is expected to be set up as a sharded collection, though
 * the test will still work in unsharded deployments.
 *
 * ### *Pre-requisite*
 * The dataset must be installed on the target cluster before running the test. The data can be
 *  downloaded  from
 * {@link
 * https://s3-us-west-2.amazonaws.com/dsi-donot-remove/AggPerformance/bestbuyproducts.bson.gz|here}
 *  and installed using mongorestore (mongorestore --gzip --archive=bestbuyproducts.bson.gz)
 *
 * In a sharded cluster, the target collection ('target_range_id') is expected to be sharded by the
 * key {_id: 1} and have chunks distributed amongst the shards.
 *
 * ### *Setup*
 * None
 *
 * ### *Test*
 *
 * The tests use a simple for loop of 2 minutes to repeatedly run a query which computes the word
 * count in the names of products. The computation is performed in three ways, once with $merge with
 * the exchange optimization enabled, and once with $merge with the exchange optimization disabled.
 * Each run will report the throughput in documents processed per second.
 *
 * ### *Owning-team*
 * mongodb/product-query
 * 
 * ### *Keywords*
 * 
 * @module workloads/bestbuy_wordcount
 */

(function() {
    "use strict";

    load("utils/exec_helpers.js");  // For 'ExecHelpers'.

    var majorVersion = db.version().split(".")[0];
    var minorVersion = db.version().split(".")[1];
    if (majorVersion < 4 || (majorVersion == 4 && minorVersion < 1)) {
        jsTestLog("New $merge features are not available on version " + tojson(serverVersion) +
                  ". Skipping test.");
        return;
    }

    var testDb = db.getSiblingDB(testDbName);
    var testColl = testDb[testCollName];

    // A predicate used to disable tests from running the mapReduce command on branches >= 4.5
    function isLegacyMRVersion() {
      return (majorVersion == 4 && minorVersion < 5) || majorVersion < 4;
    }

    function reportThroughputFromPerfInfo(testName, nDocsPerCommand, perfInfo) {
        // Report the throughput in documents per second to allow easier comparisons between
        // pipelines. This is simply the ops per second multiplied by the number of documents we
        // expect inserted per $merge stage.
        reportThroughput(testName, nDocsPerCommand * perfInfo.meanThroughput, {nThread: 1});
    }

    function measureWordCountPerformanceUsingAgg(testName, nDocsPerPipeline, pipeline) {
        var targetColl = db.target_range_id;
        // This target collection is sharded before we start - so be careful to just remove
        // everything without dropping it.
        assert.writeOK(targetColl.remove({}));

        var perfInfo = ExecHelpers.measurePerformance(function() {
            testColl.aggregate(pipeline).itcount();
        });
        reportThroughputFromPerfInfo(testName, nDocsPerPipeline, perfInfo);
    }

    function measureWordCountPerformanceUsingMapReduce(
        testName, nDocsPerCommand, map, reduce, options) {
        var targetColl = db.target_range_id;
        // This target collection is sharded before we start - so be careful to just remove
        // everything without dropping it.
        assert.writeOK(targetColl.remove({}));

        if (majorVersion < 4 || (majorVersion == 4 && minorVersion < 3)) {
            // On 4.2 and below this option is needed to output to a sharded collection and ignored
            // on unsharded deployments. On newer versions, this option is rejected on unsharded
            // deployments and unnecessary for outputting to a sharded colection.
            options.out.sharded = true;
        }

        var perfInfo = ExecHelpers.measurePerformance(function() {
            assert.commandWorked(testColl.mapReduce(map, reduce, options));
        });

        reportThroughputFromPerfInfo(testName, nDocsPerCommand, perfInfo);
    }

    //
    // Measure the performance of a computation which computes word counts in the name of software
    // and game products. We'll measure performance without outputting results to a collection,
    // outputting results with the exchange optimization disabled and then enabled, and finally by
    // using the mapReduce command. This calculation begins with a filter which is expected to be
    // indexed and also expected to limit the number of contacted shards in a sharded cluster.
    //
    (function softwareAndGameProductNameWordCounts() {
        var wordCountPipeline = [
            {$match: {type: {$in: ["Software", "Game"]}}},
            {$project: {wordOfName: {$split: ["$name", " "]}}},
            {$unwind: "$wordOfName"},
            {$group: {_id: "$wordOfName", count: {$sum: 1}}},
        ];
        var nDocsPerPipeline =
            testColl.aggregate(wordCountPipeline.concat([{$count: "count"}])).next().count;
        measureWordCountPerformanceUsingAgg(
            "filtered_word_count_no_merge", nDocsPerPipeline, wordCountPipeline);

        var wordCountPipelineWithMerge = wordCountPipeline.concat([{
            $merge: {
                into: "target_range_id",
                on: "_id",
                whenMatched: "replace",
                whenNotMatched: "insert"
            }
        }]);

        if (sharded()) {
            assert.commandWorked(
                db.adminCommand({setParameter: 1, internalQueryDisableExchange: true}));
            measureWordCountPerformanceUsingAgg(
                "filtered_word_count_no_exchange", nDocsPerPipeline, wordCountPipelineWithMerge);

            assert.commandWorked(
                db.adminCommand({setParameter: 1, internalQueryDisableExchange: false}));
            var explain = testColl.explain().aggregate(wordCountPipelineWithMerge);
            assert.commandWorked(explain);
            assert.eq(explain.mergeType, "exchange", tojson(explain));
            measureWordCountPerformanceUsingAgg(
                "filtered_word_count_use_exchange", nDocsPerPipeline, wordCountPipelineWithMerge);
        } else {
            measureWordCountPerformanceUsingAgg(
                "filtered_word_count", nDocsPerPipeline, wordCountPipelineWithMerge);
        }

        if (isLegacyMRVersion()) {
          measureWordCountPerformanceUsingMapReduce(
              "filtered_word_count_map_reduce",
              nDocsPerPipeline,
              function map() {
                  if (!this.name) {
                      return;
                  }
                  var words = this.name.split(" ");
                  for (var i = 0; i < words.length; ++i) {
                      emit(words[i], 1);
                  }
              },
              function reduce(key, values) {
                  return Array.sum(values);
              },
              {out: {merge: "target_range_id"}, query: {type: {$in: ["Game", "Software"]}}});
        }
    }());

    //
    // Measure the performance of calculating the word counts of all 'long descriptions' of the
    // product catalog. Again, we'll measure performance without outputting results to a collection,
    // outputting results with the exchange optimization disabled and then enabled. This computation
    // must look at all documents in the collection and is not expected to be able to take advantage
    // of shard targeting or indexes.
    //
    (function longDescriptionWordCount() {
        var wordCountPipeline = [
            {$project: {wordOfDesc: {$split: ["$longDescription", " "]}}},
            {$unwind: "$wordOfDesc"},
            {$group: {_id: "$wordOfDesc", count: {$sum: 1}}},
        ];

        var nDocsPerPipeline =
            testColl.aggregate(wordCountPipeline.concat([{$count: "count"}])).next().count;
        measureWordCountPerformanceUsingAgg(
            "all_word_count_no_merge", nDocsPerPipeline, wordCountPipeline);

        var wordCountPipelineWithMerge = wordCountPipeline.concat([{
            $merge: {
                into: "target_range_id",
                on: "_id",
                whenMatched: "replace",
                whenNotMatched: "insert"
            }
        }]);
        if (sharded()) {
            assert.commandWorked(
                db.adminCommand({setParameter: 1, internalQueryDisableExchange: true}));
            measureWordCountPerformanceUsingAgg(
                "all_word_count_no_exchange", nDocsPerPipeline, wordCountPipelineWithMerge);

            assert.commandWorked(
                db.adminCommand({setParameter: 1, internalQueryDisableExchange: false}));
            var explain = testColl.explain().aggregate(wordCountPipelineWithMerge);
            assert.commandWorked(explain);
            assert.eq(explain.mergeType, "exchange", tojson(explain));
            measureWordCountPerformanceUsingAgg(
                "all_word_count_use_exchange", nDocsPerPipeline, wordCountPipelineWithMerge);
        } else {
            measureWordCountPerformanceUsingAgg(
                "all_word_count", nDocsPerPipeline, wordCountPipelineWithMerge);
        }

        if (isLegacyMRVersion()) {
          function map() {
                if (!this.longDescription) {
                    return;
                }
                var words = this.longDescription.split(" ");
                for (var i = 0; i < words.length; ++i) {
                    emit(words[i], 1);
                }
            }
            function reduce(key, values) {
                return Array.sum(values);
            }
            var outOptions = {out: {merge: "target_range_id"}};
            measureWordCountPerformanceUsingMapReduce(
                "all_word_count_map_reduce", nDocsPerPipeline, map, reduce, outOptions);
      }
    }());
}());
