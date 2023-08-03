/**
 * @file
 * Measure performance of analytics workloads on the
 * {@link https://bestbuyapis.github.io/api-documentation/#overview|BestBuy Developer API data},
 * The workload size can be scaled with the testScale variable.  When set to testScale=4, we should
 * see CPU bound behavior with our standard-sized instances and see IO-bound behavior when running
 * on small instances with only 4gb of RAM.
 *
 * With enableColumnstore = "True", a columnstore index will be created before running queries.
 *
 * Results are reported as ops / second.
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
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 * columnstore, analytics
 *
 * @module workloads/bestbuy_analytics
 */

(function() {
"use strict";

load("utils/exec_helpers.js");  // For 'ExecHelpers'.

var testDb = db.getSiblingDB(testDbName);
var testColl = testDb[testCollName];

// The number of seconds to use when measuring throughput with 'ExecHelpers.measurePerformance().
// This test is designed mainly to be run on low-memory instances (4GB RAM).
var TEST_SECONDS = 2 * 60;

function measurePerformanceUsingAgg(testName, pipeline) {
    var perfInfo = ExecHelpers.measurePerformance(function() {
        testColl.aggregate(pipeline).itcount();
    }, TEST_SECONDS);
    reportThroughput(testName, perfInfo.meanThroughput, {nThread: 1});
}

var tests = [
    {name: "1_col", pipeline: [{$group: {_id: '$type', count: {$sum: 1}}}]},
    // The following cannot be satisfied by a traditional index, due to SERVER-12869.
    {
        name: "1_col_exists",
        pipeline: [{$match: {'type': {$exists: true}}}, {$group: {_id: '$type', count: {$sum: 1}}}]
    },
    {
        name: "1_col_nomatch_scalar",
        pipeline: [{$match: {'type': 'asdf'}}, {$group: {_id: '$type', count: {$sum: 1}}}]
    },
    {
        name: "2_col_nomatch_array",
        pipeline:
            [{$match: {'categoryPath.name': 'asdf'}}, {$group: {_id: '$type', count: {$sum: 1}}}]
    },
    {
        name: "2_col_selective",
        pipeline: [
            {$match: {'categoryPath.name': 'Pop', type: 'Movie'}},
            {$group: {_id: '$type', count: {$sum: 1}}}
        ]
    },
    {
        name: "2_col_unselective",
        pipeline: [
            {$match: {'categoryPath.name': 'Pop', type: 'Music'}},
            {$group: {_id: '$type', count: {$sum: 1}}}
        ]
    },
    {
        name: "group_2_col",
        pipeline: [{
            $group: {
                _id: {classId: '$classId', subclassId: '$subclassId'},
                class: {$first: '$class'},
                subclass: {$first: '$subclass'},
                count: {$sum: 1}
            }
        }]
    },
    // Simple group with complex operation on each doc.
    // Counts the products updated in each hour of the day.
    {
        name: "group_hour_in_date",
        pipeline: [{
            $group: {
                _id: {
                    $hour: {
                        date: {
                            $dateFromString: {
                                dateString: "$itemUpdateDate",
                                format: "%Y-%m-%dT%H:%M:%S",
                                timezone: "UTC"
                            }
                        },
                        timezone: "GMT"
                    }
                },
                count: {$sum: 1}
            }
        }]
    },
    // Match on several fields.
    // Traditional indexes in the original Best Buy dump would help with this one.
    {
        name: "match_3_column",
        pipeline: [
            {
                $match: {
                    $and: [
                        {salePrice: {$gt: 10, $lt: 100}},
                        {type: {$eq: "Software"}},
                        {digital: {$eq: false}}
                    ]
                }
            },
            {$group: {_id: null, avgPrice: {$avg: '$salePrice'}}}
        ]
    },
    {
        name: "match_10_column",
        pipeline: [
            {
                $match: {
                    $and: [
                        {sku: {$gt: 10000000}},
                        {productId: {$gt: 1000}},
                        {new: {$eq: false}},
                        {active: {$eq: true}},
                        {advertisedPriceRestriction: {$eq: false}},
                        {freeShipping: {$eq: false}},
                        {onlineAvailability: {$eq: true}},
                        {salePrice: {$gt: 10, $lt: 100}},
                        {type: {$eq: "Software"}},
                        {digital: {$eq: false}}
                    ]
                }
            },
            {$group: {_id: null, avgPrice: {$avg: '$salePrice'}}}
        ]
    },
    {
        name: "sort_limit_project_4",
        pipeline: [
            {$sort: {salePrice: -1}},
            {$limit: 10},
            {$project: {_id: 0, name: 1, type: 1, department: 1, salePrice: 1}}
        ]
    },
    {
        name: "sort_limit_project_10",
        pipeline: [
            {$sort: {salePrice: -1}},
            {$limit: 10},
            {
                $project: {
                    _id: 0,
                    sku: 1,
                    productId: 1,
                    new: 1,
                    active: 1,
                    advertisedPriceRestriction: 1,
                    freeShipping: 1,
                    onlineAvailability: 1,
                    salePrice: 1,
                    type: 1,
                    digital: 1
                }
            }
        ]
    },
    {
        name: "sort_limit_full_doc",
        pipeline: [
            {$sort: {salePrice: -1}},
            {$limit: 10},
        ]
    }
];

// Run tests
for (var i = 0; i < tests.length; i++) {
    measurePerformanceUsingAgg(tests[i].name, tests[i].pipeline);
}
}());
