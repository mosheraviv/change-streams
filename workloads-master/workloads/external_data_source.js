/**
 * @file
 * Measures performance of $_externalDataSources aka named pipes, a mongod feature provided for ADL.
 * Uses data from the
 * {@link https://drive.google.com/drive/folders/1xC_aAtY_W8Hn5zQq5n7opd5N4NBz1lmq|ClickBench benchmark data},
 *
 * Results are reported as floating point trials per second. Each trial runs an aggregation on an
 * external data source (named pipe) containing one million BSON objects. 
 *
 * ### *Pre-requisite*
 * The dataset will be downloaded from
 * {@link
 * https://s3-us-west-2.amazonaws.com/dsi-donot-remove/Query/Benchmarks/ClickBench/hits.100000.bson.gz|here}.
 *
 * ### *Setup*
 * None
 *
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 * named_pipes, external_data_source, adl, atlas_data_lake
 *
 * @module workloads/external_data_source
 */

(function() {
"use strict";

load("utils/mongoshell.js");  // for quiesceSystem(), reportThroughput()

////////////////////////////////////////////////////////////////////////////////////////////////////
// Parameters to be set by the framework from the test control file
//   dsi/configurations/test_control/test_control.external_data_source.yml
//
// testIdx (int) - Index into kAggPipelines[], kTestNames[] arrays for the current test case.
//   This is needed for aggregation piplines because DSI DOES NOT SUPPORT PASSING ARBITRARY STRING
//   VALUES! They seem to be required to be valid JSON key names. This is also used for test names
//   so they do not need to be specified in test_control.external_data_source.yml.
// threads (int) - Number of concurrent threads for queries; each will have a second thread for the
//   pipe writer.
////////////////////////////////////////////////////////////////////////////////////////////////////

const kAggPipelines = [
    [{ $group: { _id: "$UserID" } }],             // 0
    [{ $match: { SearchPhrase: { $ne: "" } } }],  // 1
    [{ $project: { SearchPhrase: 1 } }],          // 2
    [{ $sort: { EventTime: 1 } }]                 // 3
];
const kTestNames = [
    "external_data_source_" + threads + "_group",   // 0
    "external_data_source_" + threads + "_match",   // 1
    "external_data_source_" + threads + "_project", // 2
    "external_data_source_" + threads + "_sort"     // 3
];
const kResult = "RESULT: ";  // for easily grepping informational messages
const kUrlProtocolFile = "file://";  // required by Named Pipes feature implementation

////////////////////////////////////////////////////////////////////////////////////////////////////
// Creates an object suitable to run the desired pipeline via benchRunOnce() for 'threads' number
// of concurrent queries.
////////////////////////////////////////////////////////////////////////////////////////////////////
function createBenchRunOnceObject() {
    let opsArray = new Array(threads);
    for (var opIdx = 0; opIdx < threads; ++opIdx) {
        opsArray[opIdx] = {
            op: "command",
            ns: "test",  // database name
            writeCmd: true,
            command: {
                aggregate: "coll_" + opIdx,  // collection name
                cursor: {},
                pipeline: kAggPipelines[testIdx],
                $_externalDataSources: [{
                    collName: "coll_" + opIdx,
                    dataSources: [{
                        url: kUrlProtocolFile + "workloads_external_data_source_" + opIdx,
                        storageType: "pipe",
                        fileType: "bson"
                    }]
                }]
            }
        };
    }
    return {ops: opsArray, host: server, username: username, password: password};
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Run the benchmark and report results.
////////////////////////////////////////////////////////////////////////////////////////////////////
print(`\n${kResult} Starting benchmark run ${kTestNames[testIdx]}.`);
quiesceSystem();
const throughput = benchRunOnce(createBenchRunOnceObject())["totalOps/s"];
print(`${kResult} Finished benchmark run ${kTestNames[testIdx]}. throughput (queries / sec): ${throughput}\n\n`);

reportThroughput(kTestNames[testIdx], throughput, {nThread: threads});

})();
