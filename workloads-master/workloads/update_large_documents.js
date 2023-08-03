/**
 * @file
 * Measures the performance of updates on large documents.
 *
 * This workload exerts pressure on WiredTiger's cache management and cache eviction policy.
 *
 * ### *Test*
 *
 * The following operations are tested:
 * * Initially, the collection is populated with a fixed number of large documents
 *   (using a single 10 MB-long string field).
 * * Each document contains a non-id numerical field that will be modified by the
 *   update operations in the workload.
 * * The workload to be measured is comprised exclusively of update operations that
 *   repeatedly increment the numerical field in every document in the collection.
 * * The document sized wll remain unchanged because the long string field is left intact
 *   by the updates.
 * * Since the targets of the update operations are not randomized, all documents will
 *   contain the same value for the numerical field at the end of the workload.
 *
 * Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 * * A fixed number of large documents are inserted as part of the test initialization.
 *
 * ### *Notes*
 * 1. For all the tests, a varying number of threads is used.
 * +  We also use varying durations for the the runtime for the test.
 * +  The minimum size of each document can also be varied (defaults to 10 MB).
 * +  This test was motivated by a performance regression reported in 3.6 (see
 *    {@link https://jira.mongodb.org/browse/SERVER-36221|SERVER-36221}).
 *
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 *
 * @module workloads/update_large_documents
 */
/* global db quiesceSystem */
/* global benchRun tojson assert  */
/* global reportThroughput */

/**
 * The number of large documents to insert into the collection during setup. The default is [1].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from the config
 * file, see {@link
 * https://github.com/10gen/dsi/blob/740770d89f66fa18dcfd88531b47f5af727b9e84/configurations/test_control/test_control.misc_workloads.yml#L28-L32|test_control.misc_workloads.yml}.
 *
 */
var doc_counts = doc_counts || [2];

/**
 * The size in MB of each large document in the collection. The default is [10].
 * This should be treated as a minimum rather than an exact size due to the BSON overhead
 * and the additional numerical field incremented by the update operations.
 */
var doc_sizes_mb = doc_sizes_mb || [10];

/**
 * The number of threads to run in parallel. The default is [1].
 */
var thread_levels = thread_levels || [1];

/**
 * The duration of each test iteration in seconds. The default is [180].
 */
var runtime_secs = runtime_secs || [180];

var runTest = function(numDocs, minDocSizeMB, numThreads, testDurationSecs) {
    var testName = 'xldoc_' + humanReadableNumber(numDocs) + 'docs_' +
        humanReadableNumber(minDocSizeMB * 1024 * 1024) + 'B';
    var mydb = db.getSiblingDB('large_documents_db');
    var coll = mydb.getCollection('update' + numDocs.zeroPad(3) + minDocSizeMB.zeroPad(2) +
                                  numThreads.zeroPad(2) + testDurationSecs.zeroPad(4));

    // Logging function that prepends test name to message.
    var logPrefix = '[' + testName + '] ';
    var testLog = function(msg) {
        jsTestLog(logPrefix + msg);
    };

    testLog('Test starting: numDocs=' + numDocs + '; minDocSize=' + minDocSizeMB +
            'MB; numThreads=' + numThreads + '; duration=' + testDurationSecs + 's');

    coll.drop();
    testLog('Collection: ' + coll.getFullName());

    for (var i = 0; i < numDocs; ++i) {
        assert.writeOK(coll.save({_id: i, i: 0, x: 'x'.repeat(minDocSizeMB * 1024 * 1024)}));
    }
    assert.eq(numDocs, coll.find().itcount());
    assert(quiesceSystem());
    testLog('Inserted ' + numDocs + ' documents (' + minDocSizeMB + ' MB each).');

    var updateMultiOp = {
        op: 'update',
        ns: coll.getFullName(),
        update: {$inc: {i: 1}},
        writeCmd: true,
    };
    var ops = Array(numDocs).fill().map(function(v, i) {
        return Object.merge(updateMultiOp, {query: {_id: i}});
    });
    var benchResults = benchRun({
        ops: ops,
        parallel: numThreads,
        seconds: testDurationSecs,
        host: db.getMongo().host,
        username: username,
        password: password
    });
    testLog('Raw benchRun() results: ' + tojson(benchResults));

    var numOps = benchResults['totalOps'];
    var opsPerSec = benchResults['totalOps/s'];
    testLog('Total number of operations: ' + numOps);
    testLog('Number of operations per second: ' + opsPerSec.toFixed(2));

    reportThroughput(testName, opsPerSec, {nThread: numThreads});
    testLog('Test finished');
};

var i = 0;
doc_counts.forEach(function(numDocs) {
    jsTestLog('update_large_documents: ' + i + ': numDocs=' + numDocs);
    doc_sizes_mb.forEach(function(minDocSizeMB) {
        jsTestLog('update_large_documents: ' + i + ': minDocSizeMB=' + minDocSizeMB);
        thread_levels.forEach(function(numThreads) {
            jsTestLog('update_large_documents: ' + i + ': numThreads=' + numThreads);
            runtime_secs.forEach(function(testDurationSecs) {
                jsTestLog('update_large_documents: ' + i + ': testDurationSecs=' +
                          testDurationSecs);
                runTest(numDocs, minDocSizeMB, numThreads, testDurationSecs);
                i++;
            });
        });
    });
});
