/**
 * @file
 *
 * Execute a mix of CRUD operations in their own independent threads.
 *
 * ### *Test*
 *
 *   Execute an even mix of insert, read, update and deletes, each in their own independent thread.
 *
 *   Origin as a repro for {@link https://jira.mongodb.org/browse/BF-2385|BF-2385}. (Code originates as copied
 *   from move_chunk_with_load.js.)
 *
 * Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 *   All targets (standalone, replica, sharded)
 *
 * ### *Notes*
 *
 * - {@link https://jira.mongodb.org/browse/SERVER-29764|SERVER-29764} 'benchRun is limited to
 *   128 threads'
 * - To work around {@link https://jira.mongodb.org/browse/SERVER-29764|SERVER-29764}, 4 javascript
 *   scoped threads are launched to run 4 instances of benchRun. However, it turns out this was
 *   also a key property of this test without which
 *   {@link https://jira.mongodb.org/browse/BF-2385|BF-2385} wouldn't have happened: It is only
 *   because each type of CRUD operation is running in separate threads, that it is possible for
 *   the operations to become unbalanced.
 *
 * ### *Owning-team*
 * mongodb/product-perf
 * 
 * ### *Keywords*
 * 
 * @module workloads/mix
 */
load("libs/mixed_workload.js");

/*global
  db sharded enableSharding reportThroughput sleep server jsTest
*/

////////////////////////////////////////////////////////////////////////////////
// Start of global scope variables

/**
 * The number of threads to run in parallel. The default is [4, 64, 128, 256, 512].
 *
 * **Note:** Thread level should be divisible by 4.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var thread_levels = thread_levels || [4, 64, 128, 256, 512]; // Thread counts should be divisible by 4.

/**
 * Whether to run this test with retryable writes. Defaults to false.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var retryable_writes = retryable_writes || false;

// End of global scope variables
////////////////////////////////////////////////////////////////////////////////

(function () {
    var testDurationSecs = 7*60; // test duration/thread level. Note: stalls come in 60 sec cycles.
    var dbName = "mix";

    function testMixedLoad(collName, nThreads, retryableWrites) {
        var workload = new MixedCRUDWorkload(dbName, [collName], nThreads, retryableWrites);
        workload.initialize();

        jsTest.log("Mixed load: nThreads=" + nThreads + ", retryableWrites=" + retryableWrites + " " + (new Date()));

        workload.start();
        sleep(testDurationSecs*1000);
        var throughput = workload.stop();

        var reportNameSuffix = (retryableWrites ? "_retry" : "");
        reportThroughput("mixed_findOne" + reportNameSuffix, throughput.findOne, {nThread: nThreads});
        reportThroughput("mixed_insert" + reportNameSuffix, throughput.insert, {nThread: nThreads});
        reportThroughput("mixed_update" + reportNameSuffix, throughput.update, {nThread: nThreads});
        reportThroughput("mixed_delete" + reportNameSuffix, throughput.delete, {nThread: nThreads});
        reportThroughput("mixed_total" + reportNameSuffix, throughput.total, {nThread: nThreads});
    }

    thread_levels.forEach(function(nThreads) {
        testMixedLoad("mixed_" + nThreads, nThreads, retryable_writes);
    });
})();
