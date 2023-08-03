(function() {
    'use strict';
    load('libs/election_timing_test.js');
    // Make and reset the dbpath.
    mkdir('data/db');
    MongoRunner.dataPath = 'data/db/';
    MongoRunner.dataDir = 'data/db';

    var testCases = [
    {
        name: 'testV1Stop',
        description: 'protocolVersion 1, primary is stopped',
        protocolVersion: 1,
        // testRuns is the number of times a new ReplSetTest will be used.
        testRuns: 10,
        // testCycles is the number of election cycles that will be run per ReplSetTest lifespan.
        testCycles: 5,
        // testSetup is run after the replSet is initiated.
        // Function.prototype is the default.
        testSetup: Function.prototype,
        // Trigger an election by stepping down, stopping, or partitioning the primary.
        // stopPrimary is the default.
        electionTrigger: ElectionTimingTest.prototype.stopPrimary,
        // After the election has completed, make the old primary available again.
        // stopPrimaryReset is the default.
        testReset: ElectionTimingTest.prototype.stopPrimaryReset
    },

    {
        name: 'testV1StopTimeout1500',
        description: 'protocolVersion 1, primary is stopped, electionTimeoutMillis set to 1500',
        protocolVersion: 1,
        testRuns: 10,
        testCycles: 5,
        // The settings object is merged into the replset config settings object.
        settings: {electionTimeoutMillis: 1500}
    },

    {
        name: 'testV0Stop',
        description: 'protocolVersion 0, primary is stopped',
        protocolVersion: 0,
        testRuns: 10,
        testCycles: 5,
    },

    {
        name: 'testV1StepDown',
        description: 'protocolVersion 1, primary is stepped down',
        protocolVersion: 1,
        testRuns: 10,
        testCycles: 5,
        stepDownGuardTime: 30,
        electionTrigger: ElectionTimingTest.prototype.stepDownPrimary,
        testReset: ElectionTimingTest.prototype.stepDownPrimaryReset
    },

    {
        name: 'testV1StepDownTimeout1500',
        description: 'protocolVersion 1, primary is stepped down, electionTimeoutMillis is 1500',
        protocolVersion: 1,
        testRuns: 10,
        testCycles: 5,
        stepDownGuardTime: 30,
        settings: {electionTimeoutMillis: 1500},
        electionTrigger: ElectionTimingTest.prototype.stepDownPrimary,
        testReset: ElectionTimingTest.prototype.stepDownPrimaryReset
    },

    {
        name: 'testV0StepDown',
        description: 'protocolVersion 0, primary is stepped down',
        protocolVersion: 0,
        testRuns: 10,
        testCycles: 5,
        stepDownGuardTime: 30,
        // There is a guard time in pv0 that prevents an election right
        // after initiating.
        testSetup: function() {sleep(30 * 1000);},
        electionTrigger: ElectionTimingTest.prototype.stepDownPrimary,
        testReset: ElectionTimingTest.prototype.stepDownPrimaryReset
    } ];

    var run_tests = function() {
        // Don't run these tests against sharded clusters.
        quiesceSystem();
        if (sharded()) {
            return;
        }
        testCases.forEach(function (tc) {
            var testRun = new ElectionTimingTest(tc);
            tc.testResults = testRun.testResults;
            var res = {nThread: 0, pass: true};

            if (testRun.testErrors.length) {
                jsTestLog('Errors from: ' + tc.name);
                printjson(testRun.testErrors);
                res.errMsg = testRun.testErrors.toString();
                res.pass = false;
            }

            var allResults = [];

            tc.testResults.forEach(function (tr) {
                allResults = allResults.concat(tr.results);
            });

            print('Results: ' + tc.name);
            var resAvg = Array.avg(allResults);
            var resMin = Math.min.apply(null,allResults);
            var resMax = Math.max.apply(null,allResults);
            // Array.sort() does lexicographical order by default
            var resMed = allResults.sort(function(a,b) { return a - b; })[allResults.length / 2];
            res.nThread = allResults.length;

            print('Average over ' +  allResults.length + ' runs: ' + resAvg);
            print('Min: ' +  resMin + ' Max: ' + resMax + ' Median: ' + resMed);
            reportThroughput(tc.name, resAvg, res);
        });
    };

    run_tests();
}());
