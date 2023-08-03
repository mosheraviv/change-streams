(function() {
    'use strict';
    load('libs/election_timing_test.js');

    // Make and reset the dbpath.
    mkdir('data/db');
    MongoRunner.dataPath = 'data/db/';
    MongoRunner.dataDir = 'data/db';

    function workload(port) {
        if (authEnabled){
            var portAuth = 'mongodb://'.concat(username, ':', password, '@', port);
        } else {
            portAuth = port;
        }
        var conn = new Mongo(portAuth);
        Random.setRandomSeed();

        while (true) {
            var coll = conn.getDB('test').vehicles;
            var bulk = coll.initializeUnorderedBulkOp();

            for (var i = 0; i < 1000; i++) {
                var doc = {
                    dateAndTime: Date.now(),
                    vehicleIdentNum: Random.randInt(1000),
                    vehicleMake: Random.randInt(100000),
                    vehicleModel: Random.randInt(10000),
                    price: ((Random.rand() * 500) + Random.randInt(100000)) / 100
                };
                bulk.insert(doc);
            }
            assert.writeOK(bulk.execute({w: 'majority'}));
            sleep(Random.randInt(15));
        }
    }

    var runWorkload = function() {
        var primary = this.rst.getPrimary();
        var coll = primary.getDB('test').vehicles;
        coll.drop();
        assert.commandWorked(coll.createIndex({price: 1, vehicleMake: 1}));
        assert.commandWorked(coll.createIndex({vehicleIdentNum: 1, price: 1, vehicleMake: 1}));
        assert.commandWorked(coll.createIndex({price: 1, dateAndTime: 1, vehicleMake: 1}));

        // Create workload threads.
        this.threads = [];
        for (var i = 0; i < 30; i++) {
            var t = new ScopedThread(workload, primary.host);
            this.threads.push(t);
            t.start();
        }
        // Sleep here to give the workload a chance to get operations in flight and create
        // a reasonable load on the server before starting the failover tests.
        sleep(20 * 1000);
    };

    var testReset = function() {
        this.threads.forEach(function(t) { t.join(); });
        this.stopPrimaryReset();
        // Make sure the secondaries are up.
        this.rst.awaitSecondaryNodes();
    };

    var hardStop = function() {
        this.originalPrimary = this.rst.getNodeId(this.rst.getPrimary());
        this.rst.stop(this.originalPrimary, 9 /* SIGKILL */);
    };

    var testCases = [
        {
            name: 'loadedStop',
            description: 'pv1 under load, primary is stopped',
            protocolVersion: 1,
            // Additional options to pass to mongod.
            nodeOptions: {wiredTigerCacheSizeGB: 1},
            // testRuns is the number of times a new ReplSetTest will be used.
            testRuns: 5,
            // testCycles: the number of election cycles that will be run per ReplSetTest lifespan.
            testCycles: 5,
            // testSetup is run after the replSet is initiated.
            // Function.prototype is the default.
            testSetup: Function.prototype,
            // Function that puts load on the primary.
            runWorkload: runWorkload,
            // Trigger an election by stepping down, stopping, or partitioning the primary.
            // stopPrimary is the default.
            electionTrigger: ElectionTimingTest.prototype.stopPrimary,
            // After the election has completed, make the old primary available again.
            // stopPrimaryReset is the default.
            testReset: testReset
        },
        {
            name: 'loadedStop1500',
            description: 'pv1 under load, primary is stopped, electionTimeoutMillis 1500',
            protocolVersion: 1,
            testRuns: 5,
            testCycles: 5,
            nodeOptions: {wiredTigerCacheSizeGB: 1},
            settings: {electionTimeoutMillis: 1500},
            testSetup: Function.prototype,
            runWorkload: runWorkload,
            electionTrigger: ElectionTimingTest.prototype.stopPrimary,
            testReset: testReset
        },
        {
            name: 'loadedHardStop',
            description: 'pv1 under load, primary is hard stopped',
            protocolVersion: 1,
            testRuns: 5,
            testCycles: 5,
            nodeOptions: {wiredTigerCacheSizeGB: 1},
            testSetup: Function.prototype,
            runWorkload: runWorkload,
            electionTrigger: hardStop,
            testReset: testReset
        },
        {
            name: 'loadedHardStop1500',
            description: 'pv1 under load, primary is hard stopped, electionTimeoutMillis 1500',
            protocolVersion: 1,
            testRuns: 5,
            testCycles: 5,
            nodeOptions: {wiredTigerCacheSizeGB: 1},
            settings: {electionTimeoutMillis: 1500},
            testSetup: Function.prototype,
            runWorkload: runWorkload,
            electionTrigger: hardStop,
            testReset: testReset
        }
    ];

    var run_tests = function() {
        // Don't run these tests against sharded clusters.
        if (sharded()) {
            return;
        }
        quiesceSystem();
        testCases.forEach(function(tc) {
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

            tc.testResults.forEach(function(tr) {
                allResults = allResults.concat(tr.results);
            });

            print('Results: ' + tc.name);
            var resAvg = Array.avg(allResults);
            var resMin = Math.min.apply(null,allResults);
            var resMax = Math.max.apply(null,allResults);
            // Array.sort() does lexicographical order by default
            var resMed = allResults.sort(function(a,b) { return a - b; })[allResults.length / 2];
            // The number of threads is constant in the workload. Overloading the nThreads field to
            // hold the number of runs in the test case, a much more interesting number.
            res.nThread = allResults.length;

            print('Average over ' +  allResults.length + ' runs: ' + resAvg);
            print('Min: ' +  resMin + ' Max: ' + resMax + ' Median: ' + resMed);
            reportThroughput(tc.name, resAvg, res);
        });
    };

    run_tests();

})();
