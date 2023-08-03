/**
 * @file
 *
 * Util functions to be used as mongo shell rc file
 *
 * @module utils/mongoshell
 */

/**
 * copying Thread, ScopedThread, and CountDownLatch from parallelTester.js since we can't
 * import that file.
*/
if (typeof _threadInject != "undefined") {
    // With --enableJavaScriptProtection functions are presented as Code objects.
    // This function evals all the Code objects then calls the provided start function.
    // arguments: [startFunction, startFunction args...]
    function _threadStartWrapper() {
        // Recursively evals all the Code objects present in arguments
        // NOTE: This is a naive implementation that cannot handle cyclic objects.
        function evalCodeArgs(arg) {
            if (arg instanceof Code) {
                return eval("(" + arg.code + ")");
            } else if (arg !== null && isObject(arg)) {
                var newArg = arg instanceof Array ? [] : {};
                for (var prop in arg) {
                    if (arg.hasOwnProperty(prop)) {
                        newArg[prop] = evalCodeArgs(arg[prop]);
                    }
                }
                return newArg;
            }
            return arg;
        }
        var realStartFn;
        var newArgs = [];
        for (var i = 0, l = arguments.length; i < l; i++) {
            newArgs.push(evalCodeArgs(arguments[i]));
        }
        realStartFn = newArgs.shift();
        return realStartFn.apply(this, newArgs);
    }

    Thread = function() {
        var args = Array.prototype.slice.call(arguments);
        args.unshift(_threadStartWrapper);
        this.init.apply(this, args);
    };
    _threadInject(Thread.prototype);

    ScopedThread = function() {
        var args = Array.prototype.slice.call(arguments);
        args.unshift(_threadStartWrapper);
        this.init.apply(this, args);
    };
    ScopedThread.prototype = new Thread(function() {});
    _scopedThreadInject(ScopedThread.prototype);
}

/**
 * To use a CountDownLatch, first create one in the parent thread and then pass it into
 * a ScopedThread as a function argument:
 *      var latch = new CountDownLatch(1);
 *      var t = new ScopedThread(funcA, latch);
 *
 * You may then lower the count of the latch by calling latch.countDown() and get the value
 * of the latch with latch.getCount() from any thread.
 */
if ( typeof CountDownLatch !== 'undefined' ) {
    CountDownLatch = Object.extend(function(count) {
        if (! (this instanceof CountDownLatch)) {
            return new CountDownLatch(count);
        }
        this._descriptor = CountDownLatch._new.apply(null, arguments);

        // NOTE: The following methods have to be defined on the instance itself,
        //       and not on its prototype. This is because properties on the
        //       prototype are lost during the serialization to BSON that occurs
        //       when passing data to a child thread.

        this.await = function() {
            CountDownLatch._await(this._descriptor);
        };
        this.countDown = function() {
            CountDownLatch._countDown(this._descriptor);
        };
        this.getCount = function() {
            return CountDownLatch._getCount(this._descriptor);
        };
    }, CountDownLatch);
}


/**
 * Helper function to run a workload in parallel.
 *
 * Input:
 *     numThread: number of threads.
 *     workload: a function that runs a workload.
 *
 * Output:
 *     returns an array of run times for each thread
 */
var runWorkloadInParallel = function(numThread, workload) {
    var threads = [];
    var durationsMs = [];

    var timeWorkload = function(workload) {
        var start = Date.now();
        workload();
        var end = Date.now();
        return end - start;
    };

    for (i = 0; i < numThread; i++) {
        var t = new ScopedThread(timeWorkload, workload);
        threads.push(t);
        t.start();
    }
    threads.forEach(function(t) { durationsMs.push(t.returnData()); });

    var min = Math.min.apply(null, durationsMs);
    var max = Math.max.apply(null, durationsMs);
    if ((max - min) / min > 0.05) {
        print("WARNING: run times are inconsistent: ");
        printjson(durationsMs);
    }
    return durationsMs;
};

/**
 * Much simplified version of {@link https://developer.mozilla.org/en/docs/Web/JavaScript/Reference/Global_Objects/Number/toLocaleString|toLocaleString}.
 * It only support US / EN format.
 *
 * @param {integer} number - a numeric object (it is converted with toString before being processed).
 * @param {string} [separator=,] - the separator to use between strings of digits (defaults to ',').
 *
 * @returns {string} a language sensitive representation of this number.
 * For example, 1000 will result in the string "1,000"
 */
var localize = function(number, separator) {
    if (typeof separator === "undefined" ) {
        separator=",";
    }
    return number.toString().replace(/\B(?=(\d{3})+\b)/g, separator);
};

/*
 *  Split jobs amongst a pool of threads.
 *  Note: The 'this' pointer for the function will contain a unique (per pool)
 *        threadId value. This can, for example, be used to seed the Random number
 *        generator for each thread (to get a deterministic but different random number
 *        stream for each thread).
 *
 *  Input:
 *     poolSize  : a fixed number of threads to evaluate the jobs
 *     jobs      : an array of jobs, each jobs is itself an array of
 *                  - the function to call
 *                  - the parameters to the function
 *  Return: a json object with the following fields
 *         ok: 1 iff all jobs returned ok :1
 *         duration: the amount of time in millis to run all the jobs
 *         results: an array of the return values from each of the jobs. In the
 *                  event of an exception, the document is
 *                  {ok: 0, exception: exception instance}
 *
 */
var runJobsInPool = function(poolSize, jobs) {
    var pool = [];
    var durationsMs = [];
    var chunk = Math.ceil(jobs.length / poolSize);
    var runner = function(id, jobs) {

        var start = Date.now();
        var results = jobs.map(function(job){
            try {
                var worker = job.shift();
                return worker.apply({threadId:id}, job);
            } catch(ex) {
                print(ex);
                print("Unexpected exception evaluating " + tojson(job));
                return {ok:0, exception:ex};
            }
        });
        var end = Date.now();
        var duration = end - start;
        var ok = results.every(function(r){return r.hasOwnProperty('ok') && r.ok == 1;}) ? 1: 0;
        var r =  {ok: ok,
                  duration: duration,
                  results: results};
        // printjson(r);
        return r;
    };

    while(jobs.length) {
        var t = new ScopedThread(runner, pool.length, jobs.splice(0,chunk));
        pool.push(t);
        t.start();
    }
    return pool.map(function(t) { return t.returnData(); });
};

/**
 * Convert a function call into a job. The first parameter to createJob is the function to call.
 * The remaining arguments are the parameters to the function. For example:
 *   var job = createJob(print, "hello ", "world")
 *   var job = createJob(sh.status,true)
 *
 * An arbitrary number of parameters can be passed, so the following example is also valid:
 *   var job = createJob(print, "hello"," ", "world", " ", ", this is a longer example")
 *
 * @param {function} func - the function to be called as a job in a thread.
 * @param {any} arg1 - the first argument.
 * @param {any} arg2 - the second argument.
 *
 * @returns {array} the job to be invoked.
 */
var createJob = function(func, arg1, arg2){
    return Array.prototype.slice.call(arguments);
};

var __results = [];

/*
 *  To report results in throughput
 *
 *  Input:
 *     test_name  : string
 *     throughput : float
 *     result     : JS object has following fields
 *                  - nThread : thread count
 *                  - pass    : bool
 *                  - errMsg  : error message if pass == false
 *     trial      : trial run  true|false
 */
var reportThroughput = function(test_name, throughput, result, trial) {

    // this is message for MC to parse
    var msg = ">>> " + test_name + " : " + throughput;

    if ( typeof(result) !== undefined && result !== null ) {
        if ( "nThread" in result ) {
            msg = msg + " " + result.nThread;
        } else {
            msg = msg + " NA";
        }

        if ( "pass" in result && result.pass === false ) {
            msg = msg + " failed: " + result.errMsg;
            // FIXME: need check to make sure errMsg is present.. and properly error out
        } else {
            // ignore pass case
        }
    } else {
        result = null; // just set it to null
    }

    if ( ! trial ) {
        print(msg);
    }

    // also keep the results for final pretty printed table
    __results.push({test_name: test_name, throughput: throughput, result: result});
};

/**
 * Check if isSharded is set.
 *
 * @returns true if isSharded is defined *and* true
 */
var sharded = function() {
    if ( typeof isSharded !== 'undefined' ) {
        if ( isSharded ) {
            return true;
        }
    }
    return false;
};

/**
 * Check if isRepl is set.
 *
 * @returns true if isRepl is defined *and* true
 */
var isReplSet = function() {
    if ( typeof isRepl !== 'undefined' ) {
        if ( isRepl ) {
            return true;
        }
    }
    return false;
};

// Helpers to initialize sharding

function isDatabaseSharded(d) {
    config = d.getSiblingDB("config");
    count = config.databases.find( { "_id" : d.getName(), "partitioned" : true } ).count();
    if ( count > 0 ) {
        return true;
    }
    else {
        return false;
    }
}

// Call sh.enableSharding().
// If the db is already sharded, do nothing.
function enableSharding(d) {
    if ( isDatabaseSharded(d) ) {
        return;
    }

    assert.commandWorked(d.adminCommand({
        enableSharding: d.toString()
    }));
}

function shardCollection(d, coll, opts) {
    opts = opts || {};

    opts.shardKey = opts.shardKey || "_id";
    opts.shardKeyType = opts.shardKeyType || "hashed";
    opts.chunks = opts.chunks || 2310; // = 2*3*5*7*11

    print("Sharding collection " + coll.getFullName());
    var shardConfig = {
        shardCollection: coll.getFullName(),
        key: {}, // placeholder, set from opts.shardKey below
        numInitialChunks: opts.chunks
    };
    shardConfig['key'][opts.shardKey] = opts.shardKeyType;

    var secondsWaited = 0;
    while (!d.adminCommand(shardConfig).ok) {
        print("Waiting for shardCollection to finish...");
        sleep(1000);

        secondsWaited++;
        assert( secondsWaited < 20, "shardCollection didn't succeed in 20 seconds." );
    }
    sh.status();
    // Note: If shardKeyType is something else than hashed, all chunks are now on the primary shard
    // and the balancer is probably off too. We should add a function to move them around.
    // It seems for now, all tests use hashed shard keys anyways.
}

/**
 * waitForStates blocks until all nodes in a replset reach one of the desired states or a time
 * limit is reached. This function calls
 * {@link https://docs.mongodb.com/manual/reference/command/replSetGetStatus/|replSetGetStatus}.
 *
 * Errors / Exceptions are ignored, the function will continue to wait. This case covers the
 * scenario where the process represented by _db has not yet been added to the replica set.
 *
 * This function can be used to wait for the primary to discover that all other
 * members of the replica set are in the correct state. For example,
 * [workloads/initialsync.js]{@link module:workloads/initialsync}
 * uses this method to block at the end of the test. Otherwise the final tests may fail.
 *
 * @param {object} _db - A database reference to a replica set member.
 * @param {int} [time_limit_millis=3600000] - The minimum amount of time to wait. The floor
 * value is 1.1 seconds.
 * @param {...string} [states=["PRIMARY", "SECONDARY", "ARBITER"]] - Zero or more states.
 * @returns the amount of time the function waited in millis.
 */
function waitForStates(_db, time_limit_millis, ...states) {
    var done = false;
    var start_time = Date.now();

    time_limit_millis = time_limit_millis || 60 * 60 * 1000;
    if ( time_limit_millis < 1000 ) {
        // use 1100 so that the sleep(1000) allows for more than one check
        time_limit_millis = 1100;
    }

    if ( states.length === 0 ) {
        states = ["PRIMARY", "SECONDARY", "ARBITER"];
    }

    print("waitForStates:  " + tojson(states));
    print("waitForStates: start " + tojson(rs.status()));
    var status;
    while ( !done && Date.now() - start_time < time_limit_millis ) {
        try{
            status = assert.commandWorked(_db.adminCommand({replSetGetStatus: 1}));
            done = status.members.every(function(member){
                return states.includes(member.stateStr);
            });
        }
        catch(err) {
            print("replSetGetStatus error with [" +
                  err +
                  "]. This is informational only.");
        }
        if( !done) {
            sleep(1000);
        }
    }
    assert(done, "expected to reach done state in time limit : " + tojson(status));
    return Date.now() - start_time;
}

/**
 * waitOplog wait for secondary to catch up with the primary. This function calls
 * {@link https://docs.mongodb.com/manual/reference/command/replSetGetStatus/|replSetGetStatus}.
 * If there are no members returned then this function will return without waiting. It
 * is likely that this case will cover the scenario where the process represented by
 * replSetDB has not yet been added to the replica set.
 *
 * @param {object} [replSetDB=db] - a database reference to a replica set member.
 */
function waitOplog(replSetDB) {
    if (typeof replSetDB === 'undefined') {
        replSetDB = db;
    }
    var members = replSetDB.adminCommand("replSetGetStatus").members;
    var opDB = replSetDB.getSiblingDB("Op");
    var c = opDB.wait_oplog;
    // Only do the check if repl set
    if (members) {
        // first check to make sure all secondaries are in normal operating mode
        for (var j = 0; j <  members.length; j++) {
            var i = members[j];
            if ( i.stateStr != "PRIMARY" && i.stateStr != "SECONDARY" && i.stateStr != "ARBITER" ) {
                // neither PRIMARY or SECONDARY, fail this
                print("ERROR: replSet member " + i.name + " state is: " + i.stateStr);
                return false;
            }
        }
        if (!replSetDB.isMaster().ismaster) {
            print("ERROR: Not connected to the primary");
            return false;
        }
        // print out to get lags
        replSetDB.printSlaveReplicationInfo();
        print("Before insert with w:all");
        r = c.insert({"x":"flush"}, {writeConcern : { w:members.length, j: true}});
        print("After insert with w:all. nInserted=" + r.nInserted);
        printjson(r);
        replSetDB.printSlaveReplicationInfo();
        return r.nInserted === 1;
    }
    return true;
}

/**
 * waitOplogSharded wait for each shard to catch up with the primary.
 *
 * @param {object} [targetDB=db] - a database reference to a mongos process.
*/
function waitOplogSharded(targetDB) {
    if (typeof targetDB === 'undefined') {
        targetDB = db;
    }
    var configDB = targetDB.getSisterDB("config");
    var shards = configDB.shards.find().sort({_id: 1}).toArray();
    for (var i = 0; i < shards.length; i++) {
        var shard = shards[i];

        // Connect to each shard and do a w:all write
        var mongo = new Mongo(shard.host);
        var shardDB = mongo.getDB("admin");
        print("Running waitOplog on shard: " + tojson(shard));
        if (!waitOplog(shardDB)) {
            print("Failed to run waitOplog on shard: " + tojson(shard));
            return false;
        }
    }
    return true;
}

/**
 * Helper function to quiesce the system between tests. *targetDB* can be
 * a standalone mongod, a replica set member or a mongos process. The appropriate
 * actions will be performed based on the values returned by sharded() and isReplSet().
 * @see sharded
 * @see isReplSet
 *
 * @param {object} [targetDB=db] - a database reference to a mongo process. 
 */
var quiesceSystem = function(targetDB) {
    if (typeof targetDB === 'undefined') {
        targetDB = db;
    }
    if(!waitOplogCheck(targetDB)) {
        print("ERROR: waitOplogCheck() failed in quiesceSystem(). Returning false and not running doFSync().");
        return false;
    }
    // Check for any migrations or deletes.
    doFSync(targetDB);
    return true;
};


/**
 * Helper function to flush to disk. *targetDB* can be
 * a standalone mongod, a replica set member or a mongos process. The appropriate
 * actions will be performed based on the values returned by
 * sharded() and isReplSet().
 *
 * @see sharded
 * @see isReplSet
 * @param {object} [targetDB=db] - a database reference to a mongo process.
 */
var doFSync = function(targetDB) {
    if (typeof targetDB === 'undefined') {
        targetDB = db;
    }
    if (sharded()){
        doFSyncSharded(targetDB);
    }
    else if (isReplSet()){
        doFSyncReplicaSet(targetDB);
    }
    else {
        targetDB.adminCommand({fsync: 1});
    }
};

/**
 * For a sharded cluster, iterate through all the data bearing hosts and call fsync
 * on each of them.
 *
 * @param {object} [targetDB=db] - the database reference to a mongos process.
 */
var doFSyncSharded = function(targetDB) {
    if (typeof targetDB === 'undefined') {
        targetDB = db;
    }
    var configDB = targetDB.getSisterDB("config");
    // The sort is copied from waitOplogSharded. Not sure it's
    // necessary, but it will go in the same order each time.
    var shards = configDB.shards.find().sort({_id: 1}).toArray();
    for (var i = 0; i < shards.length; i++) {
        var shard = shards[i];
        // Connect to each shard, and fsync the replica set
        var mongo = new Mongo(shard.host);
        var shardDB = mongo.getDB("admin");
        doFSyncReplicaSet(shardDB);
    }

    var map = targetDB.adminCommand({"getShardMap": 1});
    var connector = map.map.config;
    var mongo = new Mongo(connector);
    var cfgDB = mongo.getDB("admin");
    doFSyncReplicaSet(cfgDB);
};

/**
 * Perform an fsync on each member of a replica set if the replset
 * was initiated.
 * If replSetDB is not yet initialized then it just fsync's that node
 * Otherwise raise an exception.
 *
 * @param {object} [replSetDB=db] - the database reference to a replica set member.
 */
var doFSyncReplicaSet = function(replSetDB) {
    if (typeof replSetDB === 'undefined') {
        replSetDB = db;
    }
    var status = replSetDB.adminCommand("replSetGetStatus");
    if (!status.ok) {
        print("WARNING: doFSyncReplicaSet failed, replSetGetStatus[" + replSetDB.getMongo()
              +"] status=" + tojson(status));
    }
    var members = status.members;
    // Only do the check if repl set
    if (members) {
        for (var i = 0; i <  members.length; i++) {
            // Connect to member and do fsync
            var member = members[i];
            var mongo = new Mongo(member.name);
            var adminDB = mongo.getDB("admin");
            adminDB.runCommand({fsync: 1});
        }
    }
};

/**
 * wait for the oplog to catch up. On Error this function will perform a single retry. This
 * function will call waitOplogSharded() or waitOplog() based on the return values of sharded() and
 * isReplSet()
 *
 * @see waitOplogSharded
 * @see waitOplog
 * @see sharded
 * @see isReplSet
 * @param {object} [replSetDB=db] - the database reference to a mongo process. 
 */
function waitOplogCheck(targetDB) {
    if (typeof targetDB === 'undefined') {
        targetDB = db;
    }
    // Connection is closed after some long running tests (large_initialsync.js), so we wrap this
    // in a try-catch-retry block.
    try {
        return waitOplogCheck_real(targetDB);
    } catch (err) {
        print(err);
        print("waitOplogCheck() failed. Retrying one more time...");

        return waitOplogCheck_real(targetDB);
    }
}

function waitOplogCheck_real(targetDB) {
    if (typeof targetDB === 'undefined') {
        targetDB = db;
    }
    if (sharded()) {
        return waitOplogSharded(targetDB);
    } else if (isReplSet()) {
        return waitOplog(targetDB);
    } else {
        return true;
    }
}


/*
 * Get a shard and make it the primary shard
 *
 * Preconditions:
 *      This is run against a mongos in a sharded environment
 */
var findPrimaryShard = function(db_name) {
    var configdb = db.getSisterDB("config");
    var primary_name = configdb.databases.findOne({"_id": db_name}).primary;
    return configdb.shards.findOne({"_id": primary_name}).host;
};

/*
 * Convert numbers into human readable form.
 */
var humanReadableNumber = function(n){
    if( n / 1000000000 >= 1 )
        return Math.round( n / 1000000000 ) + "B";
    else if ( n / 1000000 >= 1)
        return Math.round( n / 1000000 ) + "M";
    else if ( n / 1000 >= 1)
        return Math.round( n / 1000 ) + "K";
    else
        return n;
};

// Helper function to deal with the fact that undefined in JS is NaN, not 0.
function toNumber(n) {
    return isNaN(n) ? 0 : Number(n);
}

// from <mongodb>/jstests/libs/check_log.js
/*
 * Helper functions which connect to a server, and check its logs for particular strings.
 */
var checkLog;

(function() {
    "use strict";

    if (checkLog) {
        return;  // Protect against this file being double-loaded.
    }

    checkLog = (function() {
        var getGlobalLog = function(conn) {
            var cmdRes;
            try {
                cmdRes = conn.adminCommand({getLog: 'global'});
            } catch (e) {
                // Retry with network errors.
                print("checkLog ignoring failure: " + e);
                return null;
            }

            return assert.commandWorked(cmdRes).log;
        };

        /*
         * Calls the 'getLog' function at regular intervals on the provided connection 'conn' until
         * the provided 'msg' is found in the logs, or 5 minutes have elapsed. Throws an exception
         * on timeout.
         */
        var contains = function(conn, msg) {
            assert.soon(
                function() {
                    var logMessages = getGlobalLog(conn);
                    if (logMessages === null) {
                        return false;
                    }
                    for (var i = 0; i < logMessages.length; i++) {
                        if (logMessages[i].indexOf(msg) != -1) {
                            return true;
                        }
                    }
                    return false;
                },
                'Could not find log entries containing the following message: ' + msg,
                5 * 60 * 1000,
                300);
        };

        /*
         * Calls the 'getLog' function at regular intervals on the provided connection 'conn' until
         * the provided 'msg' is found in the logs exactly 'expectedCount' times, or 5 minutes have
         * elapsed.
         * Throws an exception on timeout.
         */
        var containsWithCount = function(conn, msg, expectedCount) {
            var count = 0;
            assert.soon(
                function() {
                    var logMessages = getGlobalLog(conn);
                    if (logMessages === null) {
                        return false;
                    }
                    for (var i = 0; i < logMessages.length; i++) {
                        if (logMessages[i].indexOf(msg) != -1) {
                            count++;
                        }
                    }

                    return expectedCount === count;
                },
                'Expected ' + expectedCount + ', but instead saw ' + count +
                    ' log entries containing the following message: ' + msg,
                5 * 60 * 1000,
                300);
        };

        return {
            getGlobalLog: getGlobalLog,
            contains: contains,
            containsWithCount: containsWithCount
        };
    })();
})();


// from <mongodb>/jstests/libs/write_concern_utils.js
// Stops replication on the given server(s).
function stopServerReplication(conn) {
    if (conn.length) {
        conn.forEach(function(n) {
            stopServerReplication(n);
        });
        return;
    }

    print("stopServerReplication called for " + conn);
    // Clear ramlog so checkLog can't find log messages from previous times this fail point was
    // enabled.
    assert.commandWorked(conn.adminCommand({clearLog: 'global'}));
    var errMsg = 'Failed to enable stopReplProducer failpoint.';
    assert.commandWorked(
        conn.adminCommand({configureFailPoint: 'stopReplProducer', mode: 'alwaysOn'}), errMsg);

    print("stopServerReplication wait for " + conn);
    // Wait until the fail point is actually hit.
    checkLog.contains(conn, 'bgsync - stopReplProducer fail point enabled');
}

/**
 * Runs a command in the bash shell.
 */
var runCmd = function(cmd) {
    runProgram('bash', '-c', cmd);
};

/**
 * Runs a list of semicolon separated bash commands on the specified host.
 */
var runCmdOnTarget = function(host, cmds) {
    hostSsh = 'ssh -A -o StrictHostKeyChecking=no ' + host;
    runCmd(hostSsh + ' "' + cmds + '"');
};

/**
 * Returns a string of format <host_addr>:<port>
 */
var hostWithPort = function(host_addr, port) {
    return host_addr + ':' + port;
};

/**
 * Attempt to establish a new Mongo connection to mongod instance running on the specified host,
 * with multiple retries.
 *
 * Returns a reference to admin Database, upon successful connnection. If connection cannot be
 * established after 2 retries, throws an exception.
 *
 */
var getAdminDB = function(host, retries) {
    var retries = retries || 2;
    var admin_db;
    assert.retry(function() {
        try {
            var conn = new Mongo(host);
            admin_db = conn.getDB("admin");
            return true;
         } catch (e) {
            print(e);
            return false;
        }
    }, "Error Connecting to admin database on host " + retries, retries);
    return admin_db;
};

var keepAliveFn = function(intervalMillis, keepAliveEndCounter, printMsgFnCbk, ...cbkArgs) {
    while (keepAliveEndCounter.getCount() > 0) {
        sleep(intervalMillis);
        printMsgFnCbk(...cbkArgs);
    }
};

/**
 * Starts a keep-alive thread which runs "printMsgFnCbk" callback function with the provided cbkArgs,
 * for every specified intervalMillis.
 */
var keepAliveStartifNeeded = function(keepAliveNeeded, printMsgFnCbk, intervalMillis, ...cbkArgs) {
    var keepAliveEndCounter = null;
    var keepAliveThread = null;

    if (!(keepAliveNeeded && typeof printMsgFnCbk === "function")) {
        return [keepAliveEndCounter,keepAliveThread];
    }

    var intervalMillis =  intervalMillis || 30 * 60 * 1000;
    keepAliveEndCounter = new CountDownLatch(1);
    keepAliveThread =
        new ScopedThread(keepAliveFn, intervalMillis, keepAliveEndCounter, printMsgFnCbk, ...cbkArgs);
    keepAliveThread.start();
    return [keepAliveThread, keepAliveEndCounter];
};

/**
 * Stops the keep-alive thread.
 */
var keepAliveStop = function(keepAliveThread, keepAliveEndCounter) {
    if (keepAliveThread) {
        keepAliveEndCounter.countDown();
        keepAliveThread.join();
    }
};
