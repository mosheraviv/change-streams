/**
 * @file
 * This workload tests the handling of concurrent updates to a single document.
 * See {@link https://jira.mongodb.org/browse/SERVER-17195|SERVER-17195}.
 *
 * ### *Test*
 *
 *   Increment a single numeric field within a single document for 5 minutes. This test is
 *   designed to ensure that this work flow does not lock access to the collection.
 *
 *   Results are reported as ops / sec.
 *
 * ### *Setup*
 *
 *   Support standalone, replica
 *   Will not run for shard
 *
 * ### *Notes*
 *
 *   Related ticket: {@link https://jira.mongodb.org/browse/SERVER-17195|SERVER-17195}
 *
 * ### *Owning-team*
 * mongodb/product-query
 *
 * ### *Keywords*
 *
 * @module workloads/contended_update
 */
/*global
  db sharded Random enableSharding shardCollection quiesceSystem
  benchRun benchStart benchFinish sh print printjson assert
  reportThroughput sleep server jsTest version
*/

/**
* The test thread levels, injected from run_workloads.py, which gets it from test_control.yml
* config file.
* It defaults to [32, 64].
*
* The value can be changed as a parameter, see
* {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
*/
var thread_levels = thread_levels || [32, 64];

var db_name="contended_update";

/**
 * The number of threads to run in parallel. The default is [1, 32, 64 ].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var thread_levels=thread_levels || [1, 32, 64];

var run_contended_update = function(d, nThread, server_addr) {
    var coll = d.foo;
    coll.drop();

    if( sharded() ) {
        shardCollection(d, coll);
    }

    coll.insert({_id:1,a:0});

    quiesceSystem();
    res = benchRun( {
        ops : [{
            ns : coll.getFullName(),
            op : "update" ,
            query: {_id: 1},
            update : { $inc: {a: 1} } ,
            writeCmd : true }],
        seconds : 300,
        host : server_addr,
        parallel : nThread,
        username: username,
        password: password
    });

    return res;
};

var run_tests = function(server_addr) {
    var d = db.getSiblingDB(db_name);
    // Run once
    if( sharded() ) {
        enableSharding(d);
    }

    thread_levels.forEach(function(threads){
        var res = run_contended_update(d, threads, server_addr);
        reportThroughput("contended_update", res["totalOps/s"], {nThread: threads});
    });
};

run_tests(server);
