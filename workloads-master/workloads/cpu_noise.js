/**
 * @file
 * cpu tests. These tests are intended to be predicatable in terms of test performance. They are
 * intended as a measures of the test infrastructure.
 *
 * ### *Test*
 *
 * The classes of tests are:
 *   * {@link https://github.com/mongodb/mongo/blob/9687c56dad047fb222076c0eb5fb25db6a796219/src/mongo/db/commands/test_commands.cpp#L125-L133|sleep}:
 *     this command runs a no-op command on the server for 10ms time (with no lock).
 *   * {@link https://github.com/mongodb/mongo/blob/9687c56dad047fb222076c0eb5fb25db6a796219/src/mongo/db/commands/cpuload.cpp#L51-L56|cpuload command}:
 *     this server side command runs a straight CPU load. The length of execution is scaled by
 *     cpuFactor. This command puts no additional load on the server beyond the cpu use.
 *   * {@link https://github.com/mongodb/mongo/blob/7ab97aaaf42b91736afd61ed7bfe684d393c89c2/src/mongo/shell/bench.cpp#L720-L733|cpuload operation}:
 *     this client side benchRun operation runs a straight CPU load on workload_client. The length
 *     of execution is scaled by cpuFactor. There is no connection to the server at all.
 *   * {@link https://github.com/mongodb/mongo/blob/9687c56dad047fb222076c0eb5fb25db6a796219/src/mongo/db/commands/generic.cpp#L111-L114|ping operation}:
 *     this command sends a ping to the server. Same command is used by nodes to monitor each other.
 *
 * Results are reported as ops / sec.
 *
 * ### *Metrics reported*
 *
 * **canary_server-cpuloop-10x**: Connect to the server, which will multiply 100 x 13 x 13 x ... hundred thousand times in a loop, then return.
 *
 * **canary_client-cpuloop-1x**: Multiply 100 x 13 x 13 x ... ten thousand times in a loop. Runs in the mongo shell, no connection to server is created.
 *
 * **canary_client-cpuloop-10x**: Multiply 100 x 13 x 13 x ... hundred thousand times in a loop. Runs in the mongo shell, no connection to server is created.
 *
 * **canary_server-sleep-10ms**: Connect to the server and issue a command to no-op for 10 milliseconds.
 *
 * **canary_ping**: Send ping command to server. Server returns immediately.
 *
 * ### *Setup*
 *
 *   Supports standalone, replica and shard
 *
 * ### *Owning-team*
 * mongodb/product-perf
 *
 * ### *Keywords*
 *
 * @module workloads/cpu_noise
 */

/*global
  db sharded Random enableSharding shardCollection quiesceSystem
  benchRun benchStart benchFinish sh print printjson assert
  reportThroughput sleep server jsTest version
*/

/**
 * The number of threads to run in parallel. The default is [1,4,8,16 ].
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var thread_levels = thread_levels || [1,4,8,16 ];

function run_simple_tests(server, name, ops_list, nThreads) {
    var prefix = resultPrefix || "";
    res = benchRun( {
        ops: ops_list,
        seconds: 10,
        host: server,
        parallel: nThreads,
        username: username,
        password: password
    });
    reportThroughput(prefix + name, res["totalOps/s"], {nThread: nThreads});
    return res;
}

//{ op: "sleepMicros", micros : NumberLong(1000) }

(function() {
    "use strict";
    // Check that operations exist. Only run against 3.5 and later servers
    var clientVersion = version().split(".");

    if ( clientVersion[0] >= 4 || ( clientVersion[0] == 3 && clientVersion[1] > 4 )) {
        for (var x in thread_levels) {
            var n = thread_levels[x];
            run_simple_tests(server,
                             "cpuloop-1x",
                             [{ns: "test.cpu",
                               op: "cpuload",
                               cpuFactor: 1}],
                             n);
            run_simple_tests(server,
                             "cpuloop-10x",
                             [{ns: "test.cpu",
                               op: "cpuload",
                               cpuFactor: 10}],
                             n);
        }
    }
    else {
        print("WARNING: Not running cpu_noise client cpu tests because client version is" +
              " less than 3.5.");
        print("client version is " + version());
    }

    for (var x in thread_levels) {
        var n = thread_levels[x];
        // run_simple_tests(server, "nop", [{ns: "test.cpu", op: "nop"}], n);
        run_simple_tests(server,
                         "sleep-1ms",
                         [{ns: "test.cpu",
                           op: "nop",
                           delay: 1}],
                         n);
        run_simple_tests(server,
                         "sleep-10ms",
                         [{ns: "test.cpu",
                           op: "nop",
                           delay: 10}],
                         n);
    }
}());
