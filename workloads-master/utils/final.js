// pretty print results

var string2len = function(ts, len, left) {
    var s = JSON.stringify(ts);

    if ( s.length > len) {
        return s.slice(0, len);
    }

    var l = s.length;
    for(var i = 0; i < (len - l); i++) {
        if ( left !== null && left === true) {
            s = s + " ";
        } else {
            s = " " + s;
        }
    }

    return s;
};

if(__results.length > 0){
    print("\n\nResults:");

    print("+--------------------------------+----------+--------------+----------+------------------------------+");
    print(   "| " + string2len("Test", 30, true) +
            " | " + string2len("Thread", 8, true) +
            " | " + string2len("Throughput", 12, true) +
            " | " + string2len("Pass?", 8, true) +
            " | " + string2len("Comment", 28, true) +
            " | "
            );
    print("|--------------------------------+----------+--------------+----------+------------------------------|");

    for(var i = 0; i < __results.length; i = i + 1) {
        if ( ! ("pass" in __results[i].result)) {
            __results[i].result.pass = true;
            __results[i].result.errMsg = "";
        } else if ( ! ("errMsg" in __results[i].result)) {
            __results[i].result.errMsg = "";
        }

        print("| " + string2len(__results[i].test_name, 30, true) +
              " | " + string2len(__results[i].result.nThread, 8 ) +
              " | " + string2len(__results[i].throughput, 12 ) +
              " | " + string2len(__results[i].result.pass, 8, true) +
              " | " + string2len(__results[i].result.errMsg, 28, true) +
              " | ");

    }

    print("+--------------------------------+----------+--------------+----------+------------------------------+");
}

// After each test we must wait for the oplog.
// waitOplogCheck already prints an error message.
assert(waitOplogCheck(), "failed to wait for the oplog");
