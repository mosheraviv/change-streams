var getLogMessages = function(node_addr) {
    var conn = new Mongo(node_addr);
    var _db = conn.getDB("admin");
    return _db.adminCommand({getLog: "global"}).log;
};

// This function reports the duration of each phase of a test. The phases are specified by the log
// messages in the phaseLogs object provided.
// phaseLogs should be an object where the keys are phase names and the values are objects
// with a 'start' field and an 'end' field that map to strings contained in the start and ending
// log message of a phase.
// Ex: {<phase 1>: {start: <startLog>, end: <endLog>}, <phase 2>: ...}
var parseLogMessages = function(logs, phaseLogs, test_name) {
    // Must be at least one phase pair for this to do anything.
    if (Object.keys(phaseLogs).length < 1) {
        return;
    }

    var startPhaseLogs = transposePhaseLogs(phaseLogs, "start");
    var endPhaseLogs = transposePhaseLogs(phaseLogs, "end");

    var phaseData = {};
    getPhaseData(startPhaseLogs, endPhaseLogs, logs, phaseData);
    reportDurations(phaseData, test_name);
};

// This function calculates the durations of each phase and then prints and reports them.
var reportDurations = function(phaseData, test_name) {
    calculatePhaseDurations(phaseData);
    print("Phase Data: " + tojson(phaseData));

    var phaseDataSums = getPhaseDataSums(phaseData);
    print("Phase Data Sums: " + tojson(phaseDataSums));

    // "No NS" refers to phases that occur on the entire procedure and not ones tied to a specific
    // namespace.
    for (var phase in phaseData["No NS"]) {
        if (phaseData["No NS"][phase].duration) {
            reportThroughput(
                phase + "_" + test_name, -1 * phaseData["No NS"][phase].duration, {nThread: 1});
        }
    }
    for (var phase in phaseDataSums) {
        if (phaseDataSums[phase].duration) {
            reportThroughput(phase + "_" + test_name, -1 * phaseDataSums[phase].duration, {nThread: 1});
        }
    }
};

// This function accepts phaseLogs in the same form as the input to parseLogMessages and returns an
// object where the keys are the start or end log messages depending on the value of start_or_end
// and the values are the phase names for that log.
// Ex: {<startLog 1>: <phase 1>, <startLog 2>: <phase 2>, ...}
var transposePhaseLogs = function(phaseLogs, start_or_end) {
    var transposedLogs = {};
    for (var phase in phaseLogs) {
        // The beginning or ending of a phase can be marked by one of multiple logs depending on
        // the code path taken. For example, initial sync prints a different log message depending
        // on whether or not there are oplog operations to apply, but both mark the beginning of the
        // oplog application phase. Phases like these will have an array of logs for their start or
        // end instead of just one log. If a phase has an array of logs, add all of them to the
        // list, otherwise just add the single {log: phase} pair.
        var currLogs = phaseLogs[phase][start_or_end];
        if (!Array.isArray(currLogs)) {
            currLogs = [currLogs];
        }
        for (var log of currLogs) {
            transposedLogs[log] = phase;
        }
    }
    return transposedLogs;
};

// In addition to tracking data about the timing of the entire procedure, we also want to track
// how long phases take on individual namespaces. For example, rather than just measuring
// the entire collection-cloning phase of initialsync, we will also keep track of the time to
// clone each individual collection. We check every single phase-marking log message for a
// relevant namespace.
// This function creates a mapping from namespace to an object containing a mapping from phases to
// the data about when they start, stop, and any additional data contained in the logs.
// Ex: {<ns>: {<phase>: {start: <ts>, end: <ts>, ...}, ... }, ...}
var getPhaseData = function(startPhaseLogs, endPhaseLogs, logs, phaseData) {
    for (var log of logs) {
        var startLog = isLogACheckpoint(log, startPhaseLogs);
        if (startLog) {
            var phase = startPhaseLogs[startLog];
            var ns = getLogNamespace(log);
            var ts = getLogTimestamp(log);
            phaseData[ns] = phaseData[ns] || {};
            phaseData[ns][phase] = phaseData[ns][phase] || {};
            phaseData[ns][phase].start = ts;
        }
        var endLog = isLogACheckpoint(log, endPhaseLogs);
        if (endLog) {
            var phase = endPhaseLogs[endLog];
            var ns = getLogNamespace(log);
            var ts = getLogTimestamp(log);
            phaseData[ns] = phaseData[ns] || {};
            phaseData[ns][phase] = phaseData[ns][phase] || {};
            phaseData[ns][phase].end = ts;
        }
    }
};

// This function iterates through each namespace and each phase and calculates the duration of the
// phase.
var calculatePhaseDurations = function(phaseData) {
    for (var ns in phaseData) {
        for (var phase in phaseData[ns]) {
            var nsPhase = phaseData[ns][phase];
            if (!nsPhase.start) {
                print("Phase " + phase + " on ns " + ns + " did not have a start time: " +
                      tojson(nsPhase));
                continue;
            }
            if (!nsPhase.end) {
                print("Phase " + phase + " on ns " + ns + " did not have a end time: " +
                      tojson(nsPhase));
                continue;
            }
            nsPhase.duration = nsPhase.end - nsPhase.start;
            assert.gte(nsPhase.duration,
                       0,
                       "Phase " + phase + " on ns " + ns + " had a negative duration: " +
                           nsPhase.duration);
        }
    }
};

// This function takes in the phase durations split up by namespace and sums together the
// durations of each namespace of each phase. It returns a map from phases to their total duration.
// This excludes the "No NS" namespace.
var getPhaseDataSums = function(phaseData) {
    var phaseDataSums = {};
    for (var ns in phaseData) {
        if (ns === "No NS") {
            continue;
        }
        for (var phase in phaseData[ns]) {
            phaseDataSums[phase] = phaseDataSums[phase] || {duration: 0};
            phaseDataSums[phase].duration += phaseData[ns][phase].duration;
        }
    }
    return phaseDataSums;
};

// This function checks if a log contains one of the phase checkpoint messages.
var isLogACheckpoint = function(log, checkpoints) {
    for (var cpLog in checkpoints) {
        if (log.indexOf(cpLog) > -1) {
            return cpLog;
        }
    }
    return undefined;
};

// This function extracts the timestamp from a log message and parses it into a date. It assumes
// the timestamp is the first 28 characters of the log message.
var getLogTimestamp = function(log) {
    return Date.parse(log.substring(0, 28));
};

// This function extracts the namespace from a log message. This assumes the namespace is the last
// string in the log, following the string "ns: ". It returns "No NS" if there is no namespace.
var getLogNamespace = function(log) {
    var ns = "";
    var parts = log.split(" ns: ");
    if (parts.length > 1) {
        ns = parts[1];
    } else {
        parts = log.split("database: ");
        if (parts.length > 1) {
            ns = parts[1];
        } else {
            return "No NS";
        }
    }
    // If there's a second part, ignore it.
    var commaSpot = ns.indexOf(",");
    if (commaSpot > 0) {
        ns = ns.slice(0, commaSpot);
    }
    return ns;
};
