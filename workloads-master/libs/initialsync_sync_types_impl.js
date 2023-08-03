/**
 * Returns the temporary data path used during rsync.
 */
var getDbPathBkp = function(dbPath) {
    return dbPath + "-bkp";
};

/**
 * Copies data directory from syncSource to syncTarget via rsync.
 *
 * @param {String} syncSource - Host from which date has to copied.
 * @param {String} syncTarget - Host that has to be synced.
 * @param {Number} port - Port on which mongod instance runs.
 * @param {String} dbPath - Mongod data directory path.
 *
 * @return {Date} Post rsync start time.
 */
var doRsync = function(syncSource, syncTarget, port, dbPath, startMongod) {
    var sourceDbPath = syncSource + ":" + dbPath + "/";
    var destDbPathBkp =  getDbPathBkp(dbPath);

    // Rsync does not support copying files between two remote hosts. So, we first ssh to syncTarget node and
    // then execute rsync to copy datafiles from the syncSource.
    // For more info regarding rsync parameters, please refer rsync man page.
    var rsyncXferCmd = "rsync -aKkv --del -e 'ssh -A -o StrictHostKeyChecking=no' " + sourceDbPath + " " + destDbPathBkp;
    jsTestLog('Copying dir ' + dbPath + " from sync source ("  + syncSource + ") to sync target (" + syncTarget + ")");

    var startTimeRsync;
    var i;

    // Rsync 3 times.
    for (i = 0; i < 3; i++) {
        startTimeRsync = Date.now();
        runCmdOnTarget(syncTarget, rsyncXferCmd);
        jsTestLog("rsync " + i + " took " + (Date.now() - startTimeRsync));
    }

    // Fsync lock the primary.
    assert.commandWorked(db.fsyncLock());

    // Final rsync.
    startTimeRsync = Date.now();
    runCmdOnTarget(syncTarget, rsyncXferCmd);
    jsTestLog("rsync " + i + " took " + (Date.now() - startTimeRsync));

    // Fsync unlock the primary.
    assert.commandWorked(db.fsyncUnlock());

    var startTimePostRsync = Date.now();
    var cmdList = [];

    print("Running post rsync commands on sync target " + syncTarget);
    // set remote environment variables especially to locate mongodb binaries.
    var remoteEnv = "source ~/.bash_profile";
    cmdList.push(remoteEnv);

    // Stop the mongod process.
    var stopMongod = "/data/workdir/mongodb/bin/mongo --port " + port + " admin --eval 'db.shutdownServer()'";
    cmdList.push(stopMongod);

    // Rename dbPath to dbPath+'-temp'.
    var dbPathTemp = dbPath + "-tmp";
    var renameCurrToTemp = "mv " + dbPath + " " + dbPathTemp;
    cmdList.push(renameCurrToTemp);

    // Rename dbPath+'-bkp' to dbPath.
    var renameBkpToCurr = "mv " + destDbPathBkp + " " + dbPath;
    cmdList.push(renameBkpToCurr);

    // Remove diagnostic.data from dbPath as those are sync source metric data.
    var metricsDir = "/diagnostic.data";
    var destMetricDir = dbPath + metricsDir;
    var rmMetricsFromCurr = "rm  -rf " + destMetricDir;
    cmdList.push(rmMetricsFromCurr);

    // Move the diagnostic.data dir from dbPath+'-temp' to dbPath, so that we retain
    // the metrics that were collected during rsync on sync target.
    var srcMetricDir = dbPathTemp + metricsDir;
    var mvMetricsFromTempToCurr = "mv " + srcMetricDir + " "+ destMetricDir;
    cmdList.push(mvMetricsFromTempToCurr);

    // To avoid unclean shutdown error, remove 'mongod.lock' file from dbPath.
    var rmLockFile = "rm  -rf " + dbPath + "/mongod.lock";
    cmdList.push(rmLockFile);

    // Restart the secondary mongod process.
    startMongod = startMongod || "/data/workdir/mongodb/bin/mongod  --config /tmp/mongo_port_27017.conf";
    cmdList.push(startMongod);

    // Execute all the commands on sync target in one shot to avoid multiple network round-trips.
    var postRsyncCmds = cmdList.join(";");
    runCmdOnTarget(syncTarget, postRsyncCmds);
    return startTimePostRsync;
}
