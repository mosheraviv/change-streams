/**
 * Change Streams library.
 */

/*global
 assert CountDownLatch Date db Error Mongo print ScopedThread sharded sleep tojson
*/


/**
 * Creates a ChangeStreamListenerThread.
 *
 * @constructor
 * @param {String} dbName - The database name.
 * @param {String} collName - The collection name.
 * @param {Array} filterPipeline - The aggregation pipeline that should follow the '$changeStream'
 *        aggregation stage.
 * @param {Object} changeStreamOptions - The options that should be passed to the '$changeStream'
 *        aggregation stage. Defaults to no options.
 * @param {Function} changeHandler - A callback function that takes a change document, a state
 *        object and an options object as parameters. The function will be called for each change
 *        document read. Defaults to a noop function.
 * @param {Object} changeHandlerOptions - An object that will be passed as argument to the
 *        changeHandler callback function. Defaults to an empty object.
 * @param {String} targetHost - The host to connect to in order to listen for the change stream.
 *        Defaults to the the default server (using the global db).
 * @param {String} readPreferenceMode - The read preference of the change stream aggregation pipeline.
 */
var ChangeStreamListenerThread = function(dbName, collName, filterPipeline, changeStreamOptions,
                                          changeHandler, changeHandlerOptions, targetHost, readPreferenceMode) {
    var _continueCounter = null;
    var _notReadyCounter = null;
    var _thread = null;
    var _filterPipeline = filterPipeline || [];
    var _changeStreamOptions = changeStreamOptions || {};
    var _changeHandler = changeHandler || function (doc) {};
    var _changeHandlerOptions = changeHandlerOptions || {};

    /**
     * Create a change stream cursor and continually read from it.
     * This function is executed in a ScopedThread.
     *
     * The 'continueCounter' parameter is a CountDownLatch used by the main thread to signal this
     * function, which is executed in a ScopedThread, if it can continue or should stop. Its count
     * must be set to 1 to indicate that the function can continue, and to 0 to indicate that it
     * should stop.
     * The 'notReadyCounter' parameter is a CountDownLatch used by this function to signal the main
     * thread that it is ready and reading change streams. It must be passed to this function with
     * a count of 1, and the function will set it to 0 when ready.
     */
    function _readChangeStream(continueCounter, notReadyCounter, dbName, collName, filterPipeline,
                               changeStreamOptions, changeHandler, changeHandlerOptions,
                               targetHost, username, password, authEnabled, readPreferenceMode) {
        try {
            assert.eq(continueCounter.getCount(), 1,
                      "The continueCounter must have an initial value of 1.");
            assert.eq(notReadyCounter.getCount(), 1,
                      "The notReadyCounter must have an initial value of 1.");
            var state = {
                "value": 0,
                "clusterTime": null
            };
            var pipeline = [{"$changeStream": changeStreamOptions}].concat(filterPipeline);

            // Initialize the db object.
            var _db;
            if (targetHost) {
                // Using a connection to the provided 'targetHost'.
                // This is used when we need to connect to a specific mongos.
                if (authEnabled){
                    var targetHostAuth = 'mongodb://'.concat(username, ':', password, '@', targetHost);
                } else {
                    var targetHostAuth = targetHost;
                }
                var conn = new Mongo(targetHostAuth);
                _db = conn.getDB(dbName);
            } else {
                // Using the default connection configured by run_workloads.py.
                _db = db.getSiblingDB(dbName);
            }

            // Function to execute a command and update the state.clusterTime variable with
            // the cluster time found in the response.
            var runCommand = function(cmdObj) {
                var res = assert.commandWorked(_db.runCommand(cmdObj));
                state.clusterTime = res.$clusterTime.clusterTime.getTime();
                return res;
            };

            // Run the aggregate command.
            var res = runCommand({aggregate: collName,
                                  pipeline: pipeline,
                                  cursor: {},
                                  $readPreference: {mode: readPreferenceMode}});
            var cursorId = res.cursor.id;
            var batch = res.cursor.firstBatch;

            // Set up the function that will be called for each document.
            var callback = function(doc) {
                changeHandler(doc, state, changeHandlerOptions);
            };

            // The aggregation command returned sucessfully. This thread is ready.
            notReadyCounter.countDown();
            // While the thread is not stopped, we go through each batch of result, call the
            // callback function for each document, and fetch the next batch with a 'getMore'
            // command.
            // Note that we don't wait for all the changes to be read as for some configurations
            // it can take several minutes. This can be revised when the performances improve.
            while (continueCounter.getCount() > 0) {
                batch.forEach(callback);
                res = runCommand({getMore: cursorId, collection: collName});
                batch = res.cursor.nextBatch;
            }
            return state.value;
        } catch (e) {
            print("ChangeStreamListenerThread interrupted by error");
            // When the error is raised by assert.commandWorked then it is already logged so we
            // don't print all the details here.
            if (!("codeName" in e)) {
                print(tojson(e));
            }
            return {"error": true, "message": tojson(e)};
        }
    }

    /**
     * Returns true if the listener thread has been created and started.
     */
    this.hasStarted = function() {
        // Double ! to return true if '_thread' is truthy or false if it is falsy.
        return !!_thread;
    };

    this.isReady = function() {
        return _notReadyCounter.getCount() === 0;
    };

    /**
     * Spawns a ScopedThread that will run _readChangeStream().
     */
    this.start = function() {
        if (_thread) {
            throw new Error("Listener thread is already active.");
        }

        _continueCounter = CountDownLatch(1);
        _notReadyCounter = CountDownLatch(1);
        _thread = new ScopedThread(_readChangeStream, _continueCounter, _notReadyCounter,
                                   dbName, collName, _filterPipeline,
                                   _changeStreamOptions, _changeHandler, _changeHandlerOptions,
                                   targetHost, username, password, authEnabled, readPreferenceMode);
        _thread.start();
    };

    /** Stops the thread. */
    this.stop = function() {
        if (!_thread) {
            throw new Error("Listener thread is not active.");
        }

        _continueCounter.countDown();
        _continueCounter = null;
    };

    /** Joins the thread. */
    this.join = function() {
        if (!_thread) {
            throw new Error("Listener thread is not active.");
        }
        if (_continueCounter) {
            throw new Error("Listener thread has not been stopped");
        }
        _thread.join();
        this.returnData = _thread.returnData();
        _thread = null;
    };
};


/**
 * Starts change stream listeners.
 *
 * @param {Number} nbListeners - The number of change stream listeners to start.
 * @param {String} dbName - The database name.
 * @param {String} collName - The collection name.
 * @param {Array} filterPipeline - The aggregation pipeline stages to append to the '$changeStream'
 *        stage.
 * @param {Object} changeStreamOptions - The options to pass to the '$changeStream' stage.
 * @param {Function} changeHandler - A callback function that takes a change document and a state
 *        object as parameters. The function will be called for each change document read.
 * @param {Object} changeHandlerOptions - An object that will be passed as argument to the
 *        changeHandler callback function. Defaults to an empty object.
 * @param {Array} mongoHosts - An array of mongos hosts to connect to. The listeners are assigned to
 *        each host sequentially (round robin). Optional.
 * @param {String} readPreferenceMode - The read preference of the change stream aggregation pipeline.
 * @return {Array} The created listeners.
 */
function startListeners(nbListeners, dbName, collName, filterPipeline,
                        changeStreamOptions, changeHandler, changeHandlerOptions, mongosHosts, targetHost, readPreferenceMode)
{
    if (mongosHosts && mongosHosts.length !== 0) {
        print("Starting " + nbListeners + " threads split on targets: " + tojson(mongosHosts));
    } else {
        print("Starting " + nbListeners + " threads");
    }
    var listeners = [];
    for (var i = 0; i < nbListeners; i++) {
        if (mongosHosts) {
            targetHost = mongosHosts[i % mongosHosts.length];
        }
        listener = new ChangeStreamListenerThread(dbName, collName, filterPipeline,
                                                  changeStreamOptions, changeHandler,
                                                  changeHandlerOptions, targetHost, readPreferenceMode);
        listener.start();
        listeners.push(listener);
    }
    return listeners;
}


/**
 * Waits for the listeners to be ready.
 *
 * @param {Array} listeners - The listeners as an array of ChangeStreamListenerThreads.
 */
function waitForListeners(listeners) {
    var start = new Date().getTime();
    // Timeout is 5s + 100ms * nb of listeners.
    // It is an arbitrary value aimed at giving enough time for all the listeners to start so
    // that when we hit a timeout it is clear something went wrong.
    var end_timeout = start + 5000 + listeners.length * 100;
    do {
        var allReady = true;
        for (var i = 0; i < listeners.length; i++) {
            if (!listeners[i].isReady()) {
                allReady = false;
                sleep(50);
                break;
            }
        }
        if (allReady) {
            return;
        }
    } while (new Date().getTime() < end_timeout);
    throw new Error("Timeout while waiting to for change stream listeners to be ready.");
}


/**
 * Starts and waits for the change stream listeners to be ready.
 *
 * @param {String} dbName - The database name.
 * @param {Array} collNames - An array of collection names on which to listen for change streams.
 * @param {Number} nListeners - The number of change streams listeners to start. Should be a
 *        multiple of the number of collections.
 * @param {Object} changeStreamOptions - Options for the '$changeStream' stage.
 * @param {Boolean} selectiveChange - Whether additional aggregation stages should be added
 *        after the '$changeStream' stage.
 * @return {Object} A dictionary mapping collection names to arrays of corresponding listeners.
 */
function startAndWaitForListeners(dbName, collNames, nListeners, changeStreamOptions, selectiveChange,
                                  changeHandler, changeHandlerOptions, mongosHosts, targetHost, readPreferenceMode) {
    print("Starting the change stream listeners");
    var listeners = {};
    var collName = null;
    var filterPipeline = getFilterPipeline(selectiveChange);
    var nCollListeners = nListeners / collNames.length;

    for (var i = 0; i < collNames.length; i++) {
        collName = collNames[i];
        listeners[collName] = startListeners(nCollListeners, dbName, collName, filterPipeline,
                                             changeStreamOptions, changeHandler, changeHandlerOptions,
                                             mongosHosts, targetHost, readPreferenceMode);
    }
    print("The listeners are started");

    print("Waiting for the listeners to be ready");
    for (collName in listeners) {
        waitForListeners(listeners[collName]);
    }
    print("All the listeners are ready");
    return listeners;
}

/**
 * Stops the change stream listeners.
 *
 * @param {Array} listeners - An array of ChangeStreamListenerThread objects.
 * @return {Array} An array containing the final state values for all the listeners.
 */
function stopListeners(listeners) {
    var states = [];
    var listener;
    for (var i = 0; i < listeners.length; i++) {
        listeners[i].stop();
    }
    for (var j = 0; j < listeners.length; j++) {
        listener = listeners[j];
        listener.join();
        if (listener.returnData.error) {
            throw new Error("A change stream listener failed with error: '" +
                            listener.returnData.message + "'");
        }
        states.push(listener.returnData);
    }
    return states;
}


/**********************************************************************
 * Helper functions to handle the change streams tests configuration. *
 **********************************************************************/

/**
 * Transforms a mongos host list as found infrastructure.out.yml into
 * a list of host strings that can be used to create a new Mongo connection.
 */
function getMongosHosts(mongosHostsConfig) {
    var mongos_targets = [];
    for (var i = 0; i < mongosHostsConfig.length; i++) {
        mongos_targets.push(mongosHostsConfig[i].private_ip);
    }
    return mongos_targets;
}


/**
 * Generates and returns a list of collection names.
 *
 * @param {String} baseName - The collections base name.
 * @param {Number} nCollections - The number of collection names to generate.
 * @return {Array} An array of collection names.
 */
function generateCollectionNames(baseName, nCollections) {
    var names = [];
    for (var i = 0; i < nCollections; i++) {
        names.push(baseName + "_" + i);
    }
    return names;
}


/**
 * Gets the filter pipeline that should follow the '$changeStream' aggregation stage.
 *
 * @param {bool} selectiveChange - The value of the 'selective_change' option for the test.
 * @return {Array} The filter pipeline to add to the change stream pipeline.
 */
function getFilterPipeline(selectiveChange) {
    if (selectiveChange) {
        return [{"$match": {"a": {"$lt": 100000}}}];
    } else {
        return [];
    }
}

/**
 * Indicates if the servers use a resume token with the BinData format.
 * This format is used before 3.7.4 and also when featureCompatibilityVersion is 3.6
 * (change streams are only supported since 3.6).
 */
function useBinDataResumeToken() {
    var version = db.version().split(".");
    if (version < [3, 7, 4]) {
        return true;
    }
    if (!sharded()) {
        // Mongos instances do not give access to the featureCompatibilityVersion.
        // On a sharded cluster we currently assume the FCV is not set to 3.6.
        // PERF-1447 was filed to handle FCV on sharded clusters in the future.
        fcvRes = db.adminCommand({ getParameter: 1, featureCompatibilityVersion: 1});
        fcv = fcvRes.featureCompatibilityVersion.version;
        if (fcv == "3.6") {
            return true;
        }
    }
    return false;
}


/**********************************************************************
 * Other common functions shared by the change stream test workloads. *
 **********************************************************************/

/**
 * Gets the base name for a test given its configuration options.
 *
 * @param {Number} nListeners - The number of change stream listeners.
 * @param {String} fullDocumentBeforeChange - The "fullDocumentBeforeChange" change stream option.
 * @param {String} fullDocument - The "fullDocument" change stream option.
 * @param {Boolean} selectiveChange - Whether the 'selective_change' option is set.
 * @param {Number} nCollections - The number of collections used by the test.
 * @param {Number} docSize - The size of the documents inserted into the collection.
 * @param {Boolean} preImageEnabled - Whether recording of the pre-images is enabled.
 * @param {String} readPreferenceMode - The read preference of the change stream aggregation pipeline.
 * @return {String} The test base name.
 */
function getTestBaseName(nListeners, fullDocumentBeforeChange, fullDocument, selectiveChange, nCollections, docSize, preImageEnabled, readPreferenceMode) {
    var baseName = `${nListeners}`;
    if (fullDocumentBeforeChange) {
        baseName += `_${fullDocumentBeforeChange}fdbc`;
    }
    if (fullDocument) {
        if (fullDocument == "updateLookup") {
            // Previously, change stream tests that had fullDocument being set to "updateLookup",
            // were having a prefix of "_lookup". The newly added prefix is omitted for this
            // fullDocument parameter in order to preserve the historical data.
            baseName += "_lookup";
        } else {
            baseName += `_${fullDocument}fd`;
        }
    }
    if (selectiveChange) {
        baseName += "_filter";
    }
    if (preImageEnabled) {
        baseName += "_preImage";
    }
    baseName += `_${nCollections}c`;

    // Previously, change stream tests were only executed with document size of 100 bytes. The newly
    // added prefix is omitted for this document size in order to preserve the historical data.
    if (docSize != 100) {
        baseName += `_${docSize}d`;
    }

    if (readPreferenceMode != "primary") {
        baseName += `_${readPreferenceMode}`;
    }

    return baseName;
}

/**
 * Returns the change stream options object for the specified configuration.
 *
 * @param {String} fullDocumentBeforeChange - The 'fullDocumentBeforeChange' parameter of the $changeStream stage.
 * @param {String} fullDocument - The 'fullDocument' parameter of the $changeStream stage.
 */
function getChangeStreamOptions(fullDocumentBeforeChange, fullDocument) {
    let result = {};
    if (fullDocumentBeforeChange != undefined) {
        result["fullDocumentBeforeChange"] = fullDocumentBeforeChange;
    }
    if (fullDocument != undefined) {
        result["fullDocument"] = fullDocument;
    }
    return result;
}

/**
  * Returns true if the FCV is 6.1 or above (when the feature flag is removed) or if feature flag
  * 'featureFlagClusterWideConfig' is enabled with FCV 6.0, false otherwise.
  */
function isClusterWideConfigFeatureAvailable(db) {
    const getParam = db.adminCommand({getParameter: 1, featureFlagClusterWideConfig: 1});
    // On replica sets, the FCV needs to be checked so that the last LTS/continuous FCV replset variants
    // are accounted for. On sharded clusters, the FCV cannot be retrieved directly from the mongos
    // so we use the binary version instead.
    if (!sharded()) {
        const fcv = db.adminCommand({getParameter: 1, featureCompatibilityVersion: 1});
        const majorFCV = fcv.featureCompatibilityVersion.version.split('.')[0];
        const minorFCV = fcv.featureCompatibilityVersion.version.split('.')[1];
        return (majorFCV > 6 || (majorFCV == 6 && minorFCV >= 1)) || (getParam.hasOwnProperty("featureFlagClusterWideConfig") &&
                                    (getParam.featureFlagClusterWideConfig.value) && (majorFCV == 6) && (minorFCV == 0));
    } else {
        const majorVersion = db.version().split('.')[0];
        const minorVersion = db.version().split('.')[1];
        return (majorVersion > 6 || (majorVersion == 6 && minorVersion >= 1)) || (getParam.hasOwnProperty("featureFlagClusterWideConfig") &&
                                    getParam.featureFlagClusterWideConfig.value);
    }
}

function testChangeStreamsCRUDWorkload(db,
                                       testName,
                                       { dbName, collNames, docSize, nCollections, nThreads, nListeners, nDocs, retryableWrites, mongosHosts, shardCollections, preImagesEnabled, changeStreamOptions, fullDocumentBeforeChange, fullDocument, selectiveChange, changeHandlerOptions, testDurationSecs, changeStreamReadPreference },
                                       targetHost,
                                       changeHandler,
                                       onStopCallback,
                                       writeOnlyWorkload) {
    // Set the number of documents to be inserted before running the workload.
    var nDocuments = nDocs != undefined ? nDocs : 10000000 / docSize;
    var workload = new MixedCRUDWorkload(dbName, collNames, nThreads, retryableWrites,
                                         mongosHosts, docSize, nDocuments, shardCollections, writeOnlyWorkload);

    // Note: the workload initialization calls quiesce at the end.
    workload.initialize();

    if (changeStreamOptions && !isClusterWideConfigFeatureAvailable(db)) {
      jsTestLog("This test requires the time-based change stream pre-/post-image retention policy to be available. Skipping test.");
      return;
    }
    if (preImagesEnabled) {
      for (var i = 0; i < collNames.length; i++) {
        assert.commandWorked(db.getSiblingDB(dbName).runCommand({collMod: collNames[i], changeStreamPreAndPostImages: {enabled: true}}));
      }
    }
    if (isClusterWideConfigFeatureAvailable(db)) {
      var changeStreamOptions = changeStreamOptions ? changeStreamOptions : {preAndPostImages: {expireAfterSeconds: "off"}};
      assert.commandWorked(db.getSiblingDB("admin").runCommand(Object.merge({setClusterParameter: {changeStreamOptions: changeStreamOptions}})));
    }

    jsTest.log("Change streams CRUD " + testName + ": nThreads=" + nThreads + " nListeners=" + nListeners + " preImagesEnabled=" + preImagesEnabled + " docSize=" + docSize + " nCollections=" + nCollections + " shardCollections=" + shardCollections + " " + new Date());

    var changeStreamOptions = getChangeStreamOptions(fullDocumentBeforeChange, fullDocument);
    var listeners = startAndWaitForListeners(dbName, collNames, nListeners, changeStreamOptions,
                                             selectiveChange, changeHandler,
                                             changeHandlerOptions, mongosHosts, targetHost, changeStreamReadPreference);

    print("Starting the workload");
    workload.start();
    sleep(testDurationSecs*1000);
    print("Finishing the workload");
    var throughput = workload.stop();

    onStopCallback(throughput, listeners, getTestBaseName(nListeners, fullDocumentBeforeChange, fullDocument, selectiveChange, nCollections, docSize, preImagesEnabled, changeStreamReadPreference));
}
