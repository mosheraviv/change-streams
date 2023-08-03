/**
 * @file
 *
 * Test the latency performance of change stream listeners under a write workload.
 * <br>
 *
 * ### *Setup*
 * None.
 *
 * ### *Test*
 * The test applies a write workload to a collection in the cluster while a number of threads
 * read change streams for the collection.
 * The measured throughputs are the average latency and maximum latency of the change streams.
 * Results are reported as seconds.
 * The latency is approximated by taking the difference between the clusterTime of a change document
 * and the clusterTime of the getmore command it came from.
 * The reading threads simply loop over the change stream cursors, keeping track of the latency
 * measurements.
 *
 * The test takes two main optional parameters: an array specifying the number of write threads
 * 'thread_levels' and an array specifying the number of change stream listeners 'listener_levels'.
 * The test then runs all the combinations of number of threads and number of listeners specified.
 * Other parameters exist to specify different change stream behaviors, the number of collections
 * to target or sharding specific options.
 *
 * ### *Notes*
 * - The test runs for 5 minutes
 *
 * ### *Owning-team*
 * mongodb/replication
 * 
 * ### *Keywords*
 * 
 * @module workloads/change_streams_latency
 */

/*global
 assert print reportThroughput sharded sleep
*/
load("libs/change_streams.js");
load("libs/mixed_workload.js");

/**
 * The actual values in use for the following parameters are injected by run_workloads.py, which gets it from the config file.
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 * Parameters: thread_levels, listener_levels, selective_change, nb_collections, mongos_hosts, shard_collections,
 * nb_docs, doc_size, pre_images_enabled, full_document_before_change, full_document, change_stream_options, test_duration_secs.
 */

/**
 * The number of threads to run in parallel. The default is [24].
 * **Note:** Thread level should be divisible by 3 * nb_collections * mongos_hosts.length.
 */
var thread_levels = thread_levels || [24];

/** The number of listeners to run in parallel. The default is [1]. */
var listener_levels = listener_levels || [1];

/**
 * Whether the change stream pipeline should include an additional filtering stage.
 * Defaults to false.
 */
var selective_change = selective_change || false;

/** The number of collections to run the workload and listeners against. Defaults to [1]. */
var nb_collections = nb_collections || [1];

/**
 * The mongos hosts assigned IP addresses. The expected format is a list of dictionaries containing
 * a "private_ip" field.
 * This parameter is used to connect through multiple mongoses in a sharded cluster.
 * If left unassigned, the workload and listeners will connect to the default mongos.
 * Only applies if the cluster is sharded.
 */
var mongos_hosts = mongos_hosts || [];

/**
 * If the collections should be sharded when running against a sharded cluster.
 * Defaults to true.
 * Only applies if the cluster is sharded.
 */
var shard_collections = shard_collections !== false;

/**
 * The size in bytes of the documents to insert in each collection during the workload.
 * Defaults to [100] document size.
 */
var doc_sizes = doc_sizes || [100];

/**
 * The number of documents to insert in each collection before starting the workload.
 * Defaults to undefined.
 * If 'nb_docs' is undefined, the value is computed dynamically: nb_docs = 10_000_000 / doc_size.
 */
var nb_docs = nb_docs || undefined;

/**
 * Represents the collection option of pre-image recording being enabled for all collections.
 * Defaults to false.
 */
var pre_images_enabled = pre_images_enabled || false;

/**
 * Possible modes for the 'fullDocumentBeforeChange' parameter of the $changeStream stage.
 * Defaults to undefined.
 */
var full_document_before_change = full_document_before_change || undefined;

/**
 * Possible modes for the 'fullDocument' parameter of the $changeStream stage.
 * Defaults to undefined.
 */
var full_document = full_document || undefined;

/*
 * A specification for the change streams options.
 * Defaults to undefined.
 */
var change_stream_options = change_stream_options || undefined;

/**
 * The test duration in seconds per thread level and listener level combination.
 * Defaults to 300 (5 minutes).
 * Note: stalls come in 60 sec cycles.
 */
var test_duration_secs = test_duration_secs || 5*60;

/**
 * Determines the read preference for the change stream.
 * Defaults to primary.
 */
var change_stream_read_preference = change_stream_read_preference || "primary";

/**
 * The target host on which change stream will be opened. Defaults to undefined.
 */
var target_host = target_host || undefined;

(function() {
    "use strict";

    load("utils/exec_helpers.js");  // For 'ExecHelpers'.

    var dbName = "change_streams";
    var retryableWrites = false;
    var mongosHosts;
    if (sharded()) {
        mongosHosts = getMongosHosts(mongos_hosts);
    }

    // The options to pass the the changeHandler function.
    var changeHandlerOptions = {"useBinDataResumeToken": useBinDataResumeToken()};

    /**
     * A callback function for the ChangeStreamListenerThread that computes average and max latency.
     */
    function changeHandler(doc, state, options) {
        // Inlining the code to extract the cluster time in order to execute in ScopedThread.
        var hex;
        if (options.useBinDataResumeToken) {
            hex = doc._id._data.hex();
        } else {
            hex = doc._id._data;
        }
        // In the resume token the clusterTime is encoded first.
        // 130 is the content id for Timestamp.
        assert.eq(130, parseInt(hex.slice(0, 2), 16));
        // The 4 following bytes are the seconds for the timestamp.
        var docClusterTime = parseInt(hex.slice(2, 10), 16);
        // Done extracting clusterTime from doc.

        var latency = state.clusterTime - docClusterTime;
        var value = state.value;
        if (!value) {
            state.value = {"count": 1, "avg": latency, "max": latency};
            return;
        }
        state.value.avg = (value.count * value.avg + latency) / (value.count + 1);
        if (latency > value.max) {
            state.value.max = latency;
        }
        state.value.count += 1;
    }

    /**
     * A callback function that prepares and reports the latency data on finishing running the given workload.
     */
    function onStopHandler(throughput, listeners, baseName, nThreads) {
        print("Stopping listeners");
        var total_latency_avg = 0;
        var total_latency_count = 0;
        var total_latency_max = null;
        var latencies = [];
        for (var collName in listeners) {
            print("Stopping listeners for collection " + collName);
            latencies = latencies.concat(stopListeners(listeners[collName]));
        }
        var latency;
        for (var j = 0; j < latencies.length; j++) {
            latency = latencies[j];
            total_latency_avg =
                (total_latency_avg * total_latency_count + latency.avg * latency.count) /
                (total_latency_count + latency.count);
            total_latency_count += latency.count;
            if (total_latency_max === null || latency.max > total_latency_max) {
                total_latency_max = latency.max;
            }
        }

        reportThroughput(baseName + "_avg_latency",
                         - total_latency_avg, {nThread: nThreads});
        reportThroughput(baseName + "_max_latency",
                         - total_latency_max, {nThread: nThreads});
        reportThroughput(baseName + "_insert",
            throughput.insert, {nThread: nThreads});
        reportThroughput(baseName + "_update",
            throughput.update, {nThread: nThreads});
        reportThroughput(baseName + "_delete",
            throughput.delete, {nThread: nThreads});
        reportThroughput(baseName + "_total",
            throughput.total, {nThread: nThreads});
    }

    /**
     * Tests the latency performance.
     *
     * @param {Array} collNames - An array of collection names on which to apply the workload.
     * @param {Number} docSize - The size in bytes of the documents to insert in each collection during the workload.
     * @param {Number} nCollections - The number of collections to run the workload and listeners against.
     * @param {Number} nThreads - The number of CRUD workload threads to use.
     * @param {Number} nListeners - The number of change stream listeners to start.
     */
    function testChangeStreams(collNames, docSize, nCollections, nThreads, nListeners) {
        var workloadParameters = {
            dbName: dbName,
            collNames: collNames,
            docSize: docSize,
            nCollections: nCollections,
            nThreads: nThreads,
            nListeners: nListeners,
            nDocs: nb_docs,
            retryableWrites: retryableWrites,
            mongosHosts: mongosHosts,
            shardCollections: shard_collections,
            preImagesEnabled: pre_images_enabled,
            changeStreamOptions: change_stream_options,
            fullDocumentBeforeChange: full_document_before_change,
            fullDocument: full_document,
            selectiveChange: selective_change,
            changeHandlerOptions: changeHandlerOptions,
            testDurationSecs: test_duration_secs,
            changeStreamReadPreference: change_stream_read_preference,
        };
        const writeOnlyWorkload = true;
        testChangeStreamsCRUDWorkload(db, "latency", workloadParameters, target_host, changeHandler, function (throughput, listeners, baseName) {
            onStopHandler(throughput, listeners, baseName, nThreads);
        }, writeOnlyWorkload);
    }

    ExecHelpers
        .cartesianProduct(
            doc_sizes,
            nb_collections,
            thread_levels,
            listener_levels
        )
        .forEach(function(args) {
            var nCollections = args[1];
            var nThreads = args[2];
            var collectionNames = generateCollectionNames("change_streams_" + nThreads, nCollections);
            testChangeStreams(collectionNames, args[0], nCollections, nThreads, args[3]);
        });
})();
