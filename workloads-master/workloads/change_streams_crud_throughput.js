/**
 * @file
 *
 * Test performance of database load in the presence of change stream listeners.
 * <br>
 *
 * ### *Setup*
 * None.
 *
 * ### *Test*
 * The test applies a mixed CRUD workload to a collection in the cluster while a number of threads
 * read change streams for the collection.
 * The measured throughputs are the throughputs of the CRUD workload.
 * The reading threads simply loop over the change stream cursor, increasing a counter without
 * saving or looking at the data.
 *
 * The test takes two main optional parameters: an array specifying the number of CRUD threads
 * 'thread_levels' and an array specifying the number of change stream listeners 'listener_levels'.
 * The test then runs all the combinations of number of threads and number of listeners specified.
 * Other parameters exist to specify different change stream behaviors, the number of collections
 * to target or sharding specific options.
 *
 * Results are reported as ops/sec.
 *
 * ### *Notes*
 * - The test runs for 5 minutes.
 *
 * ### *Owning-team*
 * mongodb/replication
 * 
 * ### *Keywords*
 * 
 * @module workloads/change_streams_crud_throughput
 */

/*global
 print reportThroughput sleep
*/
load("libs/change_streams.js");
load("libs/mixed_workload.js");

/**
 * The actual values in use for the following parameters are injected by run_workloads.py, which gets it from the config file.
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 * Parameters: thread_levels, listener_levels, selective_change, nb_collections, mongos_hosts, shard_collections,
 * doc_size, nb_docs, pre_images_enabled, full_document_before_change, full_document, change_stream_options, test_duration_secs.
 */

/**
 * The number of threads to run in parallel. The default is [4, 64].
 * **Note:** Thread level should be divisible by 4 * nb_collections * mongos_hosts.length.
 */
var thread_levels = thread_levels || [4, 64];

/** The number of listeners to run in parallel. The default is [1, 10, 100, 1000]. */
var listener_levels = listener_levels || [1, 10, 100, 1000];

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

(function() {
    "use strict";

    load("utils/exec_helpers.js");  // For 'ExecHelpers'.

    var dbName = "change_streams";
    var retryableWrites = false;
    var mongosHosts;
    if (sharded()) {
        mongosHosts = getMongosHosts(mongos_hosts);
    }

    // Options for the changeHandler callback;
    var changeHandlerOptions = null;

    /**
     * A callback function for the ChangeStreamListenerThread that counts the number of documents.
     */
    function changeHandler(doc, state, options) {
        state.value += 1;
    }

    /**
     * A callback function that reports the throughput data on finishing running the given workload.
     */
    function onStopHandler(throughput, listeners, baseName, nThreads) {
        reportThroughput(baseName + "_findOne",
                         throughput.findOne, {nThread: nThreads});
        reportThroughput(baseName + "_insert",
                         throughput.insert, {nThread: nThreads});
        reportThroughput(baseName + "_update",
                         throughput.update, {nThread: nThreads});
        reportThroughput(baseName + "_delete",
                         throughput.delete, {nThread: nThreads});
        reportThroughput(baseName + "_total",
                         throughput.total, {nThread: nThreads});

        print("Stopping the listeners");
        var counts = null;
        for (var collName in listeners) {
            counts = stopListeners(listeners[collName]);
        }
    }

    /**
     * Tests the CRUD workload performance.
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
        testChangeStreamsCRUDWorkload(db, "throughput", workloadParameters, undefined /* targetHost */, changeHandler, function (throughput, listeners, baseName) {
            onStopHandler(throughput, listeners, baseName, nThreads);
        });
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
