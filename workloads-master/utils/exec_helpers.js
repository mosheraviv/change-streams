var ExecHelpers = (function() {

    /**
     * Measures the bandwidth for executing 'func', passing the number of times 'func' has been
     * called so far as the argument to 'func'. Runs repeated trials, until the cumulative time has
     * exceeded 'testSeconds', and returns an object which includes the mean throughput in
     * operations per second as well as an array containing the latency in milliseconds of each
     * completed operation. An optional 'setupFunc' can also be specified, which will be called
     * before each trial; the time taken to perform the setup will not be included in the
     * latency figures for the trial, and will not count towards the cumulative 'testSeconds'.
     */
    function _measurePerformance(func, testSeconds, setupFunc) {
        testSeconds = testSeconds || 2 * 60;
        var elapsedMillis = 0;
        var numTrials = 0;
        quiesceSystem();
        var allLatencies = [];
        while (elapsedMillis < testSeconds * 1000) {
            numTrials++;
            if (setupFunc) {
                setupFunc(numTrials);
            }
            var start = new Date();
            func(numTrials);
            var end = new Date();
            allLatencies.push(end - start);
            elapsedMillis += (end - start);
        }
        var meanThroughput = 1000 * numTrials / elapsedMillis;
        return {meanThroughput: meanThroughput, allLatencies: allLatencies};
    }

    /**
     * Drops and recreates the given collection, if it exists and is not empty. Does not repopulate
     * the new collection. This function ensures that the collection is recreated with the same
     * options and indexes as its predecessor, and in the case of a sharded collection that its
     * chunk distribution remains consistent.
     */
    function _resetCollection(collToReset) {
        // Return immediately if the collection doesn't exist or is already empty.
        var collInfo = collToReset.exists();
        if (!collInfo || collToReset.count() == 0) {
            return;
        }
        // Helper to perform the actual drop and recreate.
        function _dropAndRecreateCollection() {
            // Obtain the list of indexes that are currently present on this collection.
            var collIndexes = collToReset.getIndexes();
            // Drop the collection, then recreate it with the same options and indexes.
            assert(collToReset.drop());
            assert.commandWorked(collToReset.runCommand("create", collInfo.options));
            assert.commandWorked(collToReset.runCommand("createIndexes", {indexes: collIndexes}));
        }
        // If the collection is not sharded, just drop and recreate it.
        if (!sharded()) {
            _dropAndRecreateCollection();
            return;
        }
        // Obtain the sharding spec and current chunk distribution for this collection.
        var configDB = collToReset.getDB().getSiblingDB("config");
        var shColl = configDB.collections.findOne({_id: collToReset.getFullName()});

        // On versions 5.0 and greater, chunk documents have a 'uuid' field that can be used to join
        // them to their collection, but no 'ns' field.
        // On versions lower than 5.0, chunk documents have the 'ns' field, but not the 'uuid'
        // field.
        var serverVersion = db.version().split(".");
        var chunksQuery = serverVersion[0] < 5 ? {ns: shColl._id} : {uuid: shColl.uuid};

        var chunkDistribution =
            configDB.chunks.find(chunksQuery).sort({min: 1}).toArray();
        // If this is a hash-sharded collection, we explicitly create a single initial chunk. This
        // will allow us to split the collection in the same way as we do for the non-hashed case.
        var isHashed = Object.keys(shColl.key).some(function(k) {
            return shColl.key[k] === "hashed";
        });

        // Drop the collection, then recreate and shard it.
        _dropAndRecreateCollection();
        assert.commandWorked(configDB.adminCommand({
            shardCollection: collToReset.getFullName(),
            key: shColl.key,
            unique: shColl.unique,
            numInitialChunks: (isHashed ? 1 : 0)
        }));
        // Split the collection at each of the max bounds of the chunks, and move the resulting
        // chunks to the appropriate shard.
        for (var i = 0; i < chunkDistribution.length; ++i) {
            // We don't need to split the final chunk in the list.
            if (i < chunkDistribution.length - 1) {
                assert.commandWorked(configDB.adminCommand(
                    {split: collToReset.getFullName(), middle: chunkDistribution[i].max}));
            }
            // Move the chunk to the shard it was previously on.
            assert.commandWorked(configDB.adminCommand({
                moveChunk: collToReset.getFullName(),
                bounds: [chunkDistribution[i].min, chunkDistribution[i].max],
                to: chunkDistribution[i].shard,
                _waitForDelete: true
            }));
        }
    }

    /**
     * Returns the Cartesian product of the given argument list of lists.
     */
    function _cartesianProduct(...args) {
        const flat = (arr) => arr.reduce((acc, val) => acc.concat(val), []);
        const flatMap = (arr, f) => flat(arr.map(f));
        return args.reduce((acc, arg) => {
            return flatMap(acc, productSoFar => {
                return arg.map(option => flat([productSoFar, option]))
            });
        });
    }

    return {measurePerformance: _measurePerformance, resetCollection: _resetCollection, cartesianProduct: _cartesianProduct};
}());
