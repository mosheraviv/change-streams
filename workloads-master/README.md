# Workloads

This is collection of mongo shell/benchRun based workloads.

# How to add a new workload?

There are few steps to do this. A good first step is talking to a
member of the performance team about what you want to do, and
reviewing the
[sys-perf guide](https://docs.google.com/a/10gen.com/document/d/1wnZQbuP9482TR7UP-SKEZtntdB2O2K3lE15nHc2XTpw/edit?usp=sharing)
for adding and running tests. A good second step is to open a PERF
JIRA ticket and to send an email to dev-prod-perf@10gen.com. For questions, reach out to
[#performance-tooling-users](https://mongodb.slack.com/archives/C01VD0LQZED) slack channel.

## Retrofit your workload

The framework provides a few helper functions/flags:

| variable | comment |
| -------- | ------- |
| `server`   | IP address for target server to be tested, could be mongod or mongos |
| `isSharded` | whether this is a sharded environment? You need properly enable sharding on your collection if this is true |
| `sharded()` | same as above, this is a function call, you do not need to worry about whether isSharded is defined, a simpler way to check |
| `isRepl` | whether this is a replica set |
| `isReplSet()` | same as above, but as a function call |


You need your workload to point to the server-under-test, for example, for `benchRun`:

```javascript
res = benchRun( {
    parallel : x ,
    seconds : 5 ,
    host: server,
    ops : ops
} );
```

Note that `benchRun` is built into the `mongo` shell.

## Report results in the proper format

The framework provides a function for you to report throughput

```js
reportThroughput( workload_name, throughput );
```

You can also report additional details, such as thread count, pass/fail, and error message:

```js
reportThroughput( workload_name, throughput, {nThread: 15, pass: false, errMsg: "Error during insert"} );
```

Please make sure the name is the same for one test, that is no need to
include thread number in test case name. The framework uses test name
to correlate results. We generally use (average) throughput as the test result. In some cases
latency / duration is reported as a negative value. This is to preserve higher-is-better semantics
in all cases.

## Add your workload

You need to add your test to workloads.yml in order to run the test
locally. If you are running the tests as part of the sys-perf
project, you will need to update the appropriate test_control configuration file
in the [DSI repo](https://github.com/10gen/dsi/tree/master/configurations/test_control). (There's
one file per evergreen task.)

## How to manually run workloads

**Note: The below will no longer work, at least for some workloads. Please use the method described
in [Using DSI Locally](https://docs.google.com/document/d/14QXOmo-ia8w72pW5zqQ2fCWfXEwiVQ8_1EoMCkB4baY/edit).**


First setup python:

```sh
virtualenv venv
source venv/bin/activate
pip install python-dateutil
pip install pyyaml
```

Then run 

```sh
python run_workloads.py --help
```

Please note:

- you need have your shell under `~/bin` folder, or you can use `--shelll` to specify mongo executable. Mac homebrew installs this as `/usr/local/bin/mongo`.
- if you want test **shard** target, please specify `-s` via command line argument

The script will pretty print all results into a table:

```
Results:
+----------------------+----------+--------------+----------+------------------------------+
| "Test"               | "Thread" | "Throughput" | "Pass?"  | "Comment"                    |
|----------------------+----------+--------------+----------+------------------------------|
| "find_and_update"    |        1 | 15232.966129 | true     | ""                           |
| "find_and_update"    |        2 | 22674.982652 | true     | ""                           |
| "find_and_update"    |        4 | 34649.832534 | true     | ""                           |
| "find_and_update"    |        8 | 34125.581692 | true     | ""                           |
| "find_and_update"    |       16 | 28876.067611 | true     | ""                           |
| "find_and_update"    |       32 | 23362.429662 | true     | ""                           |
| "find_and_update"    |       64 | 26426.276712 | true     | ""                           |
| "find_and_update"    |      128 | 17070.623782 | true     | ""                           |
+----------------------+----------+--------------+----------+------------------------------+
```

## Patch test your workload in dsi and evergreen

To run your workload within the dsi framework, you can

* [Run DSI manually from your command line](https://github.com/10gen/dsi/wiki/Running-DSI-Locally)
* [Run a patch build in Evergreen](https://docs.google.com/a/10gen.com/document/d/1wnZQbuP9482TR7UP-SKEZtntdB2O2K3lE15nHc2XTpw/edit?usp=sharing)


## Submit a Code Review

Please submit a code review and CC perf-cr@10gen.com. In addition,
include at least one performance team member on the To line
(preferably one you've already talked to about your test). Also
include any relevant people from your team.

## Installing and Using ESLint

* You can use `./bin/lint` to check fo any linting issues. This is a dumb wrapper atop `eslint`. It will automatically install `eslint` into `node_modules` for you.
* Use `./bin/lint --fix` to automatically fix them.
* The linter will use `.eslintignore` and `.eslintrc.yml` for the rules and ignores.
* Any args passed to `./bin/lint` are passed into `eslint`. You may want to refer to the [Installing and Using ESLint Wiki page](https://wiki.mongodb.com/pages/viewpage.action?spaceKey=KERNEL&title=Installing+and+Using+ESLint).

Consider creating separate `cr` and `crnl` aliases for lint and non-lint code review requests:

```sh
ESLINT_SCRIPT=/path/to/mongo/buildscripts/eslint.py
ESLINT_LOCATION=/path/to/workloads/node_modules/.bin/eslint
JIRA_USER=<your jira name>
UPLOADER=/path/to/kernel-tools/codereview/upload.py
alias crnl="$UPLOADER  --oauth2 -H mongodbcr.appspot.com -y --jira_user $JIRA_USER --git_similarity=100"
alias cr="crnl --check-eslint --eslint-location=$ESLINT_LOCATION --eslint-script=$ESLINT_SCRIPT"
```

**Note**: change `ESLINT_SCRIPT`, `ESLINT_LOCATION`, `JIRA_USER`, and `UPLOADER` to the correct values.


## Pylint version requirements

Currently pylint is pinned at 1.5.5. Please ensure that this version is the active packatge.

```sh
$ pip install pylint==1.5.5
```
