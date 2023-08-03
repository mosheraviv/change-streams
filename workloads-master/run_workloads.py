#!/usr/bin/env python
"""
Run the workloads in the custom workloads repository.
"""

from __future__ import print_function

import sys
import datetime
import os
import subprocess

import argparse
from dateutil import tz
import yaml

# How many times to repeat each test
# See PERF-814 for explanation. Note that intended use was for patch builds only, which is why this
# is just a variable you can edit, not settable in config (for now).
TEST_ITERATIONS = 1


def main():
    """Main function running the workloads"""
    # pylint complains about too many branches (by 1). Most of them are simple, so I'm disabling
    # the check

    # pylint: disable=too-many-branches,too-many-statements,too-many-locals
    parser = argparse.ArgumentParser(description='Run custom workloads')
    parser.add_argument(
        '-c',
        '--config',
        action='append',
        help='Config file to load. Can be called multiple times and combined.'
        'On conflicts the last file on the command line wins')
    parser.add_argument(
        '-l',
        '--test-list',
        help='Specify the list of tests to run from the config file')
    parser.add_argument(
        '-t',
        '--target',
        help='Address for mongos/mongod, DNS or IP, default to localhost')
    parser.add_argument(
        '-p', '--port', help='Mongos/mongod port, default to 27017')
    parser.add_argument(
        '-s',
        '--sharded',
        help='Whether target is sharded, default to not sharded',
        action='store_true')
    parser.add_argument(
        '-r',
        '--replica',
        help='Whether target is replica set, default to not a replica set',
        action='store_true')
    parser.add_argument(
        '--result-prefix',
        help='Prefix to use for test/metric name when reporting test results')
    parser.add_argument(
        '-w',
        '--workloads',
        help='list of workloads, for example "test1.js test2.js"',
        nargs='*')
    parser.add_argument(
        '--shell', help='Location of mongo shell. Default ~/bin/mongo')
    args = parser.parse_args()

    # Default config. Should be run with an actual config
    config = {
        'tests': {
            'test': ['hello.js'],
            'default': []
        },
        'shell': '~/bin/mongo',
        'shell_ssl_options': '',
        'sharded': False,
        'target': 'localhost',
        'test_list': 'default',
        'replica': False,
        'result_prefix': '',
        'port': '27017',
        'scale_factor': '1',
        "authentication": {"enabled": True, "username": "username", "password": "password",},
    }

    if args.config:
        for conf in args.config:
            config.update(yaml.safe_load(open(conf)))
    if args.test_list:
        config['test_list'] = args.test_list
    workloads = config['tests'][config['test_list']]
    print('\nWorkloads from config file for {}: {}'.format(config['test_list'],
                                                           workloads))

    # Need to check on paths for these things
    results_file = 'test_results.log'
    out = open(results_file, 'w')

    # Override defaults based on command line options
    if args.workloads:
        workloads = args.workloads

    if args.port:
        config['port'] = args.port
    if args.result_prefix:
        config['result_prefix'] = args.result_prefix
    if args.shell:
        config['shell'] = args.shell
    if args.target:
        config['target'] = args.target

    # These next two only override if set to true. Should come back to this.
    if args.sharded:
        config['sharded'] = True
    if args.replica:
        config['replica'] = True

    # Sanity check. Are sharded and replica?
    if config['sharded'] and config['replica']:
        sys.stderr.write(
            'Arguments sharded and replica are both set. Not allowed.\n'
        )
        sys.exit(1)

    print('Run test to target server: [{}] with isSharded={}'.format(
        config['target'], config['sharded']))

    mongo = os.path.expanduser(config['shell'])
    print('Using mongo shell: {}'.format(mongo))
    print('Current config: {}'.format(config))
    eval_string_base = 'var isSharded={}; var isRepl={};var server="{}:{}"; var resultPrefix="{}";'\
                                       'var scaleFactor={}; var authEnabled={}; var username="{}";'\
                                       'var password="{}";'
    if not isinstance(workloads, list):
        sys.stderr.write('Workloads is not a list.\n')
        sys.exit(1)
    if len(workloads) == 0:
        sys.stderr.write('Workloads list is empty. Nothing to do.\n')
        sys.exit(1)

    # This set of loops creates a list of workloads and their eval strings to run.
    eval_data = []
    for workload in workloads:
        print('Processing workload {}'.format(workload))
        eval_string = eval_string_base.format(
            str(config['sharded']).lower(),
            str(config['replica']).lower(), config['target'],
            str(config['port']), config['result_prefix'],
            config['scale_factor'], str(config["authentication"]["enabled"]).lower(),
            config["authentication"]["username"], config["authentication"]["password"])
        if isinstance(workload, dict):
            for workload_name, parameter_dicts in workload.items():
                workload_name = _normalize_workload_name(workload_name)
                if isinstance(parameter_dicts, dict):
                    add_parameter_dict(parameter_dicts, workload_name,
                                       eval_string, eval_data)
                else:
                    for parameter_dict in parameter_dicts:
                        # Each parameter_dict represents a different
                        # set of variables with which to run the workload.
                        add_parameter_dict(parameter_dict, workload_name,
                                           eval_string, eval_data)
        else:
            workload = _normalize_workload_name(workload)
            eval_data.append({workload: eval_string})

    # Run through each workload and run it with its eval string.

    timestamp_file = open('workload_timestamps.csv', 'w+')
    timestamp_file.write('time, test number\n')
    try:
        run_workloads(timestamp_file, out, mongo, config, eval_data)
    except subprocess.CalledProcessError as error:
        exit('ERROR: workloads/{} failed with exit status {}'.format(
            error.cmd, error.returncode))
    finally:
        timestamp_file.close()
        out.close()


def _normalize_workload_name(workload):
    return workload if workload.endswith('.js') else "{}.js".format(workload)


def run_workloads(timestamp_file, report_file, mongo_shell, config, eval_data):
    """Run all the workloads in eval_data."""
    test_nr = 1
    for eval_pair in eval_data:
        for workload_name, eval_string in eval_pair.items():
            # Timestamps for consumption by timeseries tool.
            # Need to put two timestamps, one right before and one right afterwards
            # so we get square waves rather than triangles in timeseries.
            timestamp_file.write('{}, {}\n'.format(
                datetime.datetime.now(tz.tzlocal()).isoformat(), 0))
            print("Running workload {} with eval string '{}'".format(
                workload_name, eval_string))
            timestamp_file.write('{}, {}\n'.format(
                datetime.datetime.now(tz.tzlocal()).isoformat(), test_nr))
            try:
                for n in range(TEST_ITERATIONS):
                    if TEST_ITERATIONS > 1:
                        print("Repeating workload {} {} more times.".format(
                            workload_name, TEST_ITERATIONS - n))

                    run_workload(mongo_shell, config, eval_string,
                                 workload_name, report_file)
            finally:
                timestamp_file.write('{}, {}\n'.format(
                    datetime.datetime.now(tz.tzlocal()).isoformat(), test_nr))
                timestamp_file.write('{}, {}\n'.format(
                    datetime.datetime.now(tz.tzlocal()).isoformat(), 0))
            test_nr += 1


def add_parameter_dict(parameter_dict, workload_name, eval_string, eval_data):
    """
    >>> add_parameter_dict({}, 'foo', 'bar', [])
    [{'foo': 'bar'}]
    >>> add_parameter_dict({'p': 1}, 'foo', 'bar', [])
    [{'foo': 'bar var p=1;'}]
    >>> add_parameter_dict({'p': 1, 'q': 2}, 'foo', 'bar', [])
    [{'foo': 'bar var q=2; var p=1;'}]
    >>> add_parameter_dict({'p': 1, 'q': [2, 3]}, 'foo', 'bar', [])
    [{'foo': 'bar var q=2; var p=1;'}, {'foo': 'bar var q=3; var p=1;'}]
    >>> add_parameter_dict({'p': 1, 'q': [2, 3]}, 'foo', 'bar', [{'a': 1}])
    [{'a': 1}, {'foo': 'bar var q=2; var p=1;'}, {'foo': 'bar var q=3; var p=1;'}]
    >>> add_parameter_dict({'p': [1, 4], 'q': [2, 3]}, 'foo', 'bar', [])
    [{'foo': 'bar var q=2; var p=1;'}, {'foo': 'bar var q=2; var p=4;'},\
 {'foo': 'bar var q=3; var p=1;'}, {'foo': 'bar var q=3; var p=4;'}]
    """
    # Get all combinations of vars if the parameter_dict are a matrix.
    eval_vars = [{}]

    # Iterate though each variable, if it's in matrix form, add all of the variable combinations.
    for var, value in parameter_dict.items():
        for eval_var in eval_vars:
            eval_var[var] = value

    # Create the eval string for each eval variable combo and add them all to the list.
    for eval_var in eval_vars:
        add_eval_string(eval_var, workload_name, eval_string, eval_data)
    return eval_data


def add_matrix_var_to_eval_vars(var, value_list, eval_var, new_eval_vars):
    """
    >>> add_matrix_var_to_eval_vars('foo', [], {}, [])
    []
    >>> add_matrix_var_to_eval_vars('foo', [True], {}, [])
    [{'foo': True}]
    >>> add_matrix_var_to_eval_vars('foo', [True, False], {}, [])
    [{'foo': True}, {'foo': False}]
    >>> add_matrix_var_to_eval_vars('foo', [True, False], {'bar': 1}, [])
    [{'foo': True, 'bar': 1}, {'foo': False, 'bar': 1}]
    >>> add_matrix_var_to_eval_vars('foo', [True, False], {'bar': 1}, [{'baz': 2}])
    [{'baz': 2}, {'foo': True, 'bar': 1}, {'foo': False, 'bar': 1}]
    """
    for value in value_list:
        eval_var[var] = value
        new_eval_vars.append(eval_var.copy())
    return new_eval_vars


def add_eval_string(eval_var, workload_name, eval_string, eval_data):
    """
    >>> add_eval_string({}, 'foo', 'bar', [])
    [{'foo': 'bar'}]
    >>> add_eval_string({'a': 1}, 'foo', 'bar', [])
    [{'foo': 'bar var a=1;'}]
    >>> add_eval_string({'a': 1, 'b':True}, 'foo', 'bar', [])
    [{'foo': 'bar var a=1; var b=true;'}]
    >>> add_eval_string({'a': 1, 'c':'hi'}, 'foo', 'bar', [])
    [{'foo': 'bar var a=1; var c="hi";'}]
    >>> add_eval_string({'a': 1, 'c':'hi'}, 'foo', 'bar', [{'a':'b'}])
    [{'a': 'b'}, {'foo': 'bar var a=1; var c="hi";'}]
    """
    for var, value in eval_var.items():
        if isinstance(value, str):
            eval_string += ' var {}="{}";'.format(var, value)
        elif value is True:
            eval_string += ' var {}=true;'.format(var)
        elif value is False:
            eval_string += ' var {}=false;'.format(var)
        else:
            eval_string += ' var {}={};'.format(var, value)
    eval_data.append({workload_name: eval_string})
    return eval_data


def run_workload(mongo, config, eval_string, workload_name, out):
    """Run a subprocess to execute the workload."""

    # Shell ssl options is defined as a string. We split it up by space here before passing
    # them to Popen(). It's not a great solution, but DSI needs to interpolate the options
    # defined  in shell commands in config YAMLs, so lists won't work without getting config.py
    # to convert all lists to strings.
    ssloptions = config['shell_ssl_options'].split()
    auth_settings = config['authentication']
    if auth_settings['enabled']:
        auth_string = [
            '-u',
            auth_settings['username'],
            '-p',
            auth_settings['password'],
            '--authenticationDatabase',
            'admin',
        ]
    else:
        auth_string = []
    print(        [
            mongo, '--host', config['target'], '--port', str(config['port'])
        ] + ssloptions + auth_string + [
            '--eval', eval_string, './utils/mongoshell.js',
            './workloads/{}'.format(workload_name), './utils/final.js'
        ],
)
    process = subprocess.Popen(
        [
            mongo, '--host', config['target'], '--port', str(config['port'])
        ] + ssloptions + auth_string + [
            '--eval', eval_string, './utils/mongoshell.js',
            './workloads/{}'.format(workload_name), './utils/final.js'
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b''):
        line = line.decode("utf8")
        out.write(line)
        print(line.rstrip())
        sys.stdout.flush()
    # wait for the process to terminate
    process.communicate()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, workload_name)


if __name__ == '__main__':
    main()
