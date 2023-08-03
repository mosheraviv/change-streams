#!/usr/bin/env python
"""
SSH into another node and then execute run_workloads.py there.

See test_control.cpu_noise.yml, where it's used.
"""

from __future__ import print_function

import sys
import datetime
import subprocess

import argparse
from dateutil import tz
import yaml

def main():
    """Main function running the workloads"""
    parser = argparse.ArgumentParser(
        description='Execute run_workloads.py on a list of remote hosts.')
    parser.add_argument('-c',
                        '--config',
                        required=True,
                        help='Config file to load.')
    args = parser.parse_args()

    # Default config. Should be run with an actual config
    config = {}

    config.update(yaml.safe_load(open(args.config)))
    remote_execution = config['remote_execution']

    print('\n')
    print('run_remote_workloads.py: About to execute the following command:')
    print('\n')
    print('    ' + remote_execution['remote_cmd'])
    print('\n')
    print('on the following list of remote hosts:')
    print('\n')
    print(yaml.dump(remote_execution['host_list'], default_flow_style=False))

    timestamp_file = open('workload_timestamps.csv', 'w+')
    timestamp_file.write('time, test number\n')
    try:
        run_on_hosts(timestamp_file, remote_execution)
    except subprocess.CalledProcessError as error:
        exit("ERROR: Remote command '{}' failed with exit status {}".format(
            error.cmd, error.returncode))
    finally:
        timestamp_file.close()


def run_on_hosts(timestamp_file, config):
    """Execute remote_cmd on each host_list."""
    remote_prefix = 'remote'
    if 'remote_prefix' in config:
        remote_prefix = config['remote_prefix']

    remote_nr = 1
    for host in config['host_list']:
        host_addr = ''
        if 'private_ip' in host:
            host_addr = host['private_ip']
        else:
            host_addr = host['public_ip']

        host_alias = remote_prefix + "_" + str(remote_nr)
        remote_cmd = config['remote_cmd'] + ' --result-prefix ' + host_alias

        timestamp_file.write('{}, {}\n'.format(
            datetime.datetime.now(tz.tzlocal()).isoformat(), 0))
        print("Running cmd '{}' on host: {}".format(remote_cmd, host_addr))
        timestamp_file.write('{}, {}\n'.format(
            datetime.datetime.now(tz.tzlocal()).isoformat(), remote_nr))

        try:
            run_on_host(host_addr, remote_cmd)

        finally:
            timestamp_file.write('{}, {}\n'.format(
                datetime.datetime.now(tz.tzlocal()).isoformat(), remote_nr))
            timestamp_file.write('{}, {}\n'.format(
                datetime.datetime.now(tz.tzlocal()).isoformat(), 0))
        remote_nr += 1



def run_on_host(host_addr, remote_cmd):
    """Execute ssh to another host and then run exec_remote_cmd."""
    process = subprocess.Popen(['ssh', '-A', '-oStrictHostKeyChecking=no',
                                host_addr, remote_cmd],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b''):
        line = line.decode("utf8")
        print(line.rstrip())
        sys.stdout.flush()
    # wait for the process to terminate
    process.communicate()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, host_addr + ': ' + remote_cmd)

if __name__ == '__main__':
    main()
