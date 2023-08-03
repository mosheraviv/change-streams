#!/bin/bash
hash jsdoc 2>/dev/null || { echo >&2 "Error: 'jsdoc' is required, but was not found."; exit 1; }

# if WORKLOADS_DIR is not set then use CWD
# run in a subshell to preserve the CWD
# it is assumed this is run from the parent directory or that
(
    cd ${WORKLOADS_DIR:-.}
    jsdoc -d html/ workloads/*.js utils/mongoshell.js libs/*.js -c jsdoc/jsdoc.conf --readme jsdoc/start-page.md
    tar -czf html.tgz html
)
