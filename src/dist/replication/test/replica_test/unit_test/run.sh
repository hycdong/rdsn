#!/bin/sh

if [ -z "${REPORT_DIR}" ]; then
    REPORT_DIR="."
fi

#./clear.sh
#output_xml="${REPORT_DIR}/dsn.replica.test.1.xml"
#GTEST_OUTPUT="xml:${output_xml}" ./dsn.replica.test

./clear.sh
output_xml="${REPORT_DIR}/dsn.replica.test.1.xml"
GTEST_OUTPUT="xml:${output_xml}" GTEST_FILTER="replica_bulk_load_test.*" ./dsn.replica.test
