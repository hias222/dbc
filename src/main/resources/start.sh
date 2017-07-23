#!/bin/bash

# shell script

export ANALYSIS_CLASSPATH=$VIEW_HOME:$VIEW_HOME/lib/commons-cli-1.2.jar:$VIEW_HOME/lib/commons-cli-1.2.jar:$VIEW_HOME/lib/commons-lang3-3.1.jar:$VIEW_HOME/lib/gsp.jar:$VIEW_HOME/lib/jackson-annotations-2.2.3.jar:$VIEW_HOME/lib/jackson-core-2.2.3.jar:$VIEW_HOME/lib/jackson-databind-2.2.3.jar:$VIEW_HOME/lib/log4j-api-2.0-beta9.jar:lib/log4j-core-2.0-beta9.jar:$VIEW_HOME/lib/ojdbc7.jar:$VIEW_HOME/lib/oracle-ucp-11.2.jar

export INPUT_QUERIES=/app/ids/dbcnew/viewanalyzer/Queries
export OUTPUT_DIR=/app/ids/dbcnew/viewanalyzer/Output

java -cp $ANALYSIS_CLASSPATH com.boettinger.analyzer.SqlViewsAnalyzerCli -i $INPUT_QUERIES -o $OUTPUT_DIR
