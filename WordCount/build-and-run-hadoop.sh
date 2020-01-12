#!/bin/bash

# Mastère Big Data 2019/2020 - TP Hadoop
#
# Benjamin Thery - benjamin.thery@grenoble-inp.org

#set -x

MYCLASS=$1
INPUT_FILE=$2
OUTPUT_DIR=$3

banner() {
	echo "====================================================================="
	echo "$*"
	echo "====================================================================="
}

#
# Remove hadoop output directory if it exists
#
if [ -d $OUTPUT_DIR ]; then
	read -p "Remove output directory '$OUTPUT_DIR' ? (y/n) " ANSWER
	if [ "$ANSWER" == "y" ]; then
		rm -fr $OUTPUT_DIR
	fi
fi

#
# Compile
#
banner "Compile $MYCLASS.java"
mkdir -p build
rm build/*.class 2> /dev/null

javac -classpath $(hadoop classpath):/usr/share/java:build -d build $MYCLASS.java
[ $? != 0 ] && exit 1

#
# Make jar archive
#
banner "Make jar archive $MYCLASS.jar"
jar -cvf $MYCLASS.jar -C build .
[ $? != 0 ] && exit 1

# Display jar contents
jar -tvf $MYCLASS.jar

# Run hadoop
banner "Run Hadoop"
hadoop jar $1.jar $MYCLASS $INPUT_FILE $OUTPUT_DIR 
