#!/bin/bash

echo "📦 Bootstrapping full Hadoop automation..."

# --- Step 1: Start Hadoop Services ---

echo "🚀 Starting Hadoop DFS and YARN..."
start-dfs.sh
sleep 5
start-yarn.sh
sleep 5

# --- Wait for HDFS to leave safe mode ---
echo "⏳ Waiting for HDFS to leave safe mode..."
until hdfs dfsadmin -safemode get | grep -q "Safe mode is OFF"; do
  sleep 2
done
echo "✅ HDFS is now writable."

# --- Step 2: Verify Hadoop is Running ---

echo "🔍 Verifying Hadoop daemons..."
REQUIRED_PROCESSES=("NameNode" "DataNode" "ResourceManager" "NodeManager" "SecondaryNameNode")
JPS_OUTPUT=$(jps)

for PROC in "${REQUIRED_PROCESSES[@]}"; do
    if ! echo "$JPS_OUTPUT" | grep -q "$PROC"; then
        echo "❌ Missing Hadoop component: $PROC"
        echo "⚠️ Ensure Hadoop is correctly configured and try again."
        exit 1
    fi
done
echo "✅ All Hadoop services are up."

# --- Step 3: Clean/Create Local & HDFS Directories ---

cd ~
LOCAL_DIR=~/hadoop_practicals
HDFS_INPUT_DIR=/input
HDFS_OUTPUT_DIR=/output

echo "📁 Preparing local and HDFS directories..."
rm -rf "$LOCAL_DIR"
mkdir -p "$LOCAL_DIR/input" "$LOCAL_DIR/mapper" "$LOCAL_DIR/reducer"

hdfs dfs -rm -r -f "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR"
hdfs dfs -mkdir -p "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR"

# --- Step 4: Detect Hadoop JAR Paths ---

get_hadoop_root() {
    which hadoop | xargs readlink -f | xargs dirname | xargs dirname
}

get_hadoop_version_dir() {
    find "$1" -maxdepth 1 -type d -name "hadoop-*" | head -n 1
}

locate_jars() {
    HADOOP_ROOT=$(get_hadoop_root)
    VERSION_DIR=$(get_hadoop_version_dir "$HADOOP_ROOT")
    VERSION=$(basename "$VERSION_DIR" | sed 's/hadoop-//')

    MAPREDUCE_JAR="$VERSION_DIR/share/hadoop/mapreduce/hadoop-mapreduce-examples-$VERSION.jar"
    STREAMING_JAR="$VERSION_DIR/share/hadoop/tools/lib/hadoop-streaming-$VERSION.jar"

    if [[ ! -f "$MAPREDUCE_JAR" || ! -f "$STREAMING_JAR" ]]; then
        echo "❌ Hadoop JARs not found."
        exit 1
    fi

    echo "$MAPREDUCE_JAR|$STREAMING_JAR"
}

JAR_PATHS=$(locate_jars)
MAPREDUCE_JAR=$(echo "$JAR_PATHS" | cut -d'|' -f1)
STREAMING_JAR=$(echo "$JAR_PATHS" | cut -d'|' -f2)

echo "📍 MapReduce JAR: $MAPREDUCE_JAR"
echo "📍 Streaming JAR: $STREAMING_JAR"

# --- Step 5: Wordcount Job ---

echo -e "hello world\nhadoop is powerful\nhello again" > "$LOCAL_DIR/input/wordcount_input.txt"
hdfs dfs -put -f "$LOCAL_DIR/input/wordcount_input.txt" "$HDFS_INPUT_DIR/"

echo "📝 Running built-in Wordcount..."
hdfs dfs -rm -r -f /output/wordcount_output
hadoop jar "$MAPREDUCE_JAR" wordcount "$HDFS_INPUT_DIR" /output/wordcount_output

if hdfs dfs -test -e /output/wordcount_output/_SUCCESS; then
    echo "✅ Wordcount Output:"
    hdfs dfs -cat /output/wordcount_output/part-r-00000
else
    echo "❌ Wordcount failed."
    exit 1
fi

# --- Step 6: Custom Python MapReduce Job ---

echo -e "abca\nbcda" > "$LOCAL_DIR/input/mapreduce_input.txt"
hdfs dfs -put -f "$LOCAL_DIR/input/mapreduce_input.txt" "$HDFS_INPUT_DIR/"

cat << 'EOF' > "$LOCAL_DIR/mapper/mapper.py"
#!/usr/bin/env python3
import sys
for line in sys.stdin:
    for char in line.strip():
        print(f"{char}\t1")
EOF

cat << 'EOF' > "$LOCAL_DIR/reducer/reducer.py"
#!/usr/bin/env python3
import sys
from collections import defaultdict
counts = defaultdict(int)
for line in sys.stdin:
    key, val = line.strip().split("\t")
    counts[key] += int(val)
for key in sorted(counts):
    print(f"{key}\t{counts[key]}")
EOF

chmod +x "$LOCAL_DIR/mapper/mapper.py" "$LOCAL_DIR/reducer/reducer.py"

echo "⚙️ Running custom Python MapReduce job..."
hdfs dfs -rm -r -f /output/mapreduce_output1
cd "$LOCAL_DIR"

hadoop jar "$STREAMING_JAR" \
    -input "$HDFS_INPUT_DIR/mapreduce_input.txt" \
    -output /output/mapreduce_output1 \
    -mapper mapper.py \
    -reducer reducer.py \
    -file ./mapper/mapper.py \
    -file ./reducer/reducer.py

if hdfs dfs -test -e /output/mapreduce_output1/_SUCCESS; then
    echo "✅ Custom MapReduce Output:"
    hdfs dfs -cat /output/mapreduce_output1/part-00000
else
    echo "❌ Custom MapReduce failed."
    exit 1
fi

echo "🎉 ALL TASKS COMPLETED SUCCESSFULLY!"
