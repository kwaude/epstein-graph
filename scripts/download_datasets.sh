#!/bin/bash
# Download DOJ Epstein datasets 1-8 and 12

cd ~/clawd/projects/epstein-graph/data/raw

BASE_URL="https://www.justice.gov/epstein/files"

# Download datasets 1-8
for i in {1..8}; do
    echo "Downloading DataSet $i..."
    curl -L -o "DataSet_$i.zip" "${BASE_URL}/DataSet%20${i}.zip" &
done

# Download dataset 12
echo "Downloading DataSet 12..."
curl -L -o "DataSet_12.zip" "${BASE_URL}/DataSet%2012.zip" &

wait
echo "All downloads complete!"
ls -lh *.zip
