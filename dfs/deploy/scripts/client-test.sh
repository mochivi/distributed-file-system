#!/bin/sh

mkdir ${TEST_FILES_DIR}

# Create a large test file for chunking tests
echo "Creating large test file..."
{   
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    for i in $(seq 1 50000); do
        printf "Line %06d: This is a test line... Timestamp: %s\n" $i "$timestamp"
    done
} > ${TEST_FILES_DIR}/test.txt

echo "Test file created: $(ls -lh ${TEST_FILES_DIR}/test.txt | awk '{print $5}')"

echo "Creating additional test files of different sizes..."

head -c 1024 /dev/urandom > ${TEST_FILES_DIR}/small_test.txt
head -c 10485760 /dev/urandom > ${TEST_FILES_DIR}/medium_test.txt
head -c 104857600 /dev/urandom > ${TEST_FILES_DIR}/large_test.txt

echo "All test files created:"
ls -lh ${TEST_FILES_DIR}/*test*.txt

echo "running tests..."
go test -v -parallel 8 -timeout=300s ./tests/integration/...