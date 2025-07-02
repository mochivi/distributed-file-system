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

head -c 1024 /dev/urandom > ${TEST_FILES_DIR}/small_test.txt      # 1KB
head -c 10485760 /dev/urandom > ${TEST_FILES_DIR}/medium_test.txt # 10MB
head -c 104857600 /dev/urandom > ${TEST_FILES_DIR}/large_test.txt # 100MB
head -c 1048576000 /dev/urandom > ${TEST_FILES_DIR}/xlarge_test.txt # 1GB

echo "All test files created:"
ls -lh ${TEST_FILES_DIR}/*test*.txt

echo "running tests..."

if [ "$DEBUG" = "true" ]; then
    echo "Running in debug mode - waiting for debugger connection on port 2345"
    # Run specific test with debugger
    if [ -n "$TEST_NAME" ]; then
        dlv test --headless --listen=:2345 --api-version=2 --accept-multiclient --log -- -test.run="$TEST_NAME" ./tests/e2e/... 
    else
        # Run all tests with debugger
        dlv test --headless --listen=:2345 --api-version=2 --accept-multiclient --log ./tests/e2e/...
    fi
else
    echo "Running in normal mode"
    # Your original test logic here
    # Replace this with the contents of your original client-test.sh
    go test -v -parallel 8 -timeout=300s -cover -coverprofile=/app/coverage/e2e-coverage.out -covermode=atomic ./tests/e2e/...
fi