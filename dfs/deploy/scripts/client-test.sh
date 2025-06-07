#!/bin/sh

# Create a large test file for chunking tests
echo "Creating large test file..."

mkdir test-files
cd test-files

{   
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    for i in $(seq 1 50000); do
        printf "Line %06d: This is a test line... Timestamp: %s\n" $i "$timestamp"
    done
} > test.txt

echo "Test file created: $(ls -lh test.txt | awk '{print $5}')"

echo "Creating additional test files of different sizes..."

# Small file (1KB)
head -c 1024 /dev/urandom > small_test.txt

# Medium file (10MB) 
head -c 10485760 /dev/urandom > medium_test.txt

# Large file (100MB)
head -c 104857600 /dev/urandom > large_test.txt

echo "All test files created:"
ls -lh *test*.txt

cd ..
echo "running tests..."
go test -v -timeout=300s ./tests/integration/...