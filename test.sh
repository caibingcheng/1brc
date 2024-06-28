#!/bin/bash

samples_dir=$1
if [ ! -d "$samples_dir" ]; then
    echo "Error: Directory '$samples_dir' does not exists."
    exit 1
fi

make
passed=0
failed=0
for file in $samples_dir/*.txt; do
    echo "Processing $file"
    ./main $file
    # sample.txt output sample.out
    sample_out=${file%.txt}.out
    is_ok=$(diff $sample_out output.txt)
    if [ -z "$is_ok" ]; then
        echo "Test passed"
        passed=$((passed + 1))
    else
        echo "Test failed"
        failed=$((failed + 1))
        exit 1
    fi
done

echo "Passed: $passed"
echo "Failed: $failed"

if [ $failed -gt 0 ]; then
    exit 1
fi

./main
