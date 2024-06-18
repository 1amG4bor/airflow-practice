#!/usr/bin/env bash
echo "Task started.."

for i in {1..10}
do
    echo "..processing (${i}s)"
    sleep 0.1
done

echo "Task finished!"
