#!/bin/bash

start_time=$(date +%s)

for i in {1..2}
do

#  curl -v -d '@event1.json' -H "Content-Type:application/json" POST http://localhost:8080/event
 curl -v -d '@event.json' -H "Content-Type:application/json" POST http://localhost:8080/event


 curl -v -d '@event2.json' -H "Content-Type:application/json" POST http://localhost:8080/event
done

end_time=$(date +%s)

time_diff=$((end_time - start_time))

echo "Time taken to execute commands: $time_diff seconds"

