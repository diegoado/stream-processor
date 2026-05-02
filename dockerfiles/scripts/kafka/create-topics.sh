#!/bin/sh

while read -r topicName;
do
  echo "Trying to create topic: $topicName"
  kafka-topics --create --topic "$topicName" --bootstrap-server broker:29092 --partitions 1 --replication-factor 1 --if-not-exists;
done < "/tmp/topics.txt"
