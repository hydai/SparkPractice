#!/bin/sh
spark-submit --class ReachabilityQuery --num-executors 30 target/scala-2.10/*.jar /shared/Lab6/reachability-in/friends3.txt "LOLITA UNAVAILABLE" 5 > count.txt
hdfs dfs -getmerge Lab6/reachability answer
cat count.txt
md5sum answer
