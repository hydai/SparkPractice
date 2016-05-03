#!/bin/sh
spark-submit --class WordCount --num-executors 10 target/scala-2.10/wordcount-application_2.10-1.0.jar
hdfs dfs -getmerge Lab6/wordcount answer
md5sum answer
