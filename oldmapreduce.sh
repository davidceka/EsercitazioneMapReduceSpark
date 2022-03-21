hdfs dfs -rm -r /output3
hadoop jar RatingReviewFilter.jar RatingReviewFilterDriver /sample_us.tsv /output3/partials
hadoop jar WordCount.jar WordCountDriver /output3/partials/part-r-00000 /output3/part1 &
hadoop jar WordCount.jar WordCountDriver /output3/partials/part-r-00001 /output3/part2 &
hadoop jar WordCount.jar WordCountDriver /output3/partials/part-r-00002 /output3/part3 &
hadoop jar WordCount.jar WordCountDriver /output3/partials/part-r-00003 /output3/part4 &
hadoop jar WordCount.jar WordCountDriver /output3/partials/part-r-00004 /output3/part5 