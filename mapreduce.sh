hdfs dfs -rm -r /output3
hadoop jar WordCount.jar WordCountDriver /amazon_reviews_us_Video_Games_v1_00.tsv /output3/partials
hadoop jar MaxFind.jar MaxFindDriver /output3/partials/part-r-00000 /output3/finali
hdfs dfs -cat /output3/finali/part-r-00000
hdfs dfs -cat /output3/finali/part-r-00001
hdfs dfs -cat /output3/finali/part-r-00002
hdfs dfs -cat /output3/finali/part-r-00003
hdfs dfs -cat /output3/finali/part-r-00004