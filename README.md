PageRank of entire Simple Wiki is been calculated using Hadoop and MapReduce program.

View my portfolio on www.mithunjmistry.com

---------------------------------------------------------------------------------------------------------------
PageRank using Hadoop and MapReduce
---------------------------------------------------------------------------------------------------------------
Assumption - 
All java files are in current directory. "Build" directory present in current directory.
---------------------------------------------------------------------------------------------------------------
Execution instructions - 

1. Make directory in HDFS named simplewiki.
hdfs dfs -mkdir /simplewiki
We will put our 500 MB simplewiki file in this folder.
2. Put SimpleWiki in HDFS - 
hdfs dfs -put /home/cloudera/Downloads/simplewiki.xml /simplewiki
3. Compile the program of PageRank.java (We have assumed all Hadoop files and PageRank.java is present in same folder.)
javac -cp /usr/lib/hadoop/:/usr/lib/hadoop-mapreduce/ PageRank.java -d build -Xlint jar -cvf pagerank.jar -C build/ .
Alternatively, we can export jar directly using Eclipse -> Export.
4. Run the program - 
hadoop jar /home/cloudera/pagerank.jar PageRank /simplewiki /output_simplewiki
5. Get the output file to local system - 
hdfs dfs -get /output_simplewiki/part-r-00000 /desired_local_file_system_path
---------------------------------------------------------------------------------------------------------------
Note -
If loaded in eclipse and exporting project, please add the external jar files in the Eclipse path. (by right clicking project -> properties -> Build Path -> Add External Jars -> Add hadoop jars)
---------------------------------------------------------------------------------------------------------------
I have used a combiner while counting N as it will increase the performance dramatically for big data.
However, it will still run only with Mapper and Reducer. This is just performance optimization.

Number of iterations I took are 10.

After the MapReduce job is complete, all the intermediate directory and files will be automatically deleted.
----------------------------------------------------------------------------------------------------------------
