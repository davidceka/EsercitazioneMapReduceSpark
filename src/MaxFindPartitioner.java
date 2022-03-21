/** Esempio di Partitioner. 
*   Aggiungere le seguenti istruzioni al metodo run() della classe Driver:
*   job.setPartitionerClass(WordCountPartitioner.class);
*   job.setNumReduceTasks(2);
*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Partitioner;

public class MaxFindPartitioner extends Partitioner<Text, IntWritable> {

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		return (int) key.toString().charAt(0)-49;
	}

}
