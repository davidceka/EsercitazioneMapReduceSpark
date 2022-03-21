/** Esempio di Partitioner. 
*   Aggiungere le seguenti istruzioni al metodo run() della classe Driver:
*   job.setPartitionerClass(WordCountPartitioner.class);
*   job.setNumReduceTasks(2);
*/

import org.apache.hadoop.io.Text;

import java.util.Random;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

	

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		
		Random rand = new Random(); //instance of random class
    	int upperbound = 12;
		return rand.nextInt(upperbound);

	}

}
