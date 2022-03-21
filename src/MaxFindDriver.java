

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//todo controllare stopwords e spostare partitioner

public class MaxFindDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode =ToolRunner.run(new MaxFindDriver(), args);
		System.exit(exitCode); // se l'output è 0 allora il programma ha terminato correttamente, sennò ci sono stati errori
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Invalid arguments!\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}

		String inputDir = args[0]; // path dell'input directory in HDFS
		String outputDir = args[1]; // path dell'output directory in HDFS

		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Job Name: Rating Review Filter");
		job.setJarByClass(MaxFindDriver.class); // Indico la classe che costituirà l'entry point del job

		

		job.setMapperClass(MaxFindMapper.class);
		job.setReducerClass(MaxFindReducer.class);

		// #TODO: inserire 12 per la consegna
		job.setPartitionerClass(MaxFindPartitioner.class);
		job.setNumReduceTasks(5);

		job.setOutputKeyClass(Text.class); // la classe rappresentante il data type dell'output key
		job.setOutputValueClass(IntWritable.class); // la classe rappresentante il data type dell'output value

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		boolean success = job.waitForCompletion(true); // success vale true se il job termina correttamente, false altrimenti
		if (!success) {
			throw new IllegalStateException("Job Word Count failed!");		
		}
		return 0;
	
	}

	

}
