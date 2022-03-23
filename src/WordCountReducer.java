

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { //i primi 2 parametri sono la coppia key-(list of values) )
		int sum=0;
		for (IntWritable value: values) {
			sum+=value.get();	// get() converte IntWritable in int
		}
		context.write(key,new IntWritable(sum));
	}	


}
