
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxFindReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	Text maxKey;
	int maxValue;

	public void setup(Context context) throws IOException,InterruptedException {
		maxKey=new Text();
		maxValue=0;
	}

	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException { // i primi 2 parametri sono la coppia key-(list of values) )
		/*
		 * int sum=0;
		 * for (IntWritable value: values) {
		 * sum+=value.get(); // get() converte IntWritable in int
		 * }
		 */

		for (IntWritable value : values) {
			if (value.get() > maxValue) {
				maxValue = value.get();
				maxKey.set(key);
			}
		}
	}

	public void cleanup(Context context) throws IOException,InterruptedException{
             
            context.write(new Text(maxKey.toString().substring(1)), new IntWritable(maxValue));

			//context.write(new Text(maxKey.toString().charAt(0)-48+"\t"+maxKey.toString().substring(1)), new IntWritable(maxValue));
    }

}
