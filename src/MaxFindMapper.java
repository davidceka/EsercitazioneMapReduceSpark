

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxFindMapper extends Mapper<LongWritable, Text, Text, IntWritable> { // i primi 2 data types si riferiscono alla coppia key-value in input, gli altri 2 alla coppia in output


	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] riga = value.toString().split("\t"); // converto in String perchè non lavoriamo con i Text
		
		context.write(new Text(riga[0]), new IntWritable(Integer.parseInt(riga[1])));
	}; 
}





