
public class maxmin {

    public static class maxminmapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Text t1 = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
    String[] colvalue = value.toString().split(“,”);
    
    for (int i = 0; i < colvalue.length; i++) {
    
    t1.set(String.valueOf(i + 1));
    
    context.write(t1, new DoubleWritable(Double.parseDouble(colvalue[i])));
    
    } }
    }

    public static class maxminReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double min = Integer.MAX_VALUE, max = 0;

            Iterator<DoubleWritable> iterator = values.iterator(); // Iterating

            while (iterator.hasNext()) {

                double value = iterator.next().get();

                if (value < min) { // Finding min value

                    min = value;

                }

                if (value > max) { // Finding max value

                    max = value;

                }
            }

            context.write(new Text(key), new DoubleWritable(min));

            context.write(new Text(key), new DoubleWritable(max));

        }
    }

    public static void main(String[] args) throws Exception {
    
    Path inputPath = new Path(“hdfs://localhost:54310/home/sortinput”);
    
    Path outputDir =new Path(“hdfs://localhost:54310/home/MaxMinOutput1”);
    
    Configuration conf = new Configuration();
    
    Job job = new Job(conf, “Find Minimum and Maximum”);
    
    job.setJarByClass(maxmin.class);
    
    FileSystem fs = FileSystem.get(conf);
    
    job.setOutputKeyClass(Text.class);
    
    job.setOutputValueClass(DoubleWritable.class);
    
    job.setMapperClass(maxminmapper.class);
    
    job.setReducerClass(maxminReducer.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    
    job.setOutputFormatClass(TextOutputFormat.class);
    
    FileInputFormat.addInputPath(job, inputPath);
    
    FileOutputFormat.setOutputPath(job, outputDir);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    }

}
