
import java.io.IOException;

import java.util.StringTokenizer;
 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
 
public class Ncount { 
 
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	String[] data = value.toString().split("\\t");
	    	String ngram = data[0];
	    	int year = Integer.parseInt(data[1]);
	    	year -= (year % 10);
	    	word.set(Integer.toString(year) + " " + ngram);
	    	context.write(word, one); 
	    }
	}
 
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable> {
		@Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
	      int sum = 0;
	      for (IntWritable value : values) {
	        sum += value.get();
	      }
	      context.write(key, new IntWritable(sum)); 
	    }
	}
	
	public static class MapClass2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	String[] data = value.toString().split(" ");
	    	String ngram = data[1];
	    	String dec = data[0];
	    	word.set(dec);
	    	context.write(word, one); 
	    }
	}
 
	public static class ReduceClass2 extends Reducer<Text,IntWritable,Text,IntWritable> {
		@Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
	      int sum = 0;
	      for (IntWritable value : values) {
	        sum += value.get();
	      }
	      context.write(key, new IntWritable(sum)); 
	    }
	}
 
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
      @Override
      public int getPartition(Text key, IntWritable value, int numPartitions) {
        return getLanguage(key) % numPartitions;
      }
    
      private int getLanguage(Text key) {
         if (key.getLength() > 0) {
            int c = key.charAt(0);
            if (c >= Long.decode("0x05D0").longValue() && c <= Long.decode("0x05EA").longValue())
               return 1;
         }
         return 0;
      }
    }
 
    public static void main(String[] args) throws Exception { 
    	//Added an extra job to see if it works. The jobs so far unite decades and include redundancies 
    	//which I added to aid me explore hadoop.
	    Configuration conf = new Configuration();
	    //conf.set("mapred.map.tasks","10");
	    //conf.set("mapred.reduce.tasks","2");
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "line count");
	    job.setJarByClass(Ncount.class);
	    job.setMapperClass(MapClass.class);
	    job.setPartitionerClass(PartitionerClass.class);
	    job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("/tmp"));
	    job.waitForCompletion(true);
	    job = new Job(conf, "line count2");
	    job.setJarByClass(Ncount.class);
	    job.setMapperClass(MapClass2.class);
	    job.setPartitionerClass(PartitionerClass.class);
	    job.setCombinerClass(ReduceClass2.class);
	    job.setReducerClass(ReduceClass2.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("/tmp/part-r-00000"));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
 
}