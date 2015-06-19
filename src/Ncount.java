
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
 
public class Ncount { 
	
	public static class MapClass extends Mapper<LongWritable, Text, Ngram, NgramValue> {
		private Ngram ngram = new Ngram();
		private NgramValue ngramValue = new NgramValue();
	    
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	String[] data = value.toString().split("\\t");
	    	String words = data[0];
	    	ngramValue.set(words, true, Integer.parseInt(data[2]) ,0);
	    	int year = Integer.parseInt(data[1]);
	    	year -= (year % 10);
	    	ngram.set(year, words, true);
	    	context.write(ngram, ngramValue); 
	    	ngram.setNotFirst();
	    	ngramValue.setNotFirst();
	    	context.write(ngram, ngramValue); 
	    	
	    }
	}
 
	public static class ReduceClass extends Reducer<Ngram,NgramValue,Text,IntWritable> {
		private Text wordOut = new Text(); 
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
	
			
			NgramValue previous = new NgramValue();
			int count = 0;
			int nDec = 0;
			String decade = key.getDecade().toString() + "\t";
			for (NgramValue value : values) {
				if (value.isFirst())
					nDec += value.getCount().get();
				else{
					if (!value.equals(previous) && count !=0){
						previous.setCount(count);
						wordOut.set(decade + previous.getWords() + "\t"+ nDec);
						context.write(wordOut, previous.getCount());
						count = 0;
					}
					count += value.getCount().get();
					previous.set(value);
					
					
				}
			}
			previous.setCount(count);
			wordOut.set(decade + previous.getWords() + "\t"+ nDec);
			context.write(wordOut, previous.getCount());
	    }
	}
	
	
	public static class NgramCombiner extends Reducer<Ngram,NgramValue,Ngram,NgramValue> {
		
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
			int count = 0;
			NgramValue previous = new NgramValue();
			for (NgramValue value : values) {
				if (!value.equals(previous) && count !=0){
					previous.setCount(count);
					context.write(key, previous);
					count = 0;
				}
				count += value.getCount().get();
				previous.set(value);
			}
			previous.setCount(count);
			context.write(key, previous);
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
	    job.setPartitionerClass(NgramPartitioner.class);
	    job.setCombinerClass(NgramCombiner.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setGroupingComparatorClass(NgramGroupingComparator.class);
	    job.setMapOutputKeyClass(Ngram.class);
	    job.setMapOutputValueClass(NgramValue.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    /*
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
	    */
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
 
}