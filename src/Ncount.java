

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 
public class Ncount { 
	
 
    public static void main(String[] args) throws Exception { 
    	
	    Configuration conf = new Configuration();
	    //conf.set("mapred.map.tasks","10");
	    //conf.set("mapred.reduce.tasks","2");
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "line count");
	    job.setJarByClass(Ncount.class);
	    job.setMapperClass(DecadeCount.MapClass.class);
	    job.setPartitionerClass(NgramPartitioner.class);
	    job.setCombinerClass(DecadeCount.NgramCombiner.class);
	    job.setReducerClass(DecadeCount.ReduceClass.class);
	    job.setGroupingComparatorClass(NgramGroupingComparator.class);
	    job.setMapOutputKeyClass(Ngram.class);
	    job.setMapOutputValueClass(NgramValue.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
 
}