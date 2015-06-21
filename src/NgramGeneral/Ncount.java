package NgramGeneral;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Job1.DecadeCount;
import Job1.NgramGroupingComparator;
import Job1.NgramPartitioner;
import Job2.FirstWordComparator;
import Job2.FirstWordCount;
import Job2.FirstWordGroupingComparator;

 
public class Ncount extends Configured implements Tool  { 
	
 
    

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    //conf.set("mapred.map.tasks","10");
	    //conf.set("mapred.reduce.tasks","10");
	    
	    final String inter = "/inter";
	    final String inter2 = "/inter2";
	    
	    Job job1 = Job.getInstance(conf, "Decade count");
	    job1.setJarByClass(Ncount.class);
	    job1.setMapperClass(DecadeCount.MapClass.class);
	    job1.setCombinerClass(DecadeCount.NgramCombiner.class);
	    job1.setReducerClass(DecadeCount.ReduceClass.class);
	    job1.setPartitionerClass(NgramPartitioner.class);
	    job1.setGroupingComparatorClass(NgramGroupingComparator.class);
	    job1.setMapOutputKeyClass(Ngram.class);
	    job1.setMapOutputValueClass(NgramValue.class);

	    job1.setOutputKeyClass(Ngram.class);
	    job1.setOutputValueClass(NgramValue.class);
	    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(inter));
	    
	    job1.waitForCompletion(true);
	    
	    System.out.println("JOB 1 completed");
	
	    Configuration conf2 = new Configuration();
	    //conf2.set("mapreduce.job.maps","10");
        //conf2.set("mapreduce.job.reduces","10");
	    
		Job job2 = Job.getInstance(conf2, "First word count");
		job2.setJarByClass(Ncount.class);
	    job2.setMapperClass(FirstWordCount.MapClass.class);
	    job2.setReducerClass(FirstWordCount.ReduceClass.class);
	    
		job2.setPartitionerClass(NgramPartitioner.class);
		job2.setSortComparatorClass(FirstWordComparator.class);
		job2.setGroupingComparatorClass(FirstWordGroupingComparator.class);
	    job2.setMapOutputKeyClass(Ngram.class);
	    job2.setMapOutputValueClass(NgramValue.class);
		// Set the outputs
		job2.setOutputKeyClass(Ngram.class);
		job2.setOutputValueClass(NgramValue.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(inter));
		FileOutputFormat.setOutputPath(job2, new Path(inter2));
		
		job2.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
	    //conf3.set("mapreduce.job.maps","10");
        //conf3.set("mapreduce.job.reduces","10");
	    
		Job job3 = Job.getInstance(conf3, "Pmi");
		job3.setJarByClass(Ncount.class);
	    job3.setMapperClass(Job3.Pmi.MapClass.class);
	    job3.setReducerClass(Job3.Pmi.ReduceClass.class);
	    
		job3.setPartitionerClass(NgramPartitioner.class);
		job3.setSortComparatorClass(Job3.SecondWordComparator.class);
		job3.setGroupingComparatorClass(Job3.SecondWordGroupingComparator.class);
	    job3.setMapOutputKeyClass(Ngram.class);
	    job3.setMapOutputValueClass(NgramValue.class);
		// Set the outputs
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(inter2));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		job3.waitForCompletion(true);
		
		return 1;
	}
	
	public static void main(String[] args) throws Exception { 
    	
		ToolRunner.run(new Configuration(), new Ncount(), args); 
    }


 
}