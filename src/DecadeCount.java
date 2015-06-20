import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;




public class DecadeCount {
	
	public static class MapClass extends Mapper<LongWritable, Text, Ngram, NgramValue> {
		private Ngram ngram = new Ngram();
		private NgramValue ngramValue = new NgramValue();
	    
	    
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	String[] data = value.toString().split("\\t");
	    	String[] words = data[0].split(" ");
	    	ngramValue.set(words[0]+ " " + words[1], true, Integer.parseInt(data[2]) ,0);
	    	int year = Integer.parseInt(data[1]);
	    	year -= (year % 10);
	    	ngram.set(year, words[0], words[1], true);
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
}
