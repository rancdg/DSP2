package Job2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import NgramGeneral.Ngram;
import NgramGeneral.NgramValue;



public class FirstWordCount {
	
	public static class MapClass extends Mapper<Ngram, NgramValue, Ngram, NgramValue> {
	    
	    
	    @Override
	    public void map(Ngram key, NgramValue value, Context context) throws IOException,  InterruptedException {
	    	
	    	key.setFirst();
	    	value.setFirst();
	    	context.write(key, value); 
	    	
	    	key.setNotFirst();
	    	value.setNotFirst();
	    	context.write(key, value); 
	    
	    	
	    	
	    }
	}
	    
    public static class ReduceClass extends Reducer<Ngram,NgramValue,Text,IntWritable> {
		private Text wordOut = new Text(); 
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
	
			
			int count = 0;
			
			String word = key.getDecade().toString() + "\t";
			
			for (NgramValue value : values) {
				if(value.isFirst()){
					count += value.getCount().get();
				}
				else{
					wordOut.set(word + value.getWords() + "\t" + count + "\t" + value.getNDec());
					context.write(wordOut, value.getCount());
				}
			}
			
	    }
    }
    
   
}
