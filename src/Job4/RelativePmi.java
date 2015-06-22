package Job4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import NgramGeneral.Ngram;
import NgramGeneral.NgramValue;

public class RelativePmi {
	
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

    
    public static class ReduceClass extends Reducer<Ngram,NgramValue,Text,DoubleWritable> {
		private Text wordOut = new Text(); 
	
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
	
			
			int count = 0;
			double relPmi = 0;
			

			final Double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
			final Double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
			
			String word = key.getDecade().toString()+"\t";
			for (NgramValue value : values) {
				if(value.isFirst()){
					count += value.getCount().get();
	
				}
				else{
					relPmi = value.getPmi().get() / (double)count;
					if (relPmi >= relMinPmi || value.getPmi().get() >= minPmi){
						wordOut.set(word + value.getWords());
						context.write(wordOut, value.getPmi());
					}
					
					
				}
			}
			
	    }
    }
}
