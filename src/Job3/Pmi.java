package Job3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import NgramGeneral.Ngram;
import NgramGeneral.NgramValue;

public class Pmi {
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
		private DoubleWritable pmi1 = new DoubleWritable();
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
	
			
			int count = 0;
			double logNdec = 0;
			
			String word = key.getDecade().toString() + "\t";
			for (NgramValue value : values) {
				if(value.isFirst()){
					count += value.getCount().get();
				}
				else{
					if (logNdec ==0)
						logNdec = Math.log(value.getNDec().get());
					double pmi = (Math.log(value.getCount().get())+ logNdec - Math.log(value.getCW1().get()) - Math.log(count));
					wordOut.set(word + value.getWords());
					pmi1.set(pmi);
					context.write(wordOut, pmi1);
				}
			}
			
	    }
    }
}
