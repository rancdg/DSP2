package Job3;

import java.io.IOException;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


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
	    
    public static class ReduceClass extends Reducer<Ngram,NgramValue,Ngram,NgramValue> {
    	private Ngram keyToSend = new Ngram();
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
	
			
			int count = 0;
			double logNdec = 0;
			
			for (NgramValue value : values) {
				if(value.isFirst()){
					count += value.getCount().get();
				}
				else{
					if (logNdec ==0)
						logNdec = Math.log(value.getNDec().get());
					double pmi = (Math.log(value.getCount().get())+ logNdec - Math.log(value.getCW1().get()) - Math.log(count));
					value.setPmi(pmi);
					String[] words = value.getWords().toString().split(" "); 
					keyToSend.set(key.getDecade().get(), words[0], words[1], value.getFirst().get(),pmi);
					context.write(keyToSend, value);
				}
			}
			
	    }
    } 

}
