package Job1;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import NgramGeneral.Ngram;
import NgramGeneral.NgramValue;




public class DecadeCount {
	
	public static class MapClass extends Mapper<LongWritable, Text, Ngram, NgramValue> {
		
		private Ngram ngram = new Ngram();
		private NgramValue ngramValue = new NgramValue();
		final private String pathEng = "StopEnglish";
		final private String pathHeb = "StopHebrew";
		private StopWords stop = null;
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Ngram, NgramValue>.Context context)throws IOException, InterruptedException {
			boolean includeStop = context.getConfiguration().getBoolean("stop", true);
			String path = (context.getConfiguration().get("language", "eng") == "heb"? pathHeb : pathEng);
			if (!includeStop){
				
				stop = new StopWords(path);
				stop.init();
			}
		}

	
	    
		
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	
	    	
	    	String[] data = value.toString().split("\\t");
	    	String[] words = data[0].split(" ");
	    	
	    	if(stop == null || (!stop.isStop(words[0]) && !stop.isStop(words[1]))){
		    	ngramValue.set(words[0]+ " " + words[1], true, Integer.parseInt(data[2]), 0, 0, 0);
		    	int year = Integer.parseInt(data[1]);
		    	year -= (year % 10);
		    	ngram.set(year, words[0], words[1], true, 0);
		    	context.write(ngram, ngramValue); 
		    	ngram.setNotFirst();
		    	ngramValue.setNotFirst();
		    	context.write(ngram, ngramValue); 
	    	}
	    	
	    }
	}

	
	public static class ReduceClass extends Reducer<Ngram,NgramValue,Ngram,NgramValue> {
		private Ngram keyToSend = new Ngram();
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
	
			
			NgramValue previous = new NgramValue();
			int count = 0;
			int nDec = 0;
			for (NgramValue value : values) {
				if (value.isFirst())
					nDec += value.getCount().get();
				else{
					if (!value.equals(previous) && count !=0){
						previous.setCount(count);
						previous.setNdec(nDec);
						String[] words = previous.getWords().toString().split(" "); 
						keyToSend.set(key.getDecade().get(), words[0], words[1], previous.getFirst().get(), 0);
						context.write(keyToSend, previous);
						count = 0;
					}
					count += value.getCount().get();
					previous.set(value);
					
					
				}
			}
			previous.setCount(count);
			previous.setNdec(nDec);
			String[] words = previous.getWords().toString().split(" "); 
			keyToSend.set(key.getDecade().get(), words[0], words[1], previous.getFirst().get(), 0);
			context.write(keyToSend, previous);
	    }
	}
	
	
	public static class NgramCombiner extends Reducer<Ngram,NgramValue,Ngram,NgramValue> {
		private Ngram keyToSend = new Ngram();
		@Override
	    public void reduce(Ngram key, Iterable<NgramValue> values, Context context) throws IOException,  InterruptedException {
			int count = 0;
			NgramValue previous = new NgramValue();
			for (NgramValue value : values) {
				if (!value.equals(previous) && count !=0){
					previous.setCount(count);
					String[] words = previous.getWords().toString().split(" "); 
					keyToSend.set(key.getDecade().get(), words[0], words[1], previous.getFirst().get(), 0);
					context.write(keyToSend, previous);
					count = 0;
				}
				count += value.getCount().get();
				previous.set(value);
			}
			previous.setCount(count);
			String[] words = previous.getWords().toString().split(" "); 
			keyToSend.set(key.getDecade().get(), words[0], words[1], previous.getFirst().get(), 0);
			context.write(keyToSend, previous);
	    }
	}
}
