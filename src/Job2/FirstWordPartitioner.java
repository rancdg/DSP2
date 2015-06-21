package Job2;



import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import NgramGeneral.Ngram;
import NgramGeneral.NgramValue;

	public class FirstWordPartitioner extends Partitioner<Ngram, NgramValue>{
	    @Override
	    public int getPartition(Ngram ngram, NgramValue value, int numPartitions) {
	    	Text tmp = new Text(ngram.getDecade().toString() + ngram.getW1().toString());
	    	return Math.abs(tmp.hashCode()) % numPartitions;
	    }
	}