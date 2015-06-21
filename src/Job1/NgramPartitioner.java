package Job1;

import org.apache.hadoop.mapreduce.Partitioner;

import NgramGeneral.Ngram;
import NgramGeneral.NgramValue;

	public class NgramPartitioner extends Partitioner<Ngram, NgramValue>{
	    @Override
	    public int getPartition(Ngram ngram, NgramValue value, int numPartitions) {
	        return (ngram.getDecade().get()/10) % numPartitions;
	    }
	}