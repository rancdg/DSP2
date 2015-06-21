
import org.apache.hadoop.mapreduce.Partitioner;

	public class NgramPartitioner extends Partitioner<Ngram, NgramValue>{
	    @Override
	    public int getPartition(Ngram ngram, NgramValue value, int numPartitions) {
	        return ngram.getDecade().hashCode() % numPartitions;
	    }
	}