import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

	public class NgramPartitioner extends Partitioner<Ngram, NullWritable>{
	    @Override
	    public int getPartition(Ngram ngram, NullWritable nullWritable, int numPartitions) {
	        return ngram.getDecade().hashCode() % numPartitions;
	    }
	}