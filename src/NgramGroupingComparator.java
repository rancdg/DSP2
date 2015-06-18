import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NgramGroupingComparator extends WritableComparator {
	    public NgramGroupingComparator() {
	        super(Ngram.class, true);
	    }
	    @Override
	    public int compare(WritableComparable tp1, WritableComparable tp2) {
	        Ngram ngram = (Ngram) tp1;
	        Ngram ngram2 = (Ngram) tp2;
	        return ngram.getDecade().compareTo(ngram2.getDecade());
	    }
	}