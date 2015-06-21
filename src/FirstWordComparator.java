
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class FirstWordComparator extends WritableComparator {
	
	public FirstWordComparator() {
        super(Ngram.class, true);
    }
	
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		Ngram ng1 = (Ngram)key1;
		Ngram ng2 = (Ngram)key2;
		int res = ng1.getDecade().compareTo(ng2.getDecade());
		if (res == 0)
			res = ng1.getW1().compareTo(ng2.getW1());
		if (res == 0)
			res = ng2.getFirst().compareTo(ng1.getFirst());
		return res;
	}
	
}
