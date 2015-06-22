package Job4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import NgramGeneral.Ngram;


public class RelativePmiComparator extends WritableComparator {
	
	public RelativePmiComparator() {
        super(Ngram.class, true);
    }
	
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		Ngram ng1 = (Ngram)key1;
		Ngram ng2 = (Ngram)key2;
		int res = ng1.getDecade().compareTo(ng2.getDecade());
		if (res == 0)
			res = ng2.getFirst().compareTo(ng1.getFirst());
		if (res == 0)
			res = ng2.getPmi().compareTo(ng1.getPmi());
		
		return res;
	}
	
}