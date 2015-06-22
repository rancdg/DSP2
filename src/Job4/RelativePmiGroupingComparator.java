package Job4;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import NgramGeneral.Ngram;

public class RelativePmiGroupingComparator extends WritableComparator {
	    public RelativePmiGroupingComparator() {
	        super(Ngram.class, true);
	    }
	    @Override
	    public int compare(WritableComparable tp1, WritableComparable tp2) {
	        Ngram ngram = (Ngram) tp1;
	        Ngram ngram2 = (Ngram) tp2;
	        int res = ngram.getDecade().compareTo(ngram2.getDecade());
	        return res;
	    }
}