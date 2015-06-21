package Job2;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import NgramGeneral.Ngram;

public class FirstWordGroupingComparator extends WritableComparator {
	    public FirstWordGroupingComparator() {
	        super(Ngram.class, true);
	    }
	    @Override
	    public int compare(WritableComparable tp1, WritableComparable tp2) {
	        Ngram ngram = (Ngram) tp1;
	        Ngram ngram2 = (Ngram) tp2;
	        int res = ngram.getDecade().compareTo(ngram2.getDecade());
	        if (res == 0)
	        	res = ngram.getW1().compareTo(ngram2.getW1());
	        return res;
	    }
}