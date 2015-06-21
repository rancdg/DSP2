package Job3;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import NgramGeneral.Ngram;

public class SecondWordGroupingComparator extends WritableComparator {
	    public SecondWordGroupingComparator() {
	        super(Ngram.class, true);
	    }
	    @Override
	    public int compare(WritableComparable tp1, WritableComparable tp2) {
	        Ngram ngram = (Ngram) tp1;
	        Ngram ngram2 = (Ngram) tp2;
	        int res = ngram.getDecade().compareTo(ngram2.getDecade());
	        if (res == 0)
	        	res = ngram.getW2().compareTo(ngram2.getW2());
	        return res;
	    }
}