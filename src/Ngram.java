import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class Ngram implements Writable, WritableComparable<Ngram> {
	
	private IntWritable decade;
	private Text w1;
	private Text w2;
	private BooleanWritable first;
	private BooleanWritable w1First;
	
	public Ngram(){
		set(new IntWritable(0), new Text(""), new Text(""), new BooleanWritable(false));
	}
	
	public Ngram(int decade, String w1, String w2, boolean first){
		set(new IntWritable(decade), new Text(w1), new Text(w2), new BooleanWritable(first));
	}
	
	public void set(int decade, String w1, String w2, boolean first){
		this.decade.set(decade);
		this.w1.set(w1);
		this.w1.set(w2);
		this.first.set(first);
		this.w1First = new BooleanWritable(true);
	}
	
	public void set(IntWritable decade, Text w1, Text w2, BooleanWritable first){
		this.decade = decade;
		this.w1 = w1;
		this.w2 = w2;
		this.first = first;
		this.w1First = new BooleanWritable(true);
	}
	
	public void setNotFirst(){
		first.set(false);
	}
	
	public void w2First(){
		w1First.set(false);
	}
	
	public IntWritable getDecade(){
		return decade;
	}
	
	public Text getWords(){
		return new Text(w1.toString() +" "+ w2.toString());
	}
	
	public Text getW1(){
		return new Text(w1.toString());
	}
	
	public Text getW2(){
		return new Text(w2.toString());
	}
	
	public BooleanWritable getFirst(){
		return first;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		decade.write(out);
		w1.write(out);
		w2.write(out);
		first.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		decade.readFields(in);
		w1.readFields(in);
		w2.readFields(in);
		first.readFields(in);
		
	}

	@Override
	public int compareTo(Ngram o) {
		//first, sort by decades
		int result = decade.compareTo(o.getDecade());
		//second, sort by the special token first, which is designed for counting
		if (result == 0)
			result = -first.compareTo(o.getFirst());
		
		//third, sort by w1,w2 if flag w1First is on, or w2,w1 if off
		if (result == 0){
			if(w1First.get())
				result = w1.compareTo(o.getW1());
			else
				result = w2.compareTo(o.getW2());
			
			if (result == 0){
				if(w1First.get())
					result = w2.compareTo(o.getW2());
				else
					result = w1.compareTo(o.getW1());
				
			}
		}
		return result;
	}

	
	
}
