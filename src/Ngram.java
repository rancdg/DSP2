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
	private Text words;
	private BooleanWritable first;
	
	public Ngram(){
		set(new IntWritable(0), new Text(""), new BooleanWritable(false));
	}
	
	public Ngram(int decade, String words, boolean first){
		set(new IntWritable(decade), new Text(words), new BooleanWritable(first));
	}
	
	public void set(int decade, String words, boolean first){
		this.decade.set(decade);
		this.words.set(words);
		this.first.set(first);
	}
	
	public void set(IntWritable decade, Text words, BooleanWritable first){
		this.decade = decade;
		this.words = words;
		this.first = first;
	}
	
	public void setNotFirst(){
		first.set(false);
	}
	
	public IntWritable getDecade(){
		return decade;
	}
	
	public Text getWords(){
		return words;
	}
	
	public BooleanWritable getFirst(){
		return first;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		decade.write(out);
		words.write(out);
		first.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		decade.readFields(in);
		words.readFields(in);
		first.readFields(in);
		
	}

	@Override
	public int compareTo(Ngram o) {
		int result = decade.compareTo(o.getDecade());
		if (result == 0)
			result = -first.compareTo(o.getFirst());
		if (result == 0)
			result = words.compareTo(o.getWords());
		return result;
	}

	
	
}
