package NgramGeneral;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class NgramValue implements Writable, WritableComparable<NgramValue> {
	
	private Text words;
	private BooleanWritable first;
	private IntWritable count;
	private IntWritable nDec;
	private IntWritable cW1;
	
	public NgramValue(){
		set(new Text(""), new BooleanWritable(false), new IntWritable(0), new IntWritable(0), new IntWritable(0));
	}

	public NgramValue(String words, boolean first, int count, int nDec){
		set(new Text(words), new BooleanWritable(first), new IntWritable(count), new IntWritable(nDec), new IntWritable(0));
	}
	
	public NgramValue(NgramValue n){
		set(new Text(n.getWords().toString()), new BooleanWritable(n.getFirst().get()), new IntWritable(n.getCount().get()), new IntWritable(n.getNDec().get()), new IntWritable(n.getCW1().get()));
	}
	
	public void set(String words, boolean first, int count, int nDec, int cW1){
		this.words.set(words);
		this.first.set(first);
		this.count.set(count);
		this.nDec.set(nDec);
		this.cW1.set(cW1);
	}
	
	public void set(Text words, BooleanWritable first, IntWritable count, IntWritable nDec, IntWritable cW1){
		this.words = words;
		this.first = first;
		this.count = count;
		this.nDec = nDec;
		this.cW1 = cW1;
	}
	
	
	public void set(NgramValue n){
		set(n.getWords().toString(), n.getFirst().get(), n.getCount().get(), n.getNDec().get(), n.getCW1().get());
	}
	
	//debug setter
	public void setWords(String words){
		this.words.set(words);
	}

	public void setNotFirst(){
		first.set(false);
	}
	
	public void setFirst(){
		first.set(true);
	}
	
	public void setCW1(int cW1){
		this.cW1.set(cW1);
	}
	

	
	public void setNdec(int nDec){
		this.nDec.set(nDec);
	}
	
	public Text getWords(){
		return words;
	}
	
	public IntWritable getCount(){
		return count;
	}
	
	public boolean isFirst(){
		return first.get();
	}
	
	public IntWritable getCW1(){
		return cW1;
	}
	
	
	public BooleanWritable getFirst(){
		return first;
	}
	
	public void setCount(int count){
		this.count.set(count);
	}
	
	public IntWritable getNDec(){
		return nDec;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		words.write(out);
		first.write(out);
		count.write(out);
		nDec.write(out);
		cW1.write(out);
	
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		words.readFields(in);
		first.readFields(in);
		count.readFields(in);
		nDec.readFields(in);
		cW1.readFields(in);
	}
	
	@Override
	public int compareTo(NgramValue o) {
		return words.compareTo(o.getWords());
	}
	
	@Override
	public boolean equals(Object obj) {
		NgramValue o = (NgramValue)obj;
		return (words.equals(o.getWords()) && (first.equals(o.getFirst())));
	}


	

	
	
}
