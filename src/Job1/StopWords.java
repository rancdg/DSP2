package Job1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StopWords {
	
	private String path;
	private HashMap<String, Integer> hm; 

	public StopWords(String path){
		this.path = path;
		this.hm = new HashMap<String, Integer>();
	}
	
	public void init() throws IOException{
		String line;
		Path pt=new Path(path);//Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        line=br.readLine();
        while (line != null){
        	hm.put(line, 1);
            line=br.readLine();
        }
		
	}
	
	public boolean isStop(String word){
		return hm.containsKey(word);
	}
}
