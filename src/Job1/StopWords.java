package Job1;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class StopWords {
	
	private String path;
	private HashMap<String, Integer> hm; 

	public StopWords(String path){
		this.path = path;
		this.hm = new HashMap<String, Integer>();
	}
	
	public void init() {
		try(BufferedReader br = new BufferedReader(new FileReader(path))){
			String sCurrentLine;
			 
			while ((sCurrentLine = br.readLine()) != null) {
				hm.put(sCurrentLine, 1);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public boolean isStop(String word){
		return hm.containsKey(word);
	}
}
