import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WordVO implements Writable {
	
	Text word;
	Text details;
	
	public WordVO(){
		this.word= new Text();
		this.details = new Text();
	}
	
	public void set(String word,String details){
		this.word.set(word);
		this.details.set(details);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		word.readFields(dataInput);
        details.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		word.write(dataOutput);
        details.write(dataOutput);		
	}

	public Text getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word.set(word);
	}

	public Text getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details.set(details);
	}

}