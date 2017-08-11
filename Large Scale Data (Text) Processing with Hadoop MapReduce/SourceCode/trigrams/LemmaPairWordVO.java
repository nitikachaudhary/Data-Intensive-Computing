import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LemmaPairWordVO implements Writable {
	
	Text details;
	
	public LemmaPairWordVO(){
		this.details = new Text();
	}
	
	public void set(String word,String details){
		this.details.set(details);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
        details.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
        details.write(dataOutput);		
	}


	public Text getDetails() {
		return details;
	}

	public void setDetails(String details) {
		this.details.set(details);
	}

}
