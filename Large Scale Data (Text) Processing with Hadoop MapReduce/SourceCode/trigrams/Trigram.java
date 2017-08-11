import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Trigram implements Writable,WritableComparable<Trigram> {

    private static final String ASTERIC = "*";
	Text word;
    Text neighbor1;
    Text neighbor2;
    
    public Trigram() {
        this.word = new Text();
        this.neighbor1= new Text();
        this.neighbor2= new Text();
    }

    public Trigram(Text word, Text neighbor1, Text neighbor2) {
        this.word = word;
        this.neighbor1 = neighbor1;
        this.neighbor1 = neighbor2;
    }

    public Trigram(String word, String wordNeighbor) {
    	this.word = new Text(word);
    	this.neighbor1 = new Text(neighbor1);
    	this.neighbor2 = new Text(neighbor2);
    }

    @Override
    public int compareTo(Trigram o) {
		Text oWord = o.getWord();
		Text oNeighbor1 = o.getNeighbor1();
		Text oNeighbor2 = o.getNeighbor2();
		if (word.compareTo(oWord) != 0)
			return word.compareTo(oWord);

		int val;
		String oNeighbor1String = oNeighbor1.toString();
		String thisNeighbor1 = neighbor1.toString();
		if (oNeighbor1String.equals(ASTERIC)) {
			return 1;
		} else if (thisNeighbor1.equals(ASTERIC)) {
			return -1;
		} else {
			val = neighbor1.compareTo(oNeighbor1);
		}

		if (val == 0) {
			String oNeighbor2String = oNeighbor2.toString();
			String thisNeighbor2 = neighbor2.toString();
			if (oNeighbor2String.equals(ASTERIC)) {
				return 1;
			} else if (thisNeighbor2.equals(ASTERIC)) {
				return -1;
			}
		}		
		return neighbor2.compareTo(oNeighbor2);
    }

    public static Trigram read(DataInput dataInput) throws IOException {
        Trigram trigram = new Trigram();
        trigram.readFields(dataInput);
        return trigram;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        neighbor1.write(dataOutput);
        neighbor2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        neighbor1.readFields(dataInput);
        neighbor2.readFields(dataInput);
    }

    @Override
    public String toString() {
    	String string = "{word:"+word+"=>"+" neighbor1:"+neighbor1+" neighbor2:"+neighbor2+"}";
    	return string;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if(o.getClass() != getClass()) return false;

        Trigram trigram = (Trigram) o;        
        if(neighbor1 != null){
        	if(!neighbor1.equals(trigram.neighbor1)) return false;
        }else if(trigram.neighbor1 != null){
        	return false;
        }
        
        if(neighbor2 != null){
        	if(!neighbor2.equals(trigram.neighbor2)) return false;
        }else if(trigram.neighbor2 != null){
        	return false;
        }
 
        
        if(word!=null){
        	if(!word.equals(trigram.word)) return false;
        }else if(trigram.word != null){
        	return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
    	int returnVal;
    	if(word!=null) returnVal=word.hashCode();
    	else returnVal = 0;
    	
    	int val;
    	if(neighbor1!=null) val=neighbor1.hashCode();
    	else val=0;
    	
    	int val2;
    	if(neighbor2!=null) val2=neighbor2.hashCode();
    	else val2=0;
    	
    	returnVal = 163*returnVal+val+val2;
    	return returnVal;
    
    }

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getNeighbor1() {
		return neighbor1;
	}

	public void setNeighbor1(Text neighbor1) {
		this.neighbor1 = neighbor1;
	}

	public Text getNeighbor2() {
		return neighbor2;
	}

	public void setNeighbor2(Text neighbor2) {
		this.neighbor2 = neighbor2;
	}

}