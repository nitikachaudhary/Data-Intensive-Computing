import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Writable,WritableComparable<Pair> {

    private static final String ASTERIC = "*";
	Text word;
    Text wordNeighbor;
    
    public Pair() {
        this.word = new Text();
        this.wordNeighbor = new Text();
    }

    public Pair(Text word, Text wordNeighbor) {
        this.word = word;
        this.wordNeighbor = wordNeighbor;
    }

    public Pair(String word, String wordNeighbor) {
    	this.word = new Text(word);
    	this.wordNeighbor = new Text(wordNeighbor);
    }

    @Override
    public int compareTo(Pair o) {
    	Text oWord =o.getWord();
    	Text oNeighBor = o.getWordNeighbor();
        if(word.compareTo(oWord) != 0) return word.compareTo(oWord);
        
        
        String oNeighborString = oNeighBor.toString();
        String wordNeighborString = wordNeighbor.toString();
        if(oNeighborString.equals(ASTERIC)){
        	return 1;
        }else if(wordNeighborString.equals(ASTERIC)){
        	return -1;
        }
        
        return wordNeighbor.compareTo(oNeighBor);
    }

    public static Pair read(DataInput dataInput) throws IOException {
        Pair wordPair = new Pair();
        wordPair.readFields(dataInput);
        return wordPair;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        wordNeighbor.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        wordNeighbor.readFields(dataInput);
    }

    @Override
    public String toString() {
    	String string = "{word:"+word+"=>"+" wordNeighbor:"+wordNeighbor+"}";
    	return string;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if(o.getClass() != getClass()) return false;

        Pair pair = (Pair) o;        
        if(wordNeighbor != null){
        	if(!wordNeighbor.equals(pair.wordNeighbor)) return false;
        }else if(pair.wordNeighbor != null){
        	return false;
        }
 
        
        if(word!=null){
        	if(!word.equals(pair.word)) return false;
        }else if(pair.word != null){
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
    	if(wordNeighbor!=null) val=wordNeighbor.hashCode();
    	else val=0;
    	
    	returnVal = 163*returnVal+val;
    	return returnVal;
    	
        /*int result = word != null ? word.hashCode() : 0;
        result = 163 * result + (wordNeighbor != null ? wordNeighbor.hashCode() : 0);
        return result;*/
    }

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getWordNeighbor() {
		return wordNeighbor;
	}

	public void setWordNeighbor(Text wordNeighbor) {
		this.wordNeighbor = wordNeighbor;
	}

}