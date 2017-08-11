import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class ValueMapWritable extends MapWritable{
	   @Override
	   public String toString(){
	      String finalString = "[";
	      
	      for(Writable key : this.keySet()){
	         IntWritable sum = (IntWritable) this.get(key);
	         finalString =finalString +" "+key.toString()+"->"+sum.toString()+",";
	      }

	      finalString = finalString+" ]";
	      return finalString;
	   }
	}
