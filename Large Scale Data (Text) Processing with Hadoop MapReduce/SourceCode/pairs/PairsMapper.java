import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairsMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
    
    IntWritable intWritableOne = new IntWritable(1);    
    Pair pair = new Pair();
    

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String string = value.toString();
        String[] words = string.split("\\s+");
        int size=words.length;
        int i,j;
        for(i=0;i<size-1;i++){
        	pair.setWord(new Text(words[i]));
        	for(j=i+1;j<size;j++){
        		pair.setWordNeighbor(new Text(words[j]));
        		context.write(pair, intWritableOne);
        	}
        }
        
  }
}