import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends Mapper<LongWritable,Text,Text,ValueMapWritable> {
    private ValueMapWritable map = new ValueMapWritable();
    private Text wordText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
        String string = value.toString();
        String[] words = string.split("\\s+");        
        int size=words.length;       
        int i,j;
        if(size>1){
            for(i=0;i<size-1;i++){
            	wordText.set(new Text(words[i]));
                map.clear(); 
            	for(j=i+1;j<size;j++){
                		Text text = new Text(words[j]);
                        if(map.containsKey(text)){
                           IntWritable count = (IntWritable)map.get(text);
                           count.set(count.get()+1);
                        }else{
                            map.put(text,new IntWritable(1));
                        }
                }
                context.write(wordText,map);
            }
            }
    }
}