import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsReducer extends Reducer<Pair,IntWritable,Pair,IntWritable> {
    IntWritable sum = new IntWritable();
    @Override
    protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable val : values) {
             total= total+val.get();
        }
        sum.set(total);
        context.write(key,sum);
    }
}
