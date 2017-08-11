import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, ValueMapWritable, Text, ValueMapWritable> {
    private ValueMapWritable map = new ValueMapWritable();

    @Override
    protected void reduce(Text key, Iterable<ValueMapWritable> values, Context context) throws IOException, InterruptedException {
        map.clear();
        for (ValueMapWritable valueMap : values) {
            
            Set<Writable> keySet = valueMap.keySet();
            for (Writable valueMapKey : keySet) {
                IntWritable intCount = (IntWritable) valueMap.get(valueMapKey);
                if (map.containsKey(valueMapKey)) {
                    IntWritable count = (IntWritable) map.get(valueMapKey);
                    count.set(intCount.get()+count.get());
                } else {
                    map.put(valueMapKey, intCount);
                }
            }
        }
        context.write(key, map);
    }
}
