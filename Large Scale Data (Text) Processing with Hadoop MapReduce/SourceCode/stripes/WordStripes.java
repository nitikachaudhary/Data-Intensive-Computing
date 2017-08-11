import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordStripes{
  public WordStripes() {}
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (remainingArgs.length < 2) {
      System.err.println("Error in Word Stripes class");
      System.exit(2);
    }
    Job job = new Job(conf, "word stripes");
    job.setJarByClass(WordStripes.class);
    job.setMapperClass(StripesMapper.class);
    job.setCombinerClass(StripesReducer.class);
    job.setReducerClass(StripesReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ValueMapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
