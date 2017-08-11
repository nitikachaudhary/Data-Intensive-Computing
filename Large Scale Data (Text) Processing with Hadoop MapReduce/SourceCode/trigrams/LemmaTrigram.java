import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LemmaTrigram {
	public static Map<String, String> lemmaMap = new HashMap<String,String>();

	public static void main(String[] args) throws Exception {
		populateLemmaMap(lemmaMap);

		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (remainingArgs.length < 2) {
			System.err.println("Error in LemmaTrigram class");
			System.exit(2);
		}
		Job job = new Job(conf, "word trigrams");
		job.setJarByClass(LemmaTrigram.class);
		job.setMapperClass(LemmaTrigramMapper.class);
		job.setReducerClass(LemmaTrigramReducer.class);
		job.setMapOutputKeyClass(Trigram.class);
		job.setMapOutputValueClass(LemmaPairWordVO.class);
		job.setOutputKeyClass(Trigram.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class LemmaTrigramMapper extends Mapper<LongWritable, Text, Trigram, LemmaPairWordVO> {
		Trigram trigram = new Trigram();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String string = value.toString();
			String[] array = string.split(">");
			if (array.length > 1) {
				String details = array[0] + " >";
				String words = array[1];
				words = words.replaceAll("[^A-Za-z\\s+]", "");
				String[] wordsArr = words.split("\\s+");

				int size = wordsArr.length;

				int i, j, k;
				for (i = 0; i < size - 2; i++) {
					if (lemmaMap.containsKey(wordsArr[i]))
						trigram.setWord(new Text(lemmaMap.get(wordsArr[i])));
					else
						trigram.setWord(new Text(wordsArr[i]));
					for (j = i + 1; j < size-1; j++) {
						if (lemmaMap.containsKey(wordsArr[j]))
							trigram.setNeighbor1(new Text(lemmaMap.get(wordsArr[j])));
						else
							trigram.setNeighbor1(new Text(wordsArr[j]));
						for(k =i+2; k<size;k++){
							if (lemmaMap.containsKey(wordsArr[k]))
								trigram.setNeighbor2(new Text(lemmaMap.get(wordsArr[k])));
							else
								trigram.setNeighbor2(new Text(wordsArr[j]));
						LemmaPairWordVO lemmaPairWordVO = new LemmaPairWordVO();
						lemmaPairWordVO.setDetails(details);
						context.write(trigram, lemmaPairWordVO);
						}
					}
				}
			}
		}
	}

	public static class LemmaTrigramReducer extends Reducer<Trigram, LemmaPairWordVO, Trigram, Text> {
		@Override
		protected void reduce(Trigram trigram, Iterable<LemmaPairWordVO> values, Context context)
				throws IOException, InterruptedException {
			String returnString = "[";
			int count = 0;
			for (LemmaPairWordVO wordVO : values) {
				count++;
				returnString = returnString + wordVO.getDetails().toString() + " ,";
			}
			returnString = count + " " + returnString + " ]";
			context.write(trigram, new Text(returnString));
		}

	}

	private static void populateLemmaMap(Map<String, String> lemmaMap) {
		try {
			String filePath = "new_lemmatizer.csv";
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = null;
			Scanner scanner = null;
			int index = 0;
			String key = null, value = null;
			while ((line = reader.readLine()) != null) {
				index = 0;
				scanner = new Scanner(line);
				scanner.useDelimiter(",");
				while (scanner.hasNext()) {
					if (index == 0) {
						key = scanner.next();
					} else if (index == 1) {
						value = scanner.next();
					} else
						scanner.next();
					index++;
				}
				lemmaMap.put(key, value);
			}
			scanner.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
