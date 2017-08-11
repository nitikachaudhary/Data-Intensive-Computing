import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LemmaMapper extends Mapper<LongWritable, Text, Text, WordVO> {

	Map<String, String> map;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			map = new HashMap<String,String>();
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
				map.put(key, value);
			}
			scanner.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

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

			int i;
			for (i = 0; i < size; i++) {
				if (map.containsKey(wordsArr[i])) {
					WordVO wordVO = new WordVO();
					wordVO.set(wordsArr[i], details);
					String lemma = map.get(wordsArr[i]);
					context.write(new Text(lemma), wordVO);
				}
			}
		}
	}

}
