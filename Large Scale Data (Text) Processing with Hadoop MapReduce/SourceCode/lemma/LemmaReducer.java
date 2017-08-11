import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LemmaReducer extends Reducer<Text, WordVO, Text, Text> {

	@Override
	protected void reduce(Text lemma, Iterable<WordVO> values,Context context)
			throws IOException, InterruptedException {
		String returnString="[";
		for (WordVO wordVO : values) {
			returnString=returnString+" < "+wordVO.getWord().toString()+" "+
					wordVO.getDetails().toString()+" >";
		}
		returnString=returnString+" ]";
		context.write(lemma, new Text(returnString));
	}

}
