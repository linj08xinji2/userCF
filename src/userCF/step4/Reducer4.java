package userCF.step4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer4 extends Reducer<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text + ",");
		}
		String line = null;
		if (sb.toString().endsWith(",")) {
			line = sb.substring(0, sb.length() - 1);
		}
		//outKey 行 outValue 列_值, 列_值, 列_值
		outKey.set(key);
		outValue.set(line);
		context.write(outKey, outValue);
	}
	
}