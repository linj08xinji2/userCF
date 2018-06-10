package userCF.step1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce1 extends Reducer<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String itemID = key.toString();
		// <userID,score>
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (Text value : values) {
			String userID = value.toString().split("_")[0];
			String score = value.toString().split("_")[1];
			if (map.get(userID) == null) {
				map.put(userID, Integer.valueOf(score));
			} else {
				Integer prescore = map.get(userID);
				map.put(userID, prescore + Integer.valueOf(score));
			}
		}
		StringBuilder sBuilder = new StringBuilder();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			String userID = entry.getKey();
			String score = entry.getValue() + "";
			sBuilder.append(userID + "_" + score + ",");
		}
		String lineString = null;
		if (sBuilder.toString().endsWith(",")) {
			lineString = sBuilder.substring(0, sBuilder.length() - 1);
		}
		outKey.set(itemID);
		outValue.set(lineString);
		context.write(outKey, outValue);
	}

}
