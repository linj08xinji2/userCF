package userCF.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	/**
	 * key :1 2 3 
	 * value: A,1,1 C,3,5
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String userID = values[0];
		String itemID = values[1];
		String sorce = values[2];
		// 以下key value的位置与物品推荐算法不一样
		outKey.set(userID);
		outValue.set(itemID + "_" + sorce);
		context.write(outKey, outValue);
	}
}
