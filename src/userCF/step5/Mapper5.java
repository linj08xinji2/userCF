package userCF.step5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author linj
 * 
 */
public class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	private List<String> cacheList = new ArrayList<>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		// 通过输入流将全局缓存中的右侧矩阵 读入List<String>中
		FileReader fr = new FileReader("UseritemScore3");
		BufferedReader br = new BufferedReader(fr);
		// 每一行的格式是： 行 tab 列_值,列_值,列_值,列_值
		String line = null;
		while ((line = br.readLine()) != null) {
			cacheList.add(line);
		}
		fr.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String item_matrix1 = value.toString().split("\t")[0];
		String[] user_score_array_matrix1 = value.toString().split("\t")[1]
				.split(",");

		for (String line : cacheList) {
			String item_matrix2 = line.toString().split("\t")[0];
			String[] user_score_array_matrix2 = line.toString().split("\t")[1]
					.split(",");
			// 如果物品ID物品相同
			if (item_matrix1.equals(item_matrix2)) {
				// 遍历matrix1的列
				for (String user_score_matrix1 : user_score_array_matrix1) {
					boolean flag = false;
					String user_matrix1 = user_score_matrix1.split("_")[0];
					String score_matrix1 = user_score_matrix1.split("_")[1];
					// 遍历matrix2的列
					for (String user_score_matrix2 : user_score_array_matrix2) {
						String user_matrix2 = user_score_matrix2.split("_")[0];
						if (user_matrix1.equals(user_matrix2)) {
							flag = true;
						}
					}
				// 以下key value的位置与物品推荐算法不一样
					if (flag == false) {
						outKey.set(item_matrix1);
						outValue.set(user_matrix1+ "_" + score_matrix1);
						context.write(outKey, outValue);
					}
				}

			}

		}

	}

}
