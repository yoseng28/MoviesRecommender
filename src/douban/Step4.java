package douban;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

/**
 * @author yoseng
 * 矩阵相乘--对应位置相乘
 */
class Step4 {

	public static class step4Mapper extends Mapper<LongWritable, Text, Text, Text> {

		private String filePath;

		public void setup(Context context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			filePath = split.getPath().getParent().getName();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines[] = value.toString().split("\t");
			if (filePath.equals("step2")) { // 同现矩阵
				String[] movieID_XY = lines[0].split(":");
				// A:101,4
				context.write(new Text(movieID_XY[0]), new Text("A:" + movieID_XY[1] + "," + lines[1]));
			} else if (filePath.equals("step3")) { // 评分矩阵
				String[] userID_score = lines[1].split(":");
				// B:3,4.0
				context.write(new Text(lines[0]), new Text("B:" + userID_score[0] + "," + userID_score[1]));
			}
		}
	}

	public static class step4Reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// mapA:同现矩阵 mapB:评分矩阵
			Map<String, String> mapA = new HashMap<>();
			Map<String, String> mapB = new HashMap<>();
			for (Text v : values) {
				String val = v.toString();
				if (val.startsWith("A")) {
					String[] movieID_num = val.substring(2).split(",");
					mapA.put(movieID_num[0], movieID_num[1]);
				} else if (val.startsWith("B")) {
					String[] userID_score = val.substring(2).split(",");
					// k:用户id，v：评分
					mapB.put(userID_score[0], userID_score[1]);
				}
			}

			// A:[电影ID,出现次数] (106,2)
			// B:[用户ID,评分] (4,4.0)
			double result = 0;
			Iterator<String> it_A = mapA.keySet().iterator();
			while (it_A.hasNext()) {
				String keyA = it_A.next();
				int num = Integer.parseInt(mapA.get(keyA));
				Iterator<String> it_B = mapB.keySet().iterator();
				while (it_B.hasNext()) {
					String keyB = it_B.next();
					double score = Double.parseDouble(mapB.get(keyB));
					// 结果=同现矩阵x评分矩阵
					result = num * score;
					context.write(new Text(keyB), new Text(keyA + "," + result));
				}
			}
		}
	}

	public static void runJob(String filePath1, String filePath2, String outPutPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("setp4");
		job.setMapperClass(step4Mapper.class);
		job.setReducerClass(step4Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(filePath1), new Path(filePath2));
		try {
			HDFSTools.deleteFileAndDir(outPutPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));

		System.out.println("Step4开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("Step4运行时间：" + (endTime - startTime) / 1000 + "秒");
	}

}
