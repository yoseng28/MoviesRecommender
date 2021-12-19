package douban;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

/**
 * 
 * @author yoseng
 * 计算用户对电影的评分矩阵
 */
class Step1 {

	public static class step1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines [] = value.toString().split(",");
			context.write(new Text(lines[0]), new Text(lines[1] + ":" + lines[2]));
		}
	}

	public static class step1Reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text v : values) {
				sb.append("," + v.toString());
			}
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));
		}
	}

	public static void runJob(String filePath, String outPutPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("setp1");
		job.setMapperClass(step1Mapper.class);
		job.setCombinerClass(step1Reducer.class);
		job.setReducerClass(step1Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(filePath));
		try {
			HDFSTools.deleteFileAndDir(outPutPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));

		System.out.println("Step1开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("Step1运行时间：" + (endTime - startTime) / 1000 + "秒");
	}

}
