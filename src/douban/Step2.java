package douban;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

/**
 * @author yoseng 
 * 建立电影同现矩阵
 * 
 */
class Step2 {

	public static class step2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines[] = value.toString().split("\t");
			String[] tmps = lines[1].split(",");
			for (int i = 0; i < tmps.length; i++) {
				String movieID = tmps[i].split(":")[0];
				for (int j = 0; j < tmps.length; j++) {
					String movieID_ = tmps[j].split(":")[0];
					context.write(new Text(movieID + ":" + movieID_), new IntWritable(1));
				}
			}
		}
	}

	public static class step2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum = sum + v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void runJob(String filePath, String outPutPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("setp2");
		job.setMapperClass(step2Mapper.class);
		job.setCombinerClass(step2Reducer.class);
		job.setReducerClass(step2Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(filePath));
		try {
			HDFSTools.deleteFileAndDir(outPutPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));

		System.out.println("Step2开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("Step2运行时间：" + (endTime - startTime) / 1000 + "秒");
	}

}
