package douban;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

/**
 * @author yoseng 
 * 
 */
class Step3 {

	public static class step3Mapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines[] = value.toString().split("\t");
			String[] tmps = lines[1].split(",");
			for (int i = 0; i < tmps.length; i++) {
				String [] movieID_score = tmps[i].split(":");
				String movieID = movieID_score[0];
				String score = movieID_score[1];
				context.write(new Text(movieID),new Text(lines[0]+":"+score));
			}
		}
	}


	public static void runJob(String filePath, String outPutPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("setp3");
		job.setMapperClass(step3Mapper.class);
		job.setNumReduceTasks(0);
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

		System.out.println("Step3开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("Step3运行时间：" + (endTime - startTime) / 1000 + "秒");
	}

}
