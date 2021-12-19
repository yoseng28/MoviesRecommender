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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

/**
 * @author yoseng
 *  矩阵相乘：乘积累加
 */
class Step5 {

	public static class step5Mapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines[] = value.toString().split("\t");
			context.write(new Text(lines[0]), new Text(lines[1]));
		}
	}

	public static class step5Reducer extends Reducer<Text, Text, Text, Text> {

		private int id = 1;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<>();
			for(Text v:values) {
				String [] tokens  = v.toString().split(",");
				String movieID = tokens[0];
				Double score = Double.parseDouble(tokens[1]);
				// 累加
				if(map.containsKey(movieID)) {
					map.put(movieID, map.get(movieID)+score);
				}else {
					map.put(movieID, score);
				}
			}
			
			Iterator<String> it=map.keySet().iterator();
			while(it.hasNext()) {
				String movieID = it.next();
				Double score = map.get(movieID);
				// 为使用Sqoop导出mysql提供方便，设置导出格式，分隔符为“\t”
				context.write(new Text(String.valueOf(id)+"\t"+key.toString()), new Text(movieID+"\t"+score));
				id ++;
			}
		}
	}

	public static void runJob(String filePath,String outPutPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("setp5");
		job.setMapperClass(step5Mapper.class);
		job.setReducerClass(step5Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(filePath));
		try {
			HDFSTools.deleteFileAndDir(outPutPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));

		System.out.println("Step5开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("Step5运行时间：" + (endTime - startTime) / 1000 + "秒");
	}

}
