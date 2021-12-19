package douban;

import java.io.IOException;
import java.sql.SQLException;

public class DoubanMain {

	public static void main(String[] args)
			throws ClassNotFoundException, IOException, InterruptedException, SQLException {
		String bootPath = "hdfs://192.168.184.3:8020/douban/";
		String inputPath = bootPath + "datasets";
		String out1 = bootPath + "step1";
		String out2 = bootPath + "step2";
		String out3 = bootPath + "step3";
		String out4 = bootPath + "step4";
		String out5 = bootPath + "step5";
		/*
		 * Configuration conf = new Configuration(); conf.set("fs.hdfs.impl",
		 * "org.apache.hadoop.hdfs.DistributedFileSystem");
		 */

		System.setProperty("HADOOP_USER_NAME", "root");
		Step1.runJob(inputPath, out1);
		Step2.runJob(out1, out2);
		Step3.runJob(out1, out3);
		Step4.runJob(out3, out2, out4);
		Step5.runJob(out4, out5);
		Step6.main();
	}

}
