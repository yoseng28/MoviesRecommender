package douban;

import java.io.IOException;
import java.net.URISyntaxException;

import tools.HDFSTools;


public class Test {
	
	public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException {
		// 上传数据至HDFS
		HDFSTools.uploadFile("D:\\workspace\\eclipse_workspace\\MoviesRecommender\\src\\datasets\\douban.txt","/douban/datasets");
	}

}
