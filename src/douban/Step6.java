package douban;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.tool.SqoopTool;
import com.mysql.jdbc.Statement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * Apache Sqoop moved into the Attic in 2021-06 HDFS->MySQL 
 * yoseng 2021-12-01
 * beta
 * 
 */

@SuppressWarnings("deprecation")
public class Step6 {

	private static Statement st = null;

	public static void main() throws ClassNotFoundException, SQLException {
		CreateMySQLTable();
		exportHDFS();
	}

	public static void CreateMySQLTable() throws ClassNotFoundException, SQLException {
		String URL = "jdbc:mysql://192.168.184.4:3306/douban?createDatabaseIfNotExist=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
		String UserName = "yoseng";
		String PassWord = "Yoseng123@";
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection(URL, UserName, PassWord);
		Boolean result = isTableExist(con, "result");
		if (result) {
			dropTable(con, "result");
		}
		st = (Statement) con.createStatement();
		// st.executeUpdate("create database douban;");
		String sql = "create table result(id int primary key not null AUTO_INCREMENT," + "userID varchar(20),"
				+ "imbd varchar(20)," + "score double(3,1));";
		st.executeUpdate(sql);
		st.close();
		con.close();
		System.out.println("表创建成功！");
	}

	public static boolean isTableExist(Connection con, String tableName) throws SQLException {
		DatabaseMetaData dm = con.getMetaData();
		ResultSet rs = dm.getTables(null, null, tableName, null);
		if (rs.next()) {
			return true;
		}
		return false;
	}

	public static void dropTable(Connection con, String tableName) throws SQLException {
		st = (Statement) con.createStatement();
		String sql = "drop table " + tableName + ";";
		st.executeUpdate(sql);
		System.out.println("删除" + tableName + "表成功!");
	}

	// MySQL -> HDFS
	public static void exportHDFS() {
		String[] arguments = new String[] { "--connect",
				"jdbc:mysql://192.168.184.4:3306/douban?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
				"--table", "result", "--username", "yoseng", "--password", "Yoseng123@", "--export-dir",
				"/douban/step5", "--input-fields-terminated-by", "\t" };
		Sqoop sqoop = new Sqoop(SqoopTool.getTool("export"), SqoopTool.loadPlugins(getSqoopConf()));
		Sqoop.runSqoop(sqoop, arguments);
		System.out.println("数据导入MySQL成功！");
	}

	public static Configuration getSqoopConf() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.184.3:8020");
		System.setProperty("HADOOP_USER_NAME", "root");
		return conf;
	}

}