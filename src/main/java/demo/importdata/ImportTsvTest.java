package demo.importdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.util.ToolRunner;

import demo.MyConnection;

public class ImportTsvTest {
	// You must change this location
	static Path path = new Path("file:///D:/workspace/hdp/v1/src/main/java/data.txt");
	public static void main(String[] args) throws Exception {
		// first copy local data to hdfs
		upload();
		// 新建表
		createTable();
		
		//  run importtsv
		runImport();
		
		// use hbase shell to check table : scan 'test1'
	}
	
	public static void runImport() throws Exception{
		String[] args={
				"-Dimporttsv.columns=HBASE_ROW_KEY,cf1:v01,cf1:v02,cf1:v03,cf1:v04,cf1:v05,cf1:v06"
				+ ",cf1:v07,cf1:v08,cf1:v09,cf1:v10,cf1:v11,cf1:v12,cf1:v13,cf1:v14,cf1:v15,cf1:v16"
				+ ",cf1:v17","-Dimporttsv.separator=;",MyConnection.TABLE,"/user/fansy/data.txt"
		};
		 int status = ToolRunner.run(getConf(),new ImportTsv(), args);
		 System.out.println("status:"+status);
	}
	
	public static void upload() throws IllegalArgumentException, IOException{
		FileSystem fsFileSystem = FileSystem.get(getConf());
		System.out.println(path.toString());
		fsFileSystem.copyFromLocalFile(path, new Path("data.txt"));
		System.out.println("data upload done!");
	}
	
	public static void createTable() throws IOException{
		Connection connection = ConnectionFactory.createConnection(getHConf());
		MyConnection.createTable(connection);
	}
	private static Configuration configuration =null;
	public static Configuration getConf(){
		if(configuration==null){
			configuration = new Configuration();
			configuration.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
			configuration.set("fs.defaultFS", "hdfs://node1:8020");// 指定namenode
			configuration.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
			configuration.set("yarn.resourcemanager.address", "node1:8032"); // 指定resourcemanager
			configuration.set("yarn.resourcemanager.scheduler.address", "node1:8030");// 指定资源分配器
			configuration.set("mapreduce.jobhistory.address", "node2:10020");// 指定historyserver
			configuration.set("hbase.master", "node2:16000");
			configuration.set("hbase.rootdir", "hdfs://node1:8020/hbase");
			configuration.set("hbase.zookeeper.quorum", "node2,node3,node4");
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
//			configuration.set("mapreduce.job.jar","");// 设置jar包路径
		}
		
		return configuration;
	}
	private static Configuration hConfiguration=null;
	public static Configuration getHConf(){
		if(hConfiguration==null){
			hConfiguration = HBaseConfiguration.create();
			hConfiguration.set("hbase.master", "node2:16000");
			hConfiguration.set("hbase.rootdir", "hdfs://node1:8020/hbase");
			hConfiguration.set("hbase.zookeeper.quorum", "node2,node3,node4");
			hConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
		}
		return hConfiguration;
	}
}
