package demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class MyConnection {
	public static final String TABLE = "test1";
	public static final String FAMILY = "cf1";
	public static final String COL1 = "q1";
	public static final String COL2 = "q2";
	public static final String ROWKEY1 = "rk-1";
	public static final String ROWKEY2 = "rk-2";
	
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "node2:16000");
		conf.set("hbase.rootdir", "hdfs://node1:8020/hbase");
		conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = ConnectionFactory.createConnection(conf);
		//建表
		createTable(connection);
		
		// put 数据
		put(connection);
		
		// get data
		get(connection);
		
		// scan data
		scan(connection);
		
		connection.close();
	}

	public static void createTable(Connection connection) throws IOException {
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf(TABLE);
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		hTableDescriptor.addFamily(new HColumnDescriptor(FAMILY));
		
		if (!admin.tableExists(TableName.valueOf(TABLE))) {
			// 不存在，创建表
			admin.createTable(hTableDescriptor);
			System.out.println(TABLE+"表 被新建！");
		} else {
			// 该表存在，删除后再创建
			if (!admin.isTableAvailable(tableName)) {
				// 该表disable，直接删除
				admin.deleteTable(tableName);
				System.out.println(TABLE+"表 被删除！");
			} else {
				// 该表enable状态；先disable,再删除
				admin.disableTable(tableName);
				System.out.println(TABLE+"表 被disable！");
				admin.deleteTable(tableName);
				System.out.println(TABLE+"表 被删除！");
			}
			admin.createTable(hTableDescriptor);
			System.out.println(TABLE+"表 被新建！");
		}
	}

	public static void put(Connection connection) throws IOException {
		Table table = connection.getTable(TableName.valueOf(TABLE));
		List<Put> list = new ArrayList<Put>();
		
		Put put = new Put(Bytes.toBytes(ROWKEY1));
		put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL1), Bytes.toBytes("v11"));
		put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL2), Bytes.toBytes("v12"));
		list.add(put);
		
		put = new Put(Bytes.toBytes(ROWKEY2));
		put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL1), Bytes.toBytes("v21"));
		put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL2), Bytes.toBytes("v22"));
		
		list.add(put);
		
		table.put(list);
		
		System.out.println("data putted!");
		table.close();
	}

	public static void get(Connection connection) throws IOException {
		System.out.println("get...........");
		Table table = connection.getTable(TableName.valueOf(TABLE));
		Get get = new Get(ROWKEY1.getBytes());
		Result result = table.get(get);
		System.out.println(new String(result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL1))));
		table.close();
	}

	public static void scan(Connection connection) throws IOException {
		System.out.println("scan..............");
		Table table = connection.getTable(TableName.valueOf(TABLE));

		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> list = scanner.iterator();
		Result result = null;
		while (list.hasNext()) {
			result = list.next();
			System.out.println(new String(result.getRow())+":"+new String(result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL1))));
			System.out.println(new String(result.getRow())+":"+new String(result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL2))));
		}
		table.close();
	}
}
