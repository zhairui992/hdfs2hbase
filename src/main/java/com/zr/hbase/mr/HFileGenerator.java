package com.zr.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

//import com.zr.hbase.util.KerberosUtil;


@SuppressWarnings("deprecation")
public class HFileGenerator {

	private static Logger logger = Logger.getLogger(HFileGenerator.class);

	private static String family = "dim"; // 列族

	public static void main(String[] args) {

		long startTime = System.currentTimeMillis();
		try {
			logger.info("add arg to hadoop.......................");

			// 输入 输出路径
			String outpath = "/user/zr/hbase/";
			String inputpath = "/user/zr/file/";

			Configuration conf = new Configuration();
			conf.addResource(new Path(args[0]));
			Connection conn = ConnectionFactory.createConnection(conf);
			// hbase 配置文件
			// 是否需要kerberos认证
			// KerberosUtil.kerberosInit(conf, args[1]);
			TableName teableName = TableName.valueOf("hbase_test");
			
			logger.info("create hbase table...................");
			createTable(teableName, conn);
			
			logger.info("set hadoop job.......................");

			HTable table = (HTable) conn.getTable(teableName);
			Job job = getHadoopJob(conf, outpath, table);
			

			logger.info("set multi inputs.......................");
			setMultiInputs(inputpath, job);

			logger.info("remove outpath.......................");
			removeOutPath(conf, outpath);

			logger.info("start hadoop job.......................");
			if (job.waitForCompletion(true)) {
				// 写完HFile开始load hbase
				logger.info("end hadoop job.......................");
				logger.info("start hbase loade.......................");
				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
				loader.doBulkLoad(new Path(outpath), table);
				logger.info("end hbase loade.......................");
			}

			long endTime = System.currentTimeMillis();
			logger.info("task  end,use time :" + (endTime - startTime) / 1000 + "s");

			System.exit(0);
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
	}

	private static void setMultiInputs(String inputPath, Job job) {
		MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, HbaseMapper.class);
	}

	private static void removeOutPath(Configuration conf, String outpath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path outputDir = new Path(outpath);
		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
	}

	private static void createTable(TableName tableName, Connection conn) throws IOException {
		Admin h = conn.getAdmin();
		if (h.tableExists(tableName)) {
			h.disableTable(tableName);
			h.deleteTable(tableName);
		}
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor(family));
		// 预先为hbase表分区  分区个数决定reduce数 影响mr任务效率
		HexStringSplit split = new HexStringSplit();
		byte[][] splits = split.split(20);
		h.createTable(desc, splits);

	}


	private static Job getHadoopJob(Configuration conf, String outpath, HTable table) throws IOException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(HFileGenerator.class);
		job.setReducerClass(PutSortReducer.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		HFileOutputFormat.configureIncrementalLoad(job, table);
		return job;
	}
}
