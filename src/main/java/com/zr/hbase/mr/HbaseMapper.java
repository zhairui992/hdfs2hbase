package com.zr.hbase.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private String family; // 列族
	private List<String> columns = new ArrayList<String>(); // 列
	private ImmutableBytesWritable hkey = new ImmutableBytesWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// 根据需要修改列 列族
		family = "dim";
		columns.add("dim1");
		columns.add("dim2");
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = StringUtils.split(value.toString(), "\t");
		String rowkey = values[0];
		byte[] r = Bytes.toBytes(rowkey);
		byte[] f = Bytes.toBytes(family);
		Put put = new Put(r);
		hkey.set(r);
		for (int i = 0; i < columns.size(); i++) {
			String column = columns.get(i);
			put.addColumn(f, Bytes.toBytes(column), Bytes.toBytes(values[i + 1]));
		}
		context.write(hkey, put);
	}
}