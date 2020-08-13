package com.briup.bigdata.search3.Step3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

/**
 * 提取每个页面的关键字，主要思路，因为p列族下的t列数据不是很准确 这里可以使用每个页面入链接标签中的内容作为每个页面的主题内容即为关键字
 */
public class GetKeyWordMR extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetKeyWordMR(), args);
	}

	@Override
	public int run(String[] strings) throws Exception {

		Configuration configuration = getConf();
		configuration.set("hbase.zookeeper.quorum", "aaa:2181");
		Job job = Job.getInstance(configuration);
		job.setJobName(GetKeyWordMR.class.getName());
		job.setJarByClass(GetKeyWordMR.class);
		TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("clean_webpage"), new Scan(), KeyWordMapper.class,
				Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("clean_webpage", KeyWordReducer.class, job);
		job.waitForCompletion(true);
		return 0;
	}

	/**
	 * 提取每个页面的关键字 简单提取：网页的关键字通过入链的超链接标签中的字符内容 以及当前网页中 title 来确定
	 */
	public static class KeyWordMapper extends TableMapper<Text, Text> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			// 准备好接收输出key的对象
			Text outKey = new Text();
			// 准备好接收输出key的对象
			Text outValue = new Text();
			// 设置行健为输出的key
			outKey.set(key.get());
			// 获取页面标题，即获取clean_table表中page列族 t列内容
			byte[] title_bytes = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("t"));
			// 转化成字符串
			String title = Bytes.toString(title_bytes);
			// 如果该内容不是空且长度大于0，则去入链接列表中，第一个有内容的value当做title
			if (title != null && title.length() > 0) {
				// 获取入链的列表
				NavigableMap<byte[], byte[]> il_list = value.getFamilyMap(Bytes.toBytes("il"));
				// 如果map的长度大于0，对其进行循环，获取第一个有文字值的作为当前页面主题
				// 最后为了保险，将 il中获取到的 主题 和 title进行拼接 xxxx,yyyy
				if (il_list.size() > 0) {
					for (byte[] a : il_list.keySet()) {
						if (a != null && a.length>0) {
							title += Bytes.toString(a);
							break;
						}
					}

					// 设置value
					outValue.set(title);
					// Mapper进行键值对输出
					context.write(outKey, outValue);

				}
			}
		}
	}

	public static class KeyWordReducer extends TableReducer<Text, Text, NullWritable> {
		@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           //构建一个插入数据对象Put
           Put put = new Put(key.toString().getBytes());
           //获取该页面的关键字 values中的有且只有一个的关键字字符串
           Text text = values.iterator().next();
           //输出关键字 到page列族 key列下
            put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("key"), text.toString().getBytes());
           context.write(NullWritable.get(),put);
        }
	}

}
