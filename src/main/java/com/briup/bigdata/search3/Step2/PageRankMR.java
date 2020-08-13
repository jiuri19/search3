package com.briup.bigdata.search3.Step2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

public class PageRankMR extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PageRankMR(), args);
	}

	@Override
	public int run(String[] strings) throws Exception {
		// PropertyConfigurator.configure("/Users/angelia/IdeaProjects/code2019/searchProject/src/main/resources/log4j.properties");
		// 迭代10次
		for (int i = 1; i < 10; i++) {
			Configuration configuration = getConf();
			configuration.set("hbase.zookeeper.quorum", "192.168.109.128:2181");
			Job job = Job.getInstance(configuration);
			job.setJobName(PageRankMR.class.getName());
			job.setJarByClass(PageRankMR.class);
			TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("clear_webpage"), new Scan(),
					PageRankMR.PageRankMapper.class, Text.class, Text.class, job);
			TableMapReduceUtil.initTableReducerJob("clear_webpage", PageRankMR.PageRankReducer.class, job);

			job.waitForCompletion(true);
		}
		return 0;
	}

	/**
	 * 现有已知数据 clean_webpage page ol il 列族 page:iln 该页面的入链个数 page:oln 该页面的出链个数
	 * il:url title 该页面的入链列表以及对应的标题 ol:url title 该页面的出链列表以及对应的标题
	 *
	 */
	public static class PageRankMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			// 声明接收Mapper输出key value的对象
			Text outKey = new Text();
			Text outValue = new Text();
			// 获取该页面的url即是Rowkey值
			String pageUrl = Bytes.toString(key.get());
			// 获取该页面的权重值 权重值从 page:rank列中取
			byte[] ol_pagerank_bytes = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("rank"));
			// 设置一个初始化的权重值 1，仅在第一次使用的时候生效
			double ol_pagerank ;
			if(ol_pagerank_bytes == null){
				ol_pagerank = 1.0;
			}else{
				ol_pagerank = Bytes.toDouble(ol_pagerank_bytes);
			}
			
			// 计算该页面给每个出链的权重值
			
			// 获取出链个数
			byte[] ol_size_bytes = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("oln"));
			int ol_size = Bytes.toInt(ol_size_bytes);
			// 如果有此内容，则用当前权重值除以当前页面出链接个数
			if(ol_size!=0){
				ol_pagerank /= ol_size;
			}
			// 设置值给outValue
			outValue.set(Bytes.toBytes(ol_pagerank));
			// 获取出链列表
			NavigableMap<byte[], byte[]> ol_list = value.getFamilyMap(Bytes.toBytes("ol"));
			// 为每一个输出的连接 分权重
			// 输出格式为 出链url rank
			for(Entry<byte[],byte[]> e : ol_list.entrySet()){
				outKey.set(e.getKey());
				context.write(outKey, outValue);
			}
			

		}

	}

	public static class PageRankReducer extends TableReducer<Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 计算对应的页面url的权重之和
			Double sum = 0.0;
			for(Text t : values){
				sum += Double.parseDouble(t.toString());
			}
			//   由于存在一些出链为0，也就是那些不链接任何其他网页的网， 也称为孤立网页，使得很多网页能被访问到。因此需要对
			// PageRank公式进行修正，即在简单公式的基础上增加了阻尼系数（damping factor）q， q一般取值q=0.85。
			//
			//       其意义是，在任意时刻，用户到达某页面后并继续向后浏览的概率。 1- q=
			// 0.15就是用户停止点击，随机跳到新URL的概率）的算法被用到了所有页面上，估算页面可能被上网者放入书签的概率。
			//
			//      
			// 最后，即所有这些被换算为一个百分比再乘上一个系数q。由于下面的算法，没有页面的PageRank会是0。所以，Google通过数学系统给了每个页面一个最小值。
			// ---------------------
			// 按照阻尼系数修正之后计算出该页面的权重值
			sum *= 0.85;
			// 构建输出的Put
			Put put = new Put(key.toString().getBytes());
			// 设置page列族 rank列 存放本次迭代获得的 权重值
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("rank"), Bytes.toBytes(sum));
			// 输出数据
			context.write(NullWritable.get(),put);
		}
	}

}