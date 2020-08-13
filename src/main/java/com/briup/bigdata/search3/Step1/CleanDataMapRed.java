package com.briup.bigdata.search3.Step1;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CleanDataMapRed extends Configured implements Tool {
	
	static BytesWritable bw1 = new BytesWritable();
	static BytesWritable bw2 = new BytesWritable();
	
	public static void main(String[] args) throws Exception {
		//调用run方法
		ToolRunner.run(new CleanDataMapRed(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"cleanData");
		String table1 = "t2_webpage";
		String table2 = "clear_webpage";
		conf.set("hbase.zookeeper.quorum", "192.168.109.128:2181");
		TableMapReduceUtil.initTableMapperJob(table1, new Scan(), CDMapper.class, ImmutableBytesWritable.class, MapWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(table2, CDReducer.class,job);
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class CDMapper extends TableMapper<ImmutableBytesWritable,MapWritable>{
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, MapWritable>.Context context)
				throws IOException, InterruptedException {
			//获取url
			//获取url的绝对路径
			byte[] url = value.getValue(Bytes.toBytes("f"), Bytes.toBytes("bas"));
			

			//获取数据
			//获取数据的字节数组，并把字节数组转换为int类型
			byte[] data = value.getValue(Bytes.toBytes("f"), Bytes.toBytes("st"));
			int data_flag = Bytes.toInt(data);
			
			//判断是否为正确数据
			if(data_flag == 2){
				//获取内容p:c
				//若text为null，则设置为空字符串
				byte[] text = value.getValue(Bytes.toBytes("p"), Bytes.toBytes("c"));
				if(text == null){
					text = "".getBytes();
				}
				//获取标题p:t
				//若title为null，则设置为空字符串
				byte[] title = value.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"));
				if(title == null){
					title = "".getBytes();
				}
				
				//获取ol列族下的所有数据
				NavigableMap<byte[], byte[]> ol_map = value.getFamilyMap(Bytes.toBytes("ol"));
				//获取ol列族的行数(行键个数)，即为出链个数
				int ol_size = ol_map.size();
				
				//创建一个空的出链接列表
				MapWritable ol_list = new MapWritable();
				
				//遍历ol_map获得列族ol的链接列表
				for(Entry<byte[], byte[]> e : ol_map.entrySet()){
					byte[] k = e.getKey();
					byte[] v = e.getValue();
					bw1.set(k,0,k.length);
					bw2.set(v,0,v.length);
					ol_list.put(bw1, bw2);
				}
				//获取il列族下的所有数据
				NavigableMap<byte[], byte[]> il_map = value.getFamilyMap(Bytes.toBytes("il"));
				//获取il列族的行数，即入链个数
				int il_size = il_map.size();
				
				//创建一个空的入链接列表
				MapWritable il_list = new MapWritable();
				
				//遍历il_map获得列族il的链接列表
				for(Entry<byte[],byte[]> e : il_map.entrySet()){
					byte[] k = e.getKey();
					byte[] v = e.getValue();
					bw1.set(k,0,k.length);
					bw2.set(v,0,v.length);
					il_list.put(bw1, bw2);
				}
				
				//把每个字段以键值的类型继续传递给reduce，封装到MapWritable对象输出
				MapWritable map = new MapWritable();
				
				//key为c，value为p:c，即变量text
				bw1.set(Bytes.toBytes("c"),0,Bytes.toBytes("c").length);
				bw2.set(text,0,text.length);
				map.put(bw1, bw2);
				
				//key为t，value为p:t，即变量title
				bw1.set(Bytes.toBytes("t"),0,Bytes.toBytes("t").length);
				bw2.set(title,0,title.length);
				map.put(bw1, bw2);
				
				//key为oln，value为 出链个数，即变量ol_size
				bw1.set(Bytes.toBytes("oln"),0,Bytes.toBytes("oln").length);
				bw2.set(Bytes.toBytes(ol_size),0,Bytes.toBytes(ol_size).length);
				map.put(bw1, bw2);
				
				//key为iln，value为 入链个数，即变量il_size
				bw1.set(Bytes.toBytes("iln"),0,Bytes.toBytes("iln").length);
				bw2.set(Bytes.toBytes(il_size),0,Bytes.toBytes(il_size).length);
				map.put(bw1, bw2);
				
				//key为ol_list，value为出链接列表，即变量ol_list
				bw1.set(Bytes.toBytes("ol_list"),0,Bytes.toBytes("ol_list").length);
				map.put(bw1, ol_list);
				
				//key为il_list，value为入链接列表，即变量il_list
				bw1.set(Bytes.toBytes("il_list"),0,Bytes.toBytes("il_list").length);
				map.put(bw1, il_list);
				
				//把baseurl设置为key
				key.set(url);
				//key为baseurl，value为map，传给reducer
				context.write(key, map);
			}
		}
	}
	
	public static class CDReducer extends TableReducer<ImmutableBytesWritable, MapWritable, NullWritable>{
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values,
				Reducer<ImmutableBytesWritable, MapWritable, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			//取得values中的第一个值，即Mapper输出给reducer的map
			//一个行键，即一行数据只执行一次mapper，所以key只有一个，即map只有一个
			MapWritable map = values.iterator().next();
			//构建put对象用来组织数据存入hbase，该对象的使用key作为行键
			Put put = new Put(key.get());
			
			//从map中取出key为t的value值，将其添加进put中，列族是page，列名是t
			bw1.set(Bytes.toBytes("t"),0,Bytes.toBytes("t").length);
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("t"),
					((BytesWritable)map.get(bw1)).getBytes()
					);
			
			//从map中取出key为s的value值，将其添加进put中，列族是page，列名是s
			bw1.set(Bytes.toBytes("c"),0,Bytes.toBytes("c").length);
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("c"),
					((BytesWritable)map.get(bw1)).getBytes()
					);
			
			//从map中取出key为oln的value值，将其添加进put中，列族是page，列名是oln
			bw1.set(Bytes.toBytes("oln"),0,Bytes.toBytes("oln").length);
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("oln"),
					((BytesWritable)map.get(bw1)).getBytes()
					);
			
			//从map中取出key为iln的value值，将其添加进put中，列族是page，列名是iln
			bw1.set(Bytes.toBytes("iln"),0,Bytes.toBytes("iln").length);
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("iln"),
					((BytesWritable)map.get(bw1)).getBytes()
					);
			
			//从map中取出key为ol_list的value值，该类型为MapWritable
			bw1.set(Bytes.toBytes("ol_list"),0,Bytes.toBytes("ol_list").length);
			MapWritable ol_list = (MapWritable)map.get(bw1);
			//遍历ol_list，把数据添加到ol列族中，列名是ol_list中的key值
			for(Entry<Writable,Writable> e : ol_list.entrySet()){
				put.addColumn(Bytes.toBytes("ol"), 
						((BytesWritable)e.getKey()).getBytes(),
						((BytesWritable)e.getValue()).getBytes()
						);
			}
			
			//从map中取出key为il_list的value值，该类型为MapWritable
			bw1.set(Bytes.toBytes("il_list"), 0, Bytes.toBytes("il_list").length);
			MapWritable il_list = (MapWritable)map.get(bw1);
			//遍历il_list，把数据添加到il列族中，列名是il_list中的key值
			for(Entry<Writable,Writable> e : il_list.entrySet()){
				put.addColumn(Bytes.toBytes("il"),
						((BytesWritable)e.getKey()).getBytes(), 
						((BytesWritable)e.getValue()).getBytes()
						);
			}
			
			context.write(NullWritable.get(), put);
			
		}
	}
}