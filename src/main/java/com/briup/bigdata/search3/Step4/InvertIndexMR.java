package com.briup.bigdata.search3.Step4;

import java.io.IOException;
import java.util.Map.Entry;

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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 构建倒置索引表
 * 新建表 invertindex
 * create 'invertindex','page'
 * 行健是 关键字  page列族下 放 url:rank##cnt
 * 为了方便对接页面
 * */
public class InvertIndexMR extends Configured implements Tool {
	
	static BytesWritable bw1 = new BytesWritable();
	static BytesWritable bw2 = new BytesWritable();
	
	 public static void main(String[] args) {
	        try {
	            ToolRunner.run(new InvertIndexMR(),args);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
    }
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"BuildInvertIndex");
        job.setJarByClass(InvertIndexMR.class);
        TableMapReduceUtil.initTableMapperJob("clear_webpage",new Scan(),IIMapper.class,ImmutableBytesWritable.class,MapWritable.class,job);
        TableMapReduceUtil.initTableReducerJob("invertindex",IIReducer.class,job);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        return 0;
    }

    /**
     * 读取clean_webpage中数据<Br>
     * 输入key: rowkey行键，url
     * 输入value:行键以外的数据
     * 
     * 输出key:页面的关键字
     * 输出value:该关键字对应的rank,url,c等值的MapWritable对象
     * */
	public static class IIMapper extends TableMapper<ImmutableBytesWritable,MapWritable>{
		
		
		
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //如果该行包含非空  page:key   page:rank，则为有效行，如果是有效行执行下面操作
        	byte[] page_key = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("key"));
        	byte[] page_rank = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("rank"));
        	
        	/*boolean iskey = value.containsNonEmptyColumn(Bytes.toBytes("page"), Bytes.toBytes("key"));
        	boolean isrank = value.containsNonEmptyColumn(Bytes.toBytes("page"), Bytes.toBytes("rank"));
        	if(iskey && isrank){*/
        	
        	if(page_key != null && page_rank != null){
            	//有效  则取 page:key
        		
            	//有效则取 page:rank
        	
            	//有效则取 page:c
        		byte[] page_c = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("c"));
            	//如果page:c为空 则赋值空字符串
        		if(page_c.length == 0){
        			page_c = Bytes.toBytes("");
        		}
            	//构建MapWritable进行存放三个值  rank   url   cnt
        		MapWritable map = new MapWritable();
        		
            	//页面关键字去空格
        		
        		page_key = Bytes.toBytes(Bytes.toString(page_key).trim());
        		
            	//输出 
        		
        		bw1.set(Bytes.toBytes("rank"), 0, Bytes.toBytes("rank").length);
        		bw2.set(page_rank,0,page_rank.length);
        		map.put(bw1, bw2);
        		
        		bw1.set(Bytes.toBytes("url"), 0, Bytes.toBytes("url").length);
        		bw2.set(key.get(),key.getOffset(),key.getLength());
        		map.put(bw1, bw2);
        		
        		bw1.set(Bytes.toBytes("cnt"), 0, Bytes.toBytes("cnt").length);
        		bw2.set(page_c,0,page_c.length);
        		map.put(bw1, bw2);
        		
        		key.set(page_key);
        		context.write(key, map);
        	}
        	
        }
    }
	/**
	 * 
	 * */
    public static class IIReducer extends TableReducer<ImmutableBytesWritable,MapWritable,NullWritable>{
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        	//获取页面关键字的字符串表现形式，要去空格
        	String keyword = key.get().toString().trim();
        	//如果 页面关键字字符串 长度不为0，则进行下面操作
        	if(keyword.length()>0){
            	//构建put,页面关键字为 rowkey行键
               Put put = new Put(Bytes.toBytes(keyword));
                //遍历reduce的value值
               for(MapWritable mw : values){
                	//从 reduce 的输入value中遍历得到的MapWritable中获取url  (Text)
            	   bw1.set(Bytes.toBytes("url"), 0, Bytes.toBytes("url").length);
            	   BytesWritable url = (BytesWritable) mw.get(bw1);
                	//从 reduce 的输入value中遍历得到的MapWritable中获取rank (DoubleWritable)
            	   bw1.set(Bytes.toBytes("rank"), 0, Bytes.toBytes("rank").length);
            	   BytesWritable rank = (BytesWritable) mw.get(bw1);
                	//从 reduce 的输入value中遍历得到的MapWritable中获取cnt	  (Text)
            	   bw1.set(Bytes.toBytes("cnt"), 0, Bytes.toBytes("cnt").length);
            	   BytesWritable cnt = (BytesWritable) mw.get(bw1);
            	   
                	//把数据添加到put的列中
            	 //page为列族，url为列，rank+cnt为value值
            	   String value = rank.toString()+cnt.toString();
            	   put.addColumn(Bytes.toBytes("page"), Bytes.toBytes(url.toString()), Bytes.toBytes(value));
            	   
            	   /*put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("url"), url.getBytes());
            	   put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("rank"), rank.getBytes());
            	   put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("cnt"), cnt.getBytes());*/
        	
                	
               }
                //输出
               context.write(NullWritable.get(), put);
        	}
        }
    }
}
