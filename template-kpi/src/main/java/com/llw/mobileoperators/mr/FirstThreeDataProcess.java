package com.llw.mobileoperators.mr;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.llw.mobileoperators.mr.BaseStationDataPreprocess.Counter;
import com.llw.template_kpi.mr.kpi.KPI;

/*
 * 每个用户只保留三个最长停留基站，以第一次的计算结果作为这次的输入，输入格式如下：
 * 0000000000|00000133|00-05|1.9333334
 * 0000000000|00000158|00-05|7.45
 * 0000000000|00000041|00-05|44.100002
 * 0000000000|00000194|00-05|57.1
 */
public class FirstThreeDataProcess {

	public static class Map extends Mapper<Object, Text, Text, Text>{

		private Text one = new Text();
        private Text word = new Text();
        private FirstThreeTable myThreeTable = new FirstThreeTable();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			/*
			 * 用户名+时段为key
			 * 基站名+时长为value
			 */
			try{
				myThreeTable.parseLine(value.toString());
				one.set(myThreeTable.outKey());
				word.set(myThreeTable.outValue());
				context.write(one,word);
			}catch(Exception e) {
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}
			
		}
		
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		private Text value = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			System.out.println("reduce:key="+key.toString());
			//定义一个map存储每个用户在同一时间点的所有记录，并以时长为key排序(倒排序)
			TreeMap<String, String> uploads = 
					new TreeMap<String, String>(new Comparator<String>(){      
						@Override
						/*  
			             * int compare(Object o1, Object o2) 返回一个基本类型的整型，  
			             * 返回负数表示：o1 小于o2，  
			             * 返回0 表示：o1和o2相等，  
			             * 返回正数表示：o1大于o2。  
			             */    
						public int compare(String o1, String o2) {
							double d1 = Double.valueOf(o1);
							double d2 = Double.valueOf(o2);
							return (int)(d2 - d1);
						}  
			        });
			String valueString;
			for(Text value:values) {
				valueString = value.toString();
				System.out.println("reduce:value="+valueString);
				try{
					uploads.put(valueString.split("\\|")[1], valueString.split("\\|")[0]);
				} catch (NumberFormatException e) {
					context.getCounter(Counter.TIMESKIP).increment(1);
					continue;
				}
			}
			//输出
			Entry<String, String> upload;
			Iterator<Entry<String, String>> it = uploads.entrySet().iterator();
			upload = it.next();
			//计算
			int iThree = 3;
			while(it.hasNext()) {
				value.set(upload.getKey()+"|"+upload.getValue());
				context.write(key,value);
				iThree--;
				if(iThree <= 0){
					break;
				}
				upload = it.next();
			}
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "FirstThreeDataProcess");
		job.setJarByClass(FirstThreeDataProcess.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
