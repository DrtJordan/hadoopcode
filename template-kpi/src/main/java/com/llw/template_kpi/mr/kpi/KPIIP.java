package com.llw.template_kpi.mr.kpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPIIP {
	public static class Map extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();
        private Text ips = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
            KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                context.write(word, ips);
                System.out.println("map=word:"+word.toString()+"ips:"+ips.toString());
            }
		}
		
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		private Text result = new Text();
        private Set<String> count = new HashSet<String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
            for(Text oneValue : values){
            	count.add(oneValue.toString());
            	System.out.println("reduce=key:"+key.toString()+" oneValue:"+oneValue.toString()
            	+ "countSize="+count.size());
            }
            result.set(String.valueOf(count.size()));
            context.write(key, result);
            count.clear();
		}
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "KPIIP");
		job.setJarByClass(KPIIP.class);
		
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
