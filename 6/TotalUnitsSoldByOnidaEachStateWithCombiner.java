package com.bigdata.acadgild;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TotalUnitsSoldByOnidaEachStateWithCombiner {

	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJarByClass(TotalUnitsSoldByOnidaEachStateWithCombiner.class);
		
		job.setMapperClass(TotatlUnitsSoldbyOnidaMapper.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(TotalUnitsSoldByOnidaReducer.class);
		
		job.setReducerClass(TotalUnitsSoldByOnidaReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	
	private static class TotatlUnitsSoldbyOnidaMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final String NA = "NA";
		private static final String ONIDA = "Onida";
		private static final IntWritable one = new IntWritable(1);
		private static Text state = new Text();
		
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			String[] values = value.toString().split("\\|");
			if ( ONIDA.equals(values[0].trim()) && !NA.equals(values[1]) )
			{
				state.set(values[3]);
				context.write(state, one);
			}
		}
	}
	
	private static class TotalUnitsSoldByOnidaReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable totalCounts = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
		{
			Integer count = 0;
			for ( IntWritable value : values )
			{
				count+= value.get();
			}
			totalCounts.set(count);
			context.write(key, totalCounts);
		}
	}

}
