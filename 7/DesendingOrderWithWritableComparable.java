package com.bigdata.acadgild;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DesendingOrderWithWritableComparable {

	public static void main(String[] args) throws Exception {
		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJarByClass(DesendingOrderWithWritableComparable.class);
		job.setMapOutputKeyClass(CompanyKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(DescMapper.class);
		
		job.setPartitionerClass(DescPartitioner.class);
		
		job.setNumReduceTasks(5);
		
		job.setReducerClass(DescReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	
	
	
	private static class DescMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, CompanyKey, IntWritable>
	{
		private static final String NA = "NA";
		
		public void map(LongWritable key, Text value, Context context ) throws InterruptedException, IOException {
			CompanyKey companyKey = new CompanyKey();
			IntWritable one = new IntWritable(1);
			
			String[] values = value.toString().split("\\|");
			if ( !NA.equals(values[0]) && !NA.equals(values[1]) )
			{
				companyKey.setName(values[0]);
				companyKey.setProduct(values[1]);
				companyKey.setInch(Integer.valueOf(values[2]));
				context.write(companyKey,one); 
			}
		}
		
	}
	
	private static class DescReducer extends org.apache.hadoop.mapreduce.Reducer<CompanyKey, IntWritable, Text, IntWritable>
	{
		private Text text = new Text();
		private IntWritable c = new IntWritable(0);
		
		public void reduce(CompanyKey key, Iterable<IntWritable> values, Context context ) throws IOException,InterruptedException
		{
			int count = 0;
			for ( IntWritable value : values )
			{
				count=+ value.get();
			}
			text.set(key.toString());
			c.set(count);
			context.write(text, c);
		}
	}
	
	private static class DescPartitioner extends Partitioner<CompanyKey, IntWritable>
	{
		@Override
		public int getPartition(CompanyKey arg0, IntWritable arg1, int arg2) {
			char c = arg0.getName().charAt(0);
		    if ( c == 'A' )
		    	return 0;
		    else if ( c == 'L' )
		    	return 1;
		    else if ( c == 'O' )
		    	return 2;
		    else if ( c == 'S' )
		    	return 3;
		    else 
		    	return 4;
		}
	}
	
	private static class CompanyKey implements WritableComparable<CompanyKey>
	{
		private String name;
		private String product;
		private Integer inch;

		@Override
		public void readFields(DataInput arg0) throws IOException {
			name = arg0.readUTF();
			product = arg0.readUTF();
			inch = arg0.readInt();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeUTF(name);
			arg0.writeUTF(product);
			arg0.writeInt(inch);
		}
		
		@Override
		public int compareTo(CompanyKey o) {
			int p = this.product.compareTo(o.getProduct());
			int c = this.inch.compareTo(o.getInch());
			
			if ( p == 0 )
			{
				if ( c > 0 )
					return -1;
				else if ( c < 0 )
					return 1;
				else 
					return 0;
			}
			else if ( p > 0 )
			{
				return -1;
			}
			else
				return 1;
			
			
/*			if ( this.name.compareTo(o.getName()) == 0 )
			{
				if ( this.product.compareTo(o.getProduct()) == 0 )
				{
					if ( this.inch.compareTo(o.getInch()) == 0 )
						return 0;
					else
						return this.inch.compareTo(o.getInch());
				}
				else
					return this.product.compareTo(o.getProduct());
			}
			else
				return this.name.compareTo(o.name);*/
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getProduct() {
			return product;
		}

		public void setProduct(String product) {
			this.product = product;
		}

		public int getInch() {
			return inch;
		}

		public void setInch(int inch) {
			this.inch = inch;
		}
		
		public String toString() {
			return this.name.concat("  ").concat(this.product).concat("  ").concat(String.valueOf(this.inch));
		}
	}
	
}