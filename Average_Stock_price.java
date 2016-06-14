package com.average;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class Average_Stock_price extends Configured implements Tool  {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub			
		
		int exitCode = ToolRunner.run(new Average_Stock_price(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Average_Stock_price <input path> <output path>");
			System.exit(-1);
		}

		//Job Setup
		Job job = Job.getInstance(getConf(), "average");
		
		job.setJarByClass(Average_Stock_price.class);
		
		
		//File Input and Output format
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		

		//Mapper-Reducer-Combiner specifications
		job.setMapperClass(Average_Stock_price_mapper.class);
		job.setReducerClass(Average_Stock_price_reducer.class);
		job.setCombinerClass(Average_Stock_price_combiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		//Submit job
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
