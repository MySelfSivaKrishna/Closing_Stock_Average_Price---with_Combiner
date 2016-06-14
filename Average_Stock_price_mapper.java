package com.average;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Average_Stock_price_mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] store = line.split(",");
		String stock_symbol = store[1];
		Double closing_value = Double.parseDouble(store[7]);
		context.write(new Text(stock_symbol), new Text(closing_value.toString()));
		
	}

}
