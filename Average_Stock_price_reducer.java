package com.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Average_Stock_price_reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2) throws IOException, InterruptedException {
		Double sum = 0d;
		Double count = 0d ;
		for(Text each: arg1){
			String[] spliter = each.toString().split(",");
			count +=Double.parseDouble(spliter[1]) ;
			sum +=Double.parseDouble(spliter[0]);
		}
		Double average = sum;
		average /=count;
		
		
		
		arg2.write(arg0,new DoubleWritable(average) );
		
	}

	

	
}
