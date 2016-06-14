package com.average;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Average_Stock_price_combiner extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2) throws IOException, InterruptedException {
		
		Double sum = 0d;
		int count = 0;
		for(Text each: arg1){
			count +=1 ;
			sum +=Double.parseDouble(each.toString());
		}
		String  value_combiner = sum.toString() + "," + Integer.toString(count);
		
		arg2.write(arg0,new Text(value_combiner) );
	}

}
