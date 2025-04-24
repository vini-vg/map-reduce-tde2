package com.company.analysis;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class BrazilTransactionCount {

    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text BRAZIL = new Text("Brazil");
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                TransactionParser parser = new TransactionParser();
                if (parser.parse(value.toString()) {
                    if ("Brazil".equals(parser.getCountry())) {
                        context.write(BRAZIL, ONE);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing record: " + value);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            try {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                context.write(key, new IntWritable(sum));
            } catch (Exception e) {
                System.err.println("Error reducing key: " + key);
            }
        }
    }
}