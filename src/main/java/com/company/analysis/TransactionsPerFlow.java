package com.company.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransactionsPerFlow {
    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().contains("Country;Year;")) return;

            String[] fields = value.toString().split(";");
            if (fields.length >= 5) {
                String flow = fields[4].trim();
                context.write(new Text(flow), ONE);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}