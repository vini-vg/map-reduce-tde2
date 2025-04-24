package com.company.analysis;


import org.apache.hadoop.io.*;

public class TransactionsPerCategory {
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            TransactionParser parser = new TransactionParser();
            if (parser.parse(value.toString())) {
                context.write(new Text(parser.getCategory()), ONE);
            }
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }
}
