package com.company.analysis;

import org.apache.hadoop.io.*;

public class AvgExportPriceBrazil {
    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, DoubleWritable> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            TransactionParser parser = new TransactionParser();
            if (parser.parse(value.toString()) &&
                    parser.getCountry().equals("Brazil") &&
                    parser.getFlow().equals("Export")) {

                context.write(new IntWritable(parser.getYear()),
                        new DoubleWritable(parser.getPrice()));
            }
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            context.write(key, new DoubleWritable(sum / count));
        }
    }
}
