package com.company.analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxPriceBrazil2016 {
    public static class MapperClass extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text yearKey = new Text("2016");
        private DoubleWritable price = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().contains("Country;Year;")) return;

            String[] fields = value.toString().split(";");
            if (fields.length >= 6 && "Brazil".equalsIgnoreCase(fields[0].trim())
                    && "2016".equals(fields[1].trim())) {
                try {
                    double transactionPrice = Double.parseDouble(fields[5].trim());
                    price.set(transactionPrice);
                    context.write(yearKey, price);
                } catch (NumberFormatException e) {
                    // Ignora valores inválidos
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (DoubleWritable val : values) {
                double price = val.get();
                if (price < min) min = price;
                if (price > max) max = price;
            }

            result.set("Min: " + min + ", Max: " + max);
            context.write(key, result);
        }
    }
}