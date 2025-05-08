package com.company.analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgPricePerYearBrazil {
    public static class MapperClass extends Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable price = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().contains("Country;Year;")) return;

            String[] fields = value.toString().split(";");
            if (fields.length >= 6 && "Brazil".equalsIgnoreCase(fields[0].trim())) {
                try {
                    double transactionPrice = Double.parseDouble(fields[5].trim());
                    price.set(transactionPrice);
                    context.write(new Text(fields[1].trim()), price);
                } catch (NumberFormatException e) {
                    // Ignora valores inválidos
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }
}