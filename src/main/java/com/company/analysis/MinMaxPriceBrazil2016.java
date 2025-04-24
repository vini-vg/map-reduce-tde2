package com.company.analysis;


import org.apache.hadoop.io.*;

public class MinMaxPriceBrazil2016 {
    public static class PriceWritable implements Writable {
        public double price;
        public String commodity;

        public void write(DataOutput out) throws IOException {
            out.writeDouble(price);
            out.writeUTF(commodity);
        }

        public void readFields(DataInput in) throws IOException {
            price = in.readDouble();
            commodity = in.readUTF();
        }
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, PriceWritable> {
        private final Text BRAZIL_2016 = new Text("Brazil_2016");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            TransactionParser parser = new TransactionParser();
            if (parser.parse(value.toString()) &&
                    parser.getCountry().equals("Brazil") &&
                    parser.getYear() == 2016) {

                PriceWritable price = new PriceWritable();
                price.price = parser.getPrice();
                price.commodity = parser.getCommodity();
                context.write(BRAZIL_2016, price);
            }
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, PriceWritable, Text, Text> {
        public void reduce(Text key, Iterable<PriceWritable> values, Context context)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            String minCommodity = "";
            String maxCommodity = "";

            for (PriceWritable val : values) {
                if (val.price < min) {
                    min = val.price;
                    minCommodity = val.commodity;
                }
                if (val.price > max) {
                    max = val.price;
                    maxCommodity = val.commodity;
                }
            }

            context.write(key,
                    new Text(String.format("Max: %s (USD %.2f), Min: %s (USD %.2f)",
                            maxCommodity, max, minCommodity, min)));
        }
    }
}
