package com.company.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BrazilTransactionCount {

    public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text BRAZIL_KEY = new Text("Brazil");
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Pular o cabeçalho
            if (value.toString().contains("Country;Year;Commodity code")) {
                return;
            }

            String[] fields = value.toString().split(";");

            // Verificar se tem todos os campos necessários
            if (fields.length >= 1) {
                String country = fields[0].trim();

                // Contar apenas transações do Brasil
                if ("Brazil".equalsIgnoreCase(country)) {
                    context.write(BRAZIL_KEY, ONE);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Nota: Não é necessário o método main() pois o job será controlado pelo TransactionAnalysis
}