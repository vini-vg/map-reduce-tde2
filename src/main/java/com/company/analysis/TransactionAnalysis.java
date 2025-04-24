package com.company.analysis;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransactionAnalysis {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Uso para MODO LOCAL:");
            System.err.println("  TransactionAnalysis <1-8> file:///caminho/do/arquivo.csv file:///caminho/output");
            System.err.println("Uso para CLUSTER HADOOP:");
            System.err.println("  TransactionAnalysis <1-8> hdfs:///caminho/no/hdfs.csv hdfs:///caminho/output");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // Configuração para melhorar performance com arquivos grandes
        conf.set("mapreduce.input.fileinputformat.split.minsize", "268435456"); // 256MB por split
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        Job job = Job.getInstance(conf, "Transaction Analysis - Q" + args[0]);
        job.setJarByClass(TransactionAnalysis.class);

        // Configuração dinâmica baseada no tipo de path (file:// ou hdfs://)
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Configuração dos jobs conforme a questão
        switch (args[0]) {
            case "1":
                job.setMapperClass(BrazilTransactionCount.Mapper.class);
                job.setReducerClass(BrazilTransactionCount.Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;
            case "2":
                job.setMapperClass(TransactionsPerYear.Mapper.class);
                job.setReducerClass(TransactionsPerYear.Reducer.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(IntWritable.class);
                break;
            case "3":
                job.setMapperClass(TransactionsPerCategory.Mapper.class);
                job.setReducerClass(TransactionsPerCategory.Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;
            case "4":
                job.setMapperClass(TransactionsPerFlow.Mapper.class);
                job.setReducerClass(TransactionsPerFlow.Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;
            case "5":
                job.setMapperClass(AvgPricePerYearBrazil.Mapper.class);
                job.setReducerClass(AvgPricePerYearBrazil.Reducer.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;
            case "6":
                job.setMapperClass(MinMaxPriceBrazil2016.Mapper.class);
                job.setReducerClass(MinMaxPriceBrazil2016.Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "7":
                job.setMapperClass(AvgExportPriceBrazil.Mapper.class);
                job.setReducerClass(AvgExportPriceBrazil.Reducer.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;
            case "8":
                job.setMapperClass(MinMaxPricePerYearCountry.Mapper.class);
                job.setReducerClass(MinMaxPricePerYearCountry.Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            default:
                throw new IllegalArgumentException("Questão inválida: " + args[0]);
        }

        // Configurações comuns para todos os jobs
        job.setNumReduceTasks(4); // Número de reducers para paralelismo

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
