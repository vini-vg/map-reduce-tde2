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

        // Configuração especial para Windows (se necessário)
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        conf.set("mapreduce.framework.name", "local");

        // Configuração para melhorar performance com arquivos grandes
        conf.set("mapreduce.input.fileinputformat.split.minsize", "268435456"); // 256MB por split
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        Job job = Job.getInstance(conf, "Transaction Analysis - Q" + args[0]);
        job.setJarByClass(TransactionAnalysis.class);

        // Configuração dinâmica baseada no tipo de path
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Configuração dos jobs conforme a questão
        switch (args[0]) {
            case "1": // Já está correto (BrazilTransactionCount)
                job.setMapperClass(BrazilTransactionCount.MapperClass.class);
                job.setReducerClass(BrazilTransactionCount.ReducerClass.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "2": // Transações por ano
                job.setMapperClass(TransactionsPerYear.MapperClass.class);
                job.setReducerClass(TransactionsPerYear.ReducerClass.class);
                job.setOutputKeyClass(Text.class);  // Ano como Text
                job.setOutputValueClass(IntWritable.class);
                break;

            case "3": // Transações por categoria
                job.setMapperClass(TransactionsPerCategory.MapperClass.class);
                job.setReducerClass(TransactionsPerCategory.ReducerClass.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "4": // Transações por tipo de fluxo
                job.setMapperClass(TransactionsPerFlow.MapperClass.class);
                job.setReducerClass(TransactionsPerFlow.ReducerClass.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "5": // Valor médio por ano (Brasil)
                job.setMapperClass(AvgPricePerYearBrazil.MapperClass.class);
                job.setReducerClass(AvgPricePerYearBrazil.ReducerClass.class);
                job.setOutputKeyClass(Text.class);  // Ano como Text
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "6": // Transação mais cara/barata (Brasil 2016)
                job.setMapperClass(MinMaxPriceBrazil2016.MapperClass.class);
                job.setReducerClass(MinMaxPriceBrazil2016.ReducerClass.class);
                job.setOutputKeyClass(Text.class);  // Chave fixa "min"/"max"
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "7": // Valor médio exportações (Brasil)
                job.setMapperClass(AvgExportPriceBrazil.MapperClass.class);
                job.setReducerClass(AvgExportPriceBrazil.ReducerClass.class);
                job.setOutputKeyClass(Text.class);  // Ano como Text
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "8": // Maior/menor preço por ano e país
                job.setMapperClass(MinMaxPricePerYearCountry.MapperClass.class);
                job.setReducerClass(MinMaxPricePerYearCountry.ReducerClass.class);
                job.setOutputKeyClass(Text.class);  // Formato "País_Ano"
                job.setOutputValueClass(Text.class); // Formato "min:X,max:Y"
                break;

            default:
                throw new IllegalArgumentException("Questão inválida: " + args[0]);
        }

        job.setNumReduceTasks(4); // Número de reducers para paralelismo

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}