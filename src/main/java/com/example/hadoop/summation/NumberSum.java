package com.example.hadoop.summation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class NumberSum {

    private static final Logger logger = Logger.getLogger(NumberSum.class.getName());
    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if(remainingArgs.length <2 ) {
           logger.log(new LogRecord(Level.SEVERE, "Please enter the input path and output path"));
           System.exit(2);
        }

        JobConf entries = new JobConf(configuration, NumberSum.class);

        entries.setJobName("number sum");
        entries.setJarByClass(NumberSum.class);

        entries.setMapperClass(MyMapper.class);
        entries.setReducerClass(MyReducer.class);

        entries.setMapOutputKeyClass(Text.class);
        entries.setMapOutputValueClass(LongWritable.class);
        entries.setOutputValueClass(LongWritable.class);
        entries.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(entries, new Path(remainingArgs[remainingArgs.length-2]));
        FileOutputFormat.setOutputPath(entries, new Path(remainingArgs[remainingArgs.length-1]));

        boolean complete = JobClient.runJob(entries).isComplete();

        System.out.println( complete ? "Successfully complete the job" : "Something is wrong");

    }
}
