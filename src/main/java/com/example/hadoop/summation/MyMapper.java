package com.example.hadoop.summation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private final Logger logger = Logger.getLogger(MyMapper.class.getName());

    @Override
    public void map(LongWritable longWritable, Text line, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {

        StringTokenizer stringTokenizer = new StringTokenizer(line.toString());
        Text text = new Text("total-sum");
        LongWritable sum = new LongWritable();
        long total = 0;


        while (stringTokenizer.hasMoreTokens()) {
            String value = stringTokenizer.nextToken();
            try {
                total += Long.parseLong(value);
            } catch (NumberFormatException e) {
                logger.warning("Number format problem with value " + value);
            }
        }
        sum.set(total);
        outputCollector.collect(text, sum);
    }
}
