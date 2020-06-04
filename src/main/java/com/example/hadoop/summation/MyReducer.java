package com.example.hadoop.summation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text text, Iterator<LongWritable> iterator, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {

        LongWritable longWritable = new LongWritable();
        AtomicLong total= new AtomicLong();

        iterator.forEachRemaining( (value) -> {
            total.addAndGet(value.get());
        });

        longWritable.set(total.longValue());
        outputCollector.collect(text, longWritable);
    }
}
