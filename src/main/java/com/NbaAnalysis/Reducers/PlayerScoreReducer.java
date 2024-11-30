package com.NbaAnalysis.Reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlayerScoreReducer extends Reducer<Text, IntWritable, Text, Text> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        String team = "";
        String player = "";
        String quarter = "";
        String game = "";
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
  
        String [] splitedKey = key.toString().split("_");
        game = splitedKey[0];
        team = splitedKey[1];
        player = splitedKey[2];
        quarter = splitedKey[3];

        context.write(new Text(game +","+ team + ","+ player+","), new Text(quarter + "," + result));
    }
}