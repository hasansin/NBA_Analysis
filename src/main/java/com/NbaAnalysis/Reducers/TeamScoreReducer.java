package com.NbaAnalysis.Reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TeamScoreReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalScore = 0;
        String team = "";
        String quarter = "";

        for (IntWritable val : values) {
            totalScore += val.get();
        }

        String [] splitedKey = key.toString().split("_Q");
        team = splitedKey[0];
        quarter = splitedKey[1];

        context.write(new Text(team), new Text(quarter + "\t" + totalScore));
    }
}
