package com.NbaAnalysis.Reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BaseMostScorePlayerReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final IntWritable totalScore = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Sum up all score deltas for this player
        for (IntWritable value : values) {
        sum += value.get();
        }

        totalScore.set(sum);
        context.write(new Text( key +"scored "),new Text (totalScore + " points in the full tournament"));

    }
}