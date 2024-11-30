package com.NbaAnalysis.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.NbaAnalysis.Mappers.PlayerScoreMapper;
import com.NbaAnalysis.Reducers.PlayerScoreReducer;

public class PlayerScoreExecutor {

    public static boolean runJob(String[]args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Player score team");

        job.setJarByClass(PlayerScoreExecutor.class);
        job.setMapperClass(PlayerScoreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(PlayerScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        return job.waitForCompletion(true);
    }
}
