package com.NbaAnalysis.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.NbaAnalysis.Mappers.TeamQuarterScoreMapper;
import com.NbaAnalysis.Reducers.TeamQuarterScoreReducer;
// import com.NbaAnalysis.Reducers.MaxQuaterScoreTeamReducer;

public class TeamQuarterScoreExecutor {
    public static boolean runJob(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length < 3) {
            System.err.println(
                    "Usage: Main <input path> <output path for team quarters> <output path for player scores>");
            System.exit(-1);
        }
        String inputPath = otherArgs[0];
        String outputPath = args[1];
        Job job = Job.getInstance(conf, "Highest Scoring Quarter per Team");

        job.setJarByClass(TeamQuarterScoreExecutor.class);
        job.setMapperClass(TeamQuarterScoreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TeamQuarterScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileInputFormat.addInputPath(job, new Path(inputPath));

        return job.waitForCompletion(true);
    }
}
