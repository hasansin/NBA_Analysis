package com.NbaAnalysis.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TeamScoreMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text teamQuarterKey = new Text();
    private IntWritable score = new IntWritable();
    private int previousHomeScore = 0;
    private int previousVisitorScore = 0;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t"); // Adjust delimiter if necessary
        if (fields.length < 25)
            return;

        try {
            String scoreField = fields[24];
            String team = fields[8]; // PLAYER1_TEAM_ABBREVIATION
            int quarter = Integer.parseInt(fields[5]);
            if (scoreField == null || scoreField.isEmpty() || !scoreField.contains("-")) {
                return;
            }

            // Parse the scores
            String[] scores = scoreField.split("-");
            int homeScore = Integer.parseInt(scores[0].trim());
            int visitorScore = Integer.parseInt(scores[1].trim());

            int homeScoreDiff = homeScore - previousHomeScore;
            int visitorScoreDiff = visitorScore - previousVisitorScore;

            if (!team.isEmpty() && homeScoreDiff > 0) {
                teamQuarterKey.set(team + "_Q" + quarter);
                score.set(homeScoreDiff);
                context.write(teamQuarterKey, score);
            }

            // Emit visitor team score for the quarter
            if (!team.isEmpty() && visitorScoreDiff > 0) {
                teamQuarterKey.set(team + "_Q" + quarter);
                score.set(visitorScoreDiff);
                context.write(teamQuarterKey, score);
            }

            previousHomeScore = homeScore;
            previousVisitorScore = visitorScore;
        } catch (Exception e) {
            // Handle malformed rows gracefully
            System.err.println("Error processing line: " + value.toString());
        }
    }
}
