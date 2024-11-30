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
        String[] fields = value.toString().split(",");
        if (fields.length < 25)
            return;

        try {
            String scoreField = fields.length > 24 ? fields[24].trim() : ""; // SCORE (e.g., "12 - 8")
            String team = fields[11]; // PLAYER1_TEAM_ABBREVIATION    
            String quarter = fields.length > 6 ? fields[5] : "";

            if (quarter != "" && scoreField != "") {

            // Parse the scores
            String[] scores = scoreField.split("-");
            int homeScore = Integer.parseInt(scores[0].trim());
            int visitorScore = Integer.parseInt(scores[1].trim());

            System.out.println("Home Team: =======" + team);
            System.out.println("visitorScore:========== " + visitorScore);
            System.out.println("homeScore:========== " + homeScore);
            int homeScoreDiff = homeScore - previousHomeScore;
            int visitorScoreDiff = visitorScore - previousVisitorScore;

            System.out.println("homeScoreDiff:========= " + homeScoreDiff);
            System.out.println("visitorScoreDiff:========= " + visitorScoreDiff);

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
        }
        } catch (Exception e) {
            // Handle malformed rows gracefully
            System.err.println("Error processing line: " + value.toString());
        }
    }
}
