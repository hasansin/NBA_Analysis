package com.NbaAnalysis.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TeamQuarterScoreMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final Text teamQuarterKey = new Text();
    private final IntWritable scoreValue = new IntWritable();
    private int previousHomeScore = 0;
    private int previousVisitorScore = 0;
    private String previousGameID = "";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length > 25) {
            try {
                // Extract relevant fields
                String gameId = fields[2];
                String homeTeam = fields[11]; // PLAYER1_TEAM_ABBREVIATION (home team)
                String period = fields.length > 6 ? fields[5] : ""; // PERIOD (quarter)
                String score = fields.length > 24 ? fields[24].trim() : ""; // SCORE (e.g., "12 - 8")
                 
                if (!previousGameID.equals(gameId)) {
                    previousHomeScore =  0;
                    previousVisitorScore = 0;
                } 
                if (period != "" && score != "") {
                    // Parse the scores
                    String[] scoreParts = score.split(" - ");
                    System.out.println("Home Team: =======" + homeTeam);
                    System.out.println("Period:========== " + period);
                    System.out.println("Score:========= " + score);

                    if (scoreParts.length == 2 ) {
                        int homeScore = Integer.parseInt(scoreParts[0].trim());
                        int visitorScore = Integer.parseInt(scoreParts[1].trim());

                        int homeScoreDiff = homeScore - previousHomeScore;
                        int visitorScoreDiff = visitorScore - previousVisitorScore;

                        System.out.println("asdfgjkgf))))))))++++++++++___________");
                        System.out.println("GMAE ID ======="+gameId);
                        System.out.println("PRE GMA ========="+previousGameID);
                        System.out.println("COMARE GMAES ========"+ (!previousGameID.equals(gameId)));
                        System.out.println("sHcore Team: =======" + homeScore);
                        System.out.println("PRE sHcore Team: =======" + previousHomeScore);
                        System.out.println("sVcore Team: =======" + visitorScore);
                        System.out.println("PRE sVcore Team: =======" + previousVisitorScore);

                        System.out.println("Home Team: =======" + homeScoreDiff);
                        System.out.println("Visitor Team: ========" + visitorScoreDiff);
                        System.out.println("Period:========== " + period);
                        System.out.println("Score:========= " + score);
                        // Emit home team score for the quarter
                        if (!homeTeam.isEmpty() && homeScoreDiff > 0) {
                            teamQuarterKey.set(homeTeam + "_Q" + period);
                            scoreValue.set(homeScoreDiff);
                            context.write(teamQuarterKey, scoreValue);
                        }

                        // Emit visitor team score for the quarter
                        if (!homeTeam.isEmpty() && visitorScoreDiff > 0) {
                            teamQuarterKey.set(homeTeam + "_Q" + period);
                            scoreValue.set(visitorScoreDiff);
                            context.write(teamQuarterKey, scoreValue);
                        }
                        System.err.println("Mapper emitting key: " + teamQuarterKey + ", value: " + scoreValue.get());

                        // Update previous scores
                        previousHomeScore = homeScore;
                        previousVisitorScore = visitorScore;
                        previousGameID = gameId;

                    }
                }
            } catch (Exception e) {
                // Handle malformed rows gracefully
                System.err.println("Error processing line: " + value.toString());
            }
        } else {
            System.err.println("Error processing line: " + value.toString());

        }
    }

}
