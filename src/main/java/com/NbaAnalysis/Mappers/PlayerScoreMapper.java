package com.NbaAnalysis.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PlayerScoreMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable playerScore = new IntWritable();
    private int previousHomeScore = 0;
    private int previousVisitorScore = 0;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length < 25) {
            return; // Ensure valid input
        }
        try {

            String gameId = fields[2].trim();
            String playerName = fields[7].trim(); // PLAYER1_NAME
            String teamName = fields[11].trim();
            String scoreField = fields.length > 24 ? fields[24].trim() : ""; // SCORE (e.g., "12 - 8")        int pointsScored;
            String quarter = fields[5].trim();

            if (playerName != "" && scoreField.contains("-") && !teamName.isEmpty()) {   // Parse the scores
                String[] scores = scoreField.split(" - ");
                int homeScore = Integer.parseInt(scores[0].trim());
                int visitorScore = Integer.parseInt(scores[1].trim());

                // Calculate deltas
                int homeDelta = homeScore - previousHomeScore;
                int visitorDelta = visitorScore - previousVisitorScore;

                // Emit only positive deltas
                if (homeDelta > 0) {
                    playerScore.set(homeDelta);
                    context.write(new Text(gameId + '_' + teamName + '_' + playerName + '_' + quarter), playerScore);
                }

                if (visitorDelta > 0) {
                    playerScore.set(visitorDelta);
                    context.write(new Text(gameId + '_' + teamName + '_' + playerName + '_' + quarter), playerScore);
                }

                // Update previous scores
                previousHomeScore = homeScore;
                previousVisitorScore = visitorScore;

            }

        } catch (Exception e) {
            System.err.println("Error processing line: " + value.toString());
        }

    }
}
