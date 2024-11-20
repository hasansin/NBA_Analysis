package com.NbaAnalysis.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostScoredPlayerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text playerKey = new Text();
    private IntWritable playerScore = new IntWritable();
    private int previousHomeScore = 0;
    private int previousVisitorScore = 0;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        try {
            if (fields.length > 25) {
                // Extract relevant fields
                String player = fields.length > 7 ? fields[7] :""; // PLAYER1_NAME
                String score = fields.length > 24 ? fields[24].trim() : ""; // SCORE (e.g., "12 - 8")

                if (player != "" && score.contains("-")) {
                    // Parse the scores
                    String[] scores = score.split(" - ");
                    int homeScore = Integer.parseInt(scores[0].trim());
                    int visitorScore = Integer.parseInt(scores[1].trim());

                    // Calculate deltas
                    int homeDelta = homeScore - previousHomeScore;
                    int visitorDelta = visitorScore - previousVisitorScore;

                    // Emit only positive deltas
                    if (homeDelta > 0) {
                        playerKey.set(player);
                        playerScore.set(homeDelta);
                        context.write(playerKey, playerScore);
                    }

                    if (visitorDelta > 0) {
                        playerKey.set(player);
                        playerScore.set(visitorDelta);
                        context.write(playerKey, playerScore);
                    }

                    // Update previous scores
                    previousHomeScore = homeScore;
                    previousVisitorScore = visitorScore;
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing line: " + value.toString());
        }
    }

}
