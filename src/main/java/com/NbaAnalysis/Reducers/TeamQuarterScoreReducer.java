package com.NbaAnalysis.Reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TeamQuarterScoreReducer extends Reducer<Text, IntWritable, Text,Text> {
   
    
    // private final IntWritable result = new IntWritable();
    private final Map<String, QuarterScore> teamMaxScores = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    
        // int totalScore = 0;

        // // Sum up the scores for the given (team, quarter) key
        // for (IntWritable value : values) {
        //     totalScore += value.get();
        // }


        
        // // Emit the team-quarter and total score
        // result.set(totalScore);
        // context.write(key, result);
    // Parse the key into team and quarter
//     String[] keyParts = key.toString().split("_Q");
//     String team = keyParts[0];
//     String quarter = keyParts[1];

//     System.out.println(team +"    =============trewwertpoiuytr");
//     System.out.println(quarter +"    =============trewwertpoiuytr");

//     // Sum the scores for this team_quarter
//     int totalScore = 0;
//     for (IntWritable value : values) {
//         totalScore += value.get();
//     }
//     System.out.println(totalScore +"    =============trewwertpoiuytr");


//       // Use a static map to store team -> (maxQuarter -> maxScore)
//     Map<String, QuarterScore> teamMaxScores = new HashMap<>();

//     // If the team already exists, check if this quarter has a higher score
//     if (teamMaxScores.containsKey(team)) {
//         QuarterScore currentMax = teamMaxScores.get(team);
//         if (totalScore > currentMax.maxScore) {
//             teamMaxScores.put(team, new QuarterScore(quarter, totalScore));
//         }
//     } else {
//         // Otherwise, add this quarter as the max for this team
//         teamMaxScores.put(team, new QuarterScore(quarter, totalScore));
//     }

//     // Emit the results for each team
//     for (Map.Entry<String, QuarterScore> entry : teamMaxScores.entrySet()) {
//         String resultTeam = entry.getKey();
//         QuarterScore maxScoreDetails = entry.getValue();
//         String output = maxScoreDetails.quarter + " = " + maxScoreDetails.maxScore;
//         context.write(new Text(resultTeam), new Text(output));
//     }
// }

// static class QuarterScore {
//     String quarter;
//     int maxScore;

//     QuarterScore(String quarter, int maxScore) {
//         this.quarter = quarter;
//         this.maxScore = maxScore;
//     }


// Map to store the highest-scoring quarter for each team

  // Parse the key into team and quarter
    String[] keyParts = key.toString().split("_");
    String team = keyParts[0];
    String quarter = keyParts[1];

    // Sum the scores for this team_quarter
    int totalScore = 0;
    for (IntWritable value : values) {
        totalScore += value.get();
    }

    // Update the maximum score for the team if this quarter's score is higher
    if (teamMaxScores.containsKey(team)) {
        QuarterScore currentMax = teamMaxScores.get(team);
        if (totalScore > currentMax.maxScore) {
            teamMaxScores.put(team, new QuarterScore(quarter, totalScore));
        }
    } else {
        // Add the first record for the team
        teamMaxScores.put(team, new QuarterScore(quarter, totalScore));
    }
}

@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
    // Emit the results for each team in the cleanup phase
    for (Map.Entry<String, QuarterScore> entry : teamMaxScores.entrySet()) {
        String team = entry.getKey();
        QuarterScore maxScoreDetails = entry.getValue();
        String output = team + " scored most of the points in the " + formatQuarter(maxScoreDetails.quarter) + " quarter";
        context.write(new Text(team+maxScoreDetails.maxScore), new Text(output));
    }
}

// Helper method to format quarter (e.g., "Q1" -> "1st")
private String formatQuarter(String quarter) {
    switch (quarter) {
        case "Q1": return "1st";
        case "Q2": return "2nd";
        case "Q3": return "3rd";
        case "Q4": return "4th";
        default: return quarter; // Fallback in case of unexpected input
    }
}

// Helper class to store max quarter and max score for a team
static class QuarterScore {
    String quarter;
    int maxScore;

    QuarterScore(String quarter, int maxScore) {
        this.quarter = quarter;
        this.maxScore = maxScore;
    }

    
}


}
