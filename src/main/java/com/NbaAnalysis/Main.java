package com.NbaAnalysis;

import com.NbaAnalysis.Executors.MostScoreExecutor;
import com.NbaAnalysis.Executors.TeamQuarterScoreExecutor;
import com.NbaAnalysis.Executors.TeamScoreExecutor;

// import com.NbaAnalysis.Executors.MostScoreExecutor;
// import com.NbaAnalysis.Executors.QuarterScoreExecutor;

/**
 * app.java
 *
 */
public class Main {
    public static void main(String[] args) throws Exception {

        // Run the job for highest-scoring quarters per team
        boolean baseScoreJobSuccess = TeamQuarterScoreExecutor.runJob(args);
        if (!baseScoreJobSuccess) {
            System.err.println("Failed to complete Team Quarter Job");
            System.exit(1);
        }
        System.out.println("Team Quarter Job Completed Successfully");

        // Run the job for highest-scoring player
        boolean mostScoreJobSuccess = MostScoreExecutor.runJob(args);
        if (!mostScoreJobSuccess) {
            System.err.println("Failed to complete Player Score Job");
            System.exit(1);
        }
        System.out.println("Player Score Job Completed Successfully");

        // // Run the job for Team Score for hive query
        boolean teamScoreSuccess = TeamScoreExecutor.runJob(args);
        if (!teamScoreSuccess) {
            System.out.println("Failed to complete Team Score Job");
            System.exit(1);

        }
        System.out.println("Team Score Job Completed Successfully");

        System.exit(0); // Success
    }

}
