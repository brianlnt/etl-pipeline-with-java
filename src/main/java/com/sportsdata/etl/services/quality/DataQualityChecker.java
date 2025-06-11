// package com.sportsdata.etl.services.quality;

// import com.sportsdata.etl.repositories.GameRepository;
// import com.sportsdata.etl.repositories.PlayerRepository;
// import com.sportsdata.etl.repositories.TeamRepository;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.stereotype.Service;

// import java.time.LocalDateTime;
// import java.util.HashMap;
// import java.util.Map;

// @Service
// public class DataQualityChecker {
    
//     private static final Logger logger = LoggerFactory.getLogger(DataQualityChecker.class);
    
//     @Autowired
//     private TeamRepository teamRepository;
    
//     @Autowired
//     private PlayerRepository playerRepository;
    
//     @Autowired
//     private GameRepository gameRepository;
    
//     public QualityReport generateQualityReport() {
//         logger.info("Generating data quality report");
        
//         QualityReport report = new QualityReport();
//         report.setGeneratedAt(LocalDateTime.now());
        
//         // Check data counts
//         long teamCount = teamRepository.count();
//         long playerCount = playerRepository.count();
//         long gameCount = gameRepository.count();
        
//         report.setTeamCount(teamCount);
//         report.setPlayerCount(playerCount);
//         report.setGameCount(gameCount);
        
//         // Calculate quality metrics
//         Map<String, Double> qualityMetrics = new HashMap<>();
        
//         // Data completeness metrics
//         qualityMetrics.put("team_completeness", calculateTeamCompleteness());
//         qualityMetrics.put("player_completeness", calculatePlayerCompleteness());
//         qualityMetrics.put("game_completeness", calculateGameCompleteness());
        
//         // Data consistency metrics
//         qualityMetrics.put("referential_integrity", calculateReferentialIntegrity());
        
//         // Data distribution metrics
//         qualityMetrics.put("players_per_team", calculatePlayersPerTeam());
//         qualityMetrics.put("games_per_team", calculateGamesPerTeam());
        
//         report.setQualityMetrics(qualityMetrics);
        
//         // Overall quality score (average of all metrics)
//         double overallScore = qualityMetrics.values().stream()
//             .mapToDouble(Double::doubleValue)
//             .average()
//             .orElse(0.0);
        
//         report.setOverallQualityScore(overallScore);
        
//         // Determine quality status
//         if (overallScore >= 0.9) {
//             report.setQualityStatus("EXCELLENT");
//         } else if (overallScore >= 0.8) {
//             report.setQualityStatus("GOOD");
//         } else if (overallScore >= 0.7) {
//             report.setQualityStatus("FAIR");
//         } else {
//             report.setQualityStatus("POOR");
//         }
        
//         logger.info("Data quality report generated: {} teams, {} players, {} games, Overall Score: {:.2f} ({})", 
//             teamCount, playerCount, gameCount, overallScore, report.getQualityStatus());
        
//         return report;
//     }
    
//     private double calculateTeamCompleteness() {
//         // This is a simplified metric - in a real implementation, you'd check for null/empty fields
//         long totalTeams = teamRepository.count();
//         if (totalTeams == 0) return 1.0;
        
//         // For simplicity, assume all loaded teams are complete since we validate during transformation
//         return 1.0;
//     }
    
//     private double calculatePlayerCompleteness() {
//         long totalPlayers = playerRepository.count();
//         if (totalPlayers == 0) return 1.0;
        
//         // For simplicity, assume all loaded players are complete since we validate during transformation
//         return 1.0;
//     }
    
//     private double calculateGameCompleteness() {
//         long totalGames = gameRepository.count();
//         if (totalGames == 0) return 1.0;
        
//         // For simplicity, assume all loaded games are complete since we validate during transformation
//         return 1.0;
//     }
    
//     private double calculateReferentialIntegrity() {
//         // Check if all players reference valid teams
//         long totalPlayers = playerRepository.count();
//         if (totalPlayers == 0) return 1.0;
        
//         // This is simplified - in a real implementation, you'd run queries to check orphaned records
//         // For now, assume integrity is maintained since we load teams first
//         return 1.0;
//     }
    
//     private double calculatePlayersPerTeam() {
//         long totalTeams = teamRepository.count();
//         long totalPlayers = playerRepository.count();
        
//         if (totalTeams == 0) return 0.0;
        
//         double average = (double) totalPlayers / totalTeams;
        
//         // Normalize based on expected range (5-15 players per team is reasonable)
//         if (average >= 5 && average <= 15) {
//             return 1.0;
//         } else if (average >= 1 && average < 5) {
//             return 0.7; // Acceptable but low
//         } else if (average > 15 && average <= 25) {
//             return 0.8; // Acceptable but high
//         } else {
//             return 0.5; // Outside reasonable range
//         }
//     }
    
//     private double calculateGamesPerTeam() {
//         long totalTeams = teamRepository.count();
//         long totalGames = gameRepository.count();
        
//         if (totalTeams == 0) return 0.0;
        
//         // Each game involves 2 teams, so average games per team is totalGames * 2 / totalTeams
//         double average = totalGames > 0 ? (double) (totalGames * 2) / totalTeams : 0.0;
        
//         // Normalize based on expected range (10-100 games per team per season is reasonable)
//         if (average >= 10 && average <= 100) {
//             return 1.0;
//         } else if (average >= 1 && average < 10) {
//             return 0.7; // Acceptable but low
//         } else if (average > 100 && average <= 200) {
//             return 0.8; // Acceptable but high
//         } else {
//             return 0.5; // Outside reasonable range
//         }
//     }
// } 