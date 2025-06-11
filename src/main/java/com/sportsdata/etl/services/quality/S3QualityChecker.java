package com.sportsdata.etl.services.quality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sportsdata.etl.services.loaders.S3DataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class S3QualityChecker {
    
    private static final Logger logger = LoggerFactory.getLogger(S3QualityChecker.class);
    
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;
    
    @Value("${etl.s3.bucket-name}")
    private String bucketName;
    
    @Value("${etl.s3.prefix:sports-data}")
    private String keyPrefix;
    
    public S3QualityChecker() {
        this.s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("aws.region", "us-east-1")))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    public QualityReport generateQualityReport() {
        logger.info("Generating S3-based data quality report");
        
        QualityReport report = new QualityReport();
        report.setGeneratedAt(LocalDateTime.now());
        
        try {
            // Get latest data from S3
            String latestDataPath = findLatestDataPath();
            
            if (latestDataPath == null) {
                logger.warn("No data found in S3 bucket: {}", bucketName);
                return createEmptyReport();
            }
            
            // Read metadata file to get counts
            S3DataLoader.Metadata metadata = readMetadata(latestDataPath);
            
            if (metadata != null) {
                report.setTeamCount(metadata.getTeamsCount());
                report.setPlayerCount(metadata.getPlayersCount());
                report.setGameCount(metadata.getGamesCount());
            } else {
                logger.warn("Could not read metadata, using object count fallback");
                setCountsFromObjectListing(report, latestDataPath);
            }
            
            // Calculate quality metrics
            Map<String, Double> qualityMetrics = calculateQualityMetrics(report);
            report.setQualityMetrics(qualityMetrics);
            
            // Overall quality score (average of all metrics)
            double overallScore = qualityMetrics.values().stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0.0);
            
            report.setOverallQualityScore(overallScore);
            
            // Determine quality status
            if (overallScore >= 0.9) {
                report.setQualityStatus("EXCELLENT");
            } else if (overallScore >= 0.8) {
                report.setQualityStatus("GOOD");
            } else if (overallScore >= 0.7) {
                report.setQualityStatus("FAIR");
            } else {
                report.setQualityStatus("POOR");
            }
            
            logger.info("S3 data quality report generated: {} teams, {} players, {} games, Overall Score: {:.2f} ({})",
                    report.getTeamCount(), report.getPlayerCount(), report.getGameCount(),
                    overallScore, report.getQualityStatus());
            
        } catch (Exception e) {
            logger.error("Error generating S3 quality report", e);
            return createErrorReport(e.getMessage());
        }
        
        return report;
    }
    
    private String findLatestDataPath() {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(keyPrefix + "/")
                    .delimiter("/")
                    .build();
            
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            
            // Find the most recent data directory
            String latestPath = null;
            for (var commonPrefix : listResponse.commonPrefixes()) {
                String prefixStr = commonPrefix.prefix();
                if (latestPath == null || prefixStr.compareTo(latestPath) > 0) {
                    latestPath = prefixStr;
                }
            }
            
            return latestPath != null ? latestPath.replaceAll("/$", "") : null;
            
        } catch (Exception e) {
            logger.error("Error finding latest data path in S3", e);
            return null;
        }
    }
    
    private S3DataLoader.Metadata readMetadata(String dataPath) {
        try {
            String metadataKey = dataPath + "/metadata.json";
            
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(metadataKey)
                    .build();
            
            String content = s3Client.getObjectAsBytes(getRequest).asUtf8String();
            return objectMapper.readValue(content, S3DataLoader.Metadata.class);
            
        } catch (Exception e) {
            logger.warn("Could not read metadata from S3: {}", e.getMessage());
            return null;
        }
    }
    
    private void setCountsFromObjectListing(QualityReport report, String dataPath) {
        try {
            int teamCount = countObjectsInPrefix(dataPath + "/teams/");
            int playerCount = countObjectsInPrefix(dataPath + "/players/");
            int gameCount = countObjectsInPrefix(dataPath + "/games/");
            
            report.setTeamCount(teamCount);
            report.setPlayerCount(playerCount);
            report.setGameCount(gameCount);
            
        } catch (Exception e) {
            logger.error("Error counting objects from S3 listing", e);
            report.setTeamCount(0);
            report.setPlayerCount(0);
            report.setGameCount(0);
        }
    }
    
    private int countObjectsInPrefix(String prefix) {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();
            
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            return listResponse.contents().size();
            
        } catch (Exception e) {
            logger.warn("Could not count objects in prefix: {}", prefix);
            return 0;
        }
    }
    
    private Map<String, Double> calculateQualityMetrics(QualityReport report) {
        Map<String, Double> metrics = new HashMap<>();
        
        // Data completeness metrics - for S3, we assume data is complete if it exists
        metrics.put("data_availability", report.getTeamCount() > 0 && 
                                       report.getPlayerCount() > 0 && 
                                       report.getGameCount() > 0 ? 1.0 : 0.0);
        
        // Data distribution metrics
        if (report.getTeamCount() > 0) {
            double playersPerTeam = (double) report.getPlayerCount() / report.getTeamCount();
            double gamesPerTeam = (double) report.getGameCount() / report.getTeamCount();
            
            // Reasonable ratios (adjustable based on business logic)
            metrics.put("players_per_team_ratio", Math.min(playersPerTeam / 15.0, 1.0)); // Assume ~15 players per team is good
            metrics.put("games_per_team_ratio", Math.min(gamesPerTeam / 50.0, 1.0)); // Assume ~50 games per team is good
        } else {
            metrics.put("players_per_team_ratio", 0.0);
            metrics.put("games_per_team_ratio", 0.0);
        }
        
        // Data freshness - check if data was uploaded recently
        metrics.put("data_freshness", 1.0); // For now, assume S3 data is fresh
        
        return metrics;
    }
    
    private QualityReport createEmptyReport() {
        QualityReport report = new QualityReport();
        report.setGeneratedAt(LocalDateTime.now());
        report.setTeamCount(0);
        report.setPlayerCount(0);
        report.setGameCount(0);
        report.setQualityMetrics(new HashMap<>());
        report.setOverallQualityScore(0.0);
        report.setQualityStatus("NO_DATA");
        return report;
    }
    
    private QualityReport createErrorReport(String errorMessage) {
        QualityReport report = new QualityReport();
        report.setGeneratedAt(LocalDateTime.now());
        report.setTeamCount(0);
        report.setPlayerCount(0);
        report.setGameCount(0);
        report.setQualityMetrics(new HashMap<>());
        report.setOverallQualityScore(0.0);
        report.setQualityStatus("ERROR: " + errorMessage);
        return report;
    }
    
    public boolean checkS3Connection() {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucketName));
            return true;
        } catch (Exception e) {
            logger.error("S3 connection check failed for bucket: {}", bucketName, e);
            return false;
        }
    }
} 