package com.sportsdata.etl.services.loaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import com.sportsdata.etl.services.pipeline.EtlPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class S3DataLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(S3DataLoader.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
    
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;
    
    @Value("${etl.s3.bucket-name}")
    private String bucketName;
    
    @Value("${etl.s3.region:us-east-1}")
    private String region;
    
    @Value("${etl.s3.prefix:sports-data}")
    private String keyPrefix;
    
    public S3DataLoader() {
        this.s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("aws.region", "us-east-1")))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    public EtlPipeline.LoadResult loadAllData(EtlPipeline.TransformedData transformedData) {
        EtlPipeline.LoadResult result = new EtlPipeline.LoadResult();
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        
        try {
            logger.info("Starting S3 data loading process to bucket: {}", bucketName);
            
            // Load teams
            int teamsLoaded = loadTeamsToS3(transformedData.getTeams(), timestamp);
            result.setTeamsLoaded(teamsLoaded);
            
            // Load players
            int playersLoaded = loadPlayersToS3(transformedData.getPlayers(), timestamp);
            result.setPlayersLoaded(playersLoaded);
            
            // Load games
            int gamesLoaded = loadGamesToS3(transformedData.getGames(), timestamp);
            result.setGamesLoaded(gamesLoaded);
            
            // Create metadata file
            createMetadataFile(result, timestamp);
            
            result.setSuccess(true);
            
            logger.info("S3 data loading completed successfully: {} teams, {} players, {} games", 
                teamsLoaded, playersLoaded, gamesLoaded);
            
        } catch (Exception e) {
            logger.error("S3 data loading failed", e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
            throw new RuntimeException("S3 loading failed", e);
        }
        
        return result;
    }
    
    private int loadTeamsToS3(List<Team> teams, String timestamp) {
        if (teams == null || teams.isEmpty()) {
            logger.info("No teams to upload to S3");
            return 0;
        }
        
        try {
            logger.info("Uploading {} teams to S3", teams.size());
            
            String key = String.format("%s/%s/teams/teams-%s.json", keyPrefix, timestamp, timestamp);
            String jsonContent = objectMapper.writeValueAsString(teams);
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("application/json")
                    .build();
            
            s3Client.putObject(putRequest, RequestBody.fromString(jsonContent));
            
            logger.info("Successfully uploaded {} teams to S3 key: {}", teams.size(), key);
            return teams.size();
            
        } catch (Exception e) {
            logger.error("Failed to upload teams to S3", e);
            throw new RuntimeException("Teams S3 upload failed", e);
        }
    }
    
    private int loadPlayersToS3(List<Player> players, String timestamp) {
        if (players == null || players.isEmpty()) {
            logger.info("No players to upload to S3");
            return 0;
        }
        
        try {
            logger.info("Uploading {} players to S3", players.size());
            
            String key = String.format("%s/%s/players/players-%s.json", keyPrefix, timestamp, timestamp);
            String jsonContent = objectMapper.writeValueAsString(players);
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("application/json")
                    .build();
            
            s3Client.putObject(putRequest, RequestBody.fromString(jsonContent));
            
            logger.info("Successfully uploaded {} players to S3 key: {}", players.size(), key);
            return players.size();
            
        } catch (Exception e) {
            logger.error("Failed to upload players to S3", e);
            throw new RuntimeException("Players S3 upload failed", e);
        }
    }
    
    private int loadGamesToS3(List<Game> games, String timestamp) {
        if (games == null || games.isEmpty()) {
            logger.info("No games to upload to S3");
            return 0;
        }
        
        try {
            logger.info("Uploading {} games to S3", games.size());
            
            String key = String.format("%s/%s/games/games-%s.json", keyPrefix, timestamp, timestamp);
            String jsonContent = objectMapper.writeValueAsString(games);
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("application/json")
                    .build();
            
            s3Client.putObject(putRequest, RequestBody.fromString(jsonContent));
            
            logger.info("Successfully uploaded {} games to S3 key: {}", games.size(), key);
            return games.size();
            
        } catch (Exception e) {
            logger.error("Failed to upload games to S3", e);
            throw new RuntimeException("Games S3 upload failed", e);
        }
    }
    
    private void createMetadataFile(EtlPipeline.LoadResult result, String timestamp) {
        try {
            // Create metadata object
            Metadata metadata = new Metadata();
            metadata.setTimestamp(timestamp);
            metadata.setTeamsCount(result.getTeamsLoaded());
            metadata.setPlayersCount(result.getPlayersLoaded());
            metadata.setGamesCount(result.getGamesLoaded());
            metadata.setTotalRecords(result.getTeamsLoaded() + result.getPlayersLoaded() + result.getGamesLoaded());
            metadata.setLoadedAt(LocalDateTime.now());
            
            String key = String.format("%s/%s/metadata.json", keyPrefix, timestamp);
            String jsonContent = objectMapper.writeValueAsString(metadata);
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("application/json")
                    .build();
            
            s3Client.putObject(putRequest, RequestBody.fromString(jsonContent));
            
            logger.info("Successfully uploaded metadata to S3 key: {}", key);
            
        } catch (Exception e) {
            logger.warn("Failed to upload metadata to S3", e);
            // Don't fail the entire process for metadata upload failure
        }
    }
    
    public void loadTeamsOnly(List<Team> teams) {
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        loadTeamsToS3(teams, timestamp);
    }
    
    public void loadPlayersOnly(List<Player> players) {
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        loadPlayersToS3(players, timestamp);
    }
    
    public void loadGamesOnly(List<Game> games) {
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        loadGamesToS3(games, timestamp);
    }
    
    public boolean checkS3Connection() {
        try {
            s3Client.headBucket(builder -> builder.bucket(bucketName));
            return true;
        } catch (S3Exception e) {
            logger.error("S3 connection check failed for bucket: {}", bucketName, e);
            return false;
        }
    }
    
    // Metadata inner class
    public static class Metadata {
        private String timestamp;
        private int teamsCount;
        private int playersCount;
        private int gamesCount;
        private int totalRecords;
        private LocalDateTime loadedAt;
        
        // Getters and setters
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
        
        public int getTeamsCount() { return teamsCount; }
        public void setTeamsCount(int teamsCount) { this.teamsCount = teamsCount; }
        
        public int getPlayersCount() { return playersCount; }
        public void setPlayersCount(int playersCount) { this.playersCount = playersCount; }
        
        public int getGamesCount() { return gamesCount; }
        public void setGamesCount(int gamesCount) { this.gamesCount = gamesCount; }
        
        public int getTotalRecords() { return totalRecords; }
        public void setTotalRecords(int totalRecords) { this.totalRecords = totalRecords; }
        
        public LocalDateTime getLoadedAt() { return loadedAt; }
        public void setLoadedAt(LocalDateTime loadedAt) { this.loadedAt = loadedAt; }
    }
} 