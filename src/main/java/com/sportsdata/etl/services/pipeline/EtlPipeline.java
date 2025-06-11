package com.sportsdata.etl.services.pipeline;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import com.sportsdata.etl.services.extractors.CsvDataExtractor;
import com.sportsdata.etl.services.extractors.JsonApiExtractor;
import com.sportsdata.etl.services.extractors.XmlFeedExtractor;
import com.sportsdata.etl.services.loaders.S3DataLoader;
import com.sportsdata.etl.services.quality.S3QualityChecker;
import com.sportsdata.etl.services.quality.QualityReport;
import com.sportsdata.etl.services.transformers.DataCleaner;
import com.sportsdata.etl.services.transformers.DataStandardizer;
import com.sportsdata.etl.services.transformers.DataValidator;
import com.sportsdata.etl.utils.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class EtlPipeline {
    
    private static final Logger logger = LoggerFactory.getLogger(EtlPipeline.class);
    
    @Autowired
    private CsvDataExtractor csvExtractor;
    
    @Autowired
    private JsonApiExtractor jsonExtractor;
    
    @Autowired
    private XmlFeedExtractor xmlExtractor;
    
    @Autowired
    private DataValidator dataValidator;
    
    @Autowired
    private DataCleaner dataCleaner;
    
    @Autowired
    private DataStandardizer dataStandardizer;
    
    @Autowired
    private S3DataLoader s3DataLoader;
    
    @Autowired
    private S3QualityChecker qualityChecker;
    
    @Autowired
    private MetricsCollector metricsCollector;
    
    public PipelineResult executeFullPipeline(PipelineConfig config) {
        String pipelineId = UUID.randomUUID().toString();
        LocalDateTime startTime = LocalDateTime.now();
        
        logger.info("Starting ETL pipeline execution - Pipeline ID: {}", pipelineId);
        
        PipelineResult result = new PipelineResult(pipelineId, startTime);
        
        try {
            // Phase 1: Extraction
            logger.info("Phase 1: Starting data extraction");
            ExtractedData extractedData = extractData(config);
            result.setExtractedData(extractedData);
            metricsCollector.recordExtractionMetrics(extractedData);
            
            // Phase 2: Transformation and Validation
            logger.info("Phase 2: Starting data transformation and validation");
            TransformedData transformedData = transformAndValidateData(extractedData);
            result.setTransformedData(transformedData);
            metricsCollector.recordTransformationMetrics(transformedData);
            
            // Phase 3: Loading
            logger.info("Phase 3: Starting data loading");
            LoadResult loadResult = loadData(transformedData);
            result.setLoadResult(loadResult);
            metricsCollector.recordLoadMetrics(loadResult);
            
            // Phase 4: Quality Assessment
            logger.info("Phase 4: Running data quality assessment");
            QualityReport qualityReport = qualityChecker.generateQualityReport();
            result.setQualityReport(qualityReport);
            
            result.setEndTime(LocalDateTime.now());
            result.setSuccess(true);
            
            logger.info("ETL pipeline completed successfully - Pipeline ID: {}, Duration: {} ms", 
                pipelineId, result.getDurationMs());
            
        } catch (Exception e) {
            logger.error("ETL pipeline failed - Pipeline ID: {}", pipelineId, e);
            result.setEndTime(LocalDateTime.now());
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }
        
        return result;
    }
    
    private ExtractedData extractData(PipelineConfig config) {
        ExtractedData extractedData = new ExtractedData();
        
        // Extract teams from CSV
        if (config.getTeamsCsvPath() != null) {
            try {
                logger.info("Extracting teams from CSV: {}", config.getTeamsCsvPath());
                List<Team> teams = csvExtractor.extractTeams(config.getTeamsCsvPath());
                extractedData.setTeams(teams);
                logger.info("Extracted {} teams from CSV", teams.size());
            } catch (Exception e) {
                logger.error("Failed to extract teams from CSV: {}", config.getTeamsCsvPath(), e);
                throw new RuntimeException("Teams extraction failed", e);
            }
        }
        
        // Extract players from JSON
        if (config.getPlayersJsonPath() != null) {
            try {
                logger.info("Extracting players from JSON: {}", config.getPlayersJsonPath());
                List<Player> players = jsonExtractor.extractPlayers(config.getPlayersJsonPath());
                extractedData.setPlayers(players);
                logger.info("Extracted {} players from JSON", players.size());
            } catch (Exception e) {
                logger.error("Failed to extract players from JSON: {}", config.getPlayersJsonPath(), e);
                throw new RuntimeException("Players extraction failed", e);
            }
        }
        
        // Extract games from XML
        if (config.getGamesXmlPath() != null) {
            try {
                logger.info("Extracting games from XML: {}", config.getGamesXmlPath());
                List<Game> games = xmlExtractor.extractGames(config.getGamesXmlPath());
                extractedData.setGames(games);
                logger.info("Extracted {} games from XML", games.size());
            } catch (Exception e) {
                logger.error("Failed to extract games from XML: {}", config.getGamesXmlPath(), e);
                throw new RuntimeException("Games extraction failed", e);
            }
        }
        
        return extractedData;
    }
    
    private TransformedData transformAndValidateData(ExtractedData extractedData) {
        TransformedData transformedData = new TransformedData();
        
        // Transform and validate teams
        if (extractedData.getTeams() != null) {
            logger.info("Transforming and validating {} teams", extractedData.getTeams().size());
            List<Team> validatedTeams = dataValidator.validateTeams(extractedData.getTeams());
            List<Team> cleanedTeams = dataCleaner.cleanTeams(validatedTeams);
            List<Team> standardizedTeams = dataStandardizer.standardizeTeams(cleanedTeams);
            transformedData.setTeams(standardizedTeams);
            logger.info("Processed teams: {} -> {} valid", extractedData.getTeams().size(), standardizedTeams.size());
        }
        
        // Transform and validate players
        if (extractedData.getPlayers() != null) {
            logger.info("Transforming and validating {} players", extractedData.getPlayers().size());
            List<Player> validatedPlayers = dataValidator.validatePlayers(extractedData.getPlayers());
            List<Player> cleanedPlayers = dataCleaner.cleanPlayers(validatedPlayers);
            List<Player> standardizedPlayers = dataStandardizer.standardizePlayers(cleanedPlayers);
            transformedData.setPlayers(standardizedPlayers);
            logger.info("Processed players: {} -> {} valid", extractedData.getPlayers().size(), standardizedPlayers.size());
        }
        
        // Transform and validate games
        if (extractedData.getGames() != null) {
            logger.info("Transforming and validating {} games", extractedData.getGames().size());
            List<Game> validatedGames = dataValidator.validateGames(extractedData.getGames());
            List<Game> cleanedGames = dataCleaner.cleanGames(validatedGames);
            List<Game> standardizedGames = dataStandardizer.standardizeGames(cleanedGames);
            transformedData.setGames(standardizedGames);
            logger.info("Processed games: {} -> {} valid", extractedData.getGames().size(), standardizedGames.size());
        }
        
        return transformedData;
    }
    
    private LoadResult loadData(TransformedData transformedData) {
        return s3DataLoader.loadAllData(transformedData);
    }
    
    // Data Transfer Objects
    public static class PipelineConfig {
        private String teamsCsvPath;
        private String playersJsonPath;
        private String gamesXmlPath;
        
        public PipelineConfig() {}
        
        public PipelineConfig(String teamsCsvPath, String playersJsonPath, String gamesXmlPath) {
            this.teamsCsvPath = teamsCsvPath;
            this.playersJsonPath = playersJsonPath;
            this.gamesXmlPath = gamesXmlPath;
        }
        
        // Getters and setters
        public String getTeamsCsvPath() { return teamsCsvPath; }
        public void setTeamsCsvPath(String teamsCsvPath) { this.teamsCsvPath = teamsCsvPath; }
        
        public String getPlayersJsonPath() { return playersJsonPath; }
        public void setPlayersJsonPath(String playersJsonPath) { this.playersJsonPath = playersJsonPath; }
        
        public String getGamesXmlPath() { return gamesXmlPath; }
        public void setGamesXmlPath(String gamesXmlPath) { this.gamesXmlPath = gamesXmlPath; }
    }
    
    public static class ExtractedData {
        private List<Team> teams;
        private List<Player> players;
        private List<Game> games;
        
        // Getters and setters
        public List<Team> getTeams() { return teams; }
        public void setTeams(List<Team> teams) { this.teams = teams; }
        
        public List<Player> getPlayers() { return players; }
        public void setPlayers(List<Player> players) { this.players = players; }
        
        public List<Game> getGames() { return games; }
        public void setGames(List<Game> games) { this.games = games; }
    }
    
    public static class TransformedData {
        private List<Team> teams;
        private List<Player> players;
        private List<Game> games;
        
        // Getters and setters
        public List<Team> getTeams() { return teams; }
        public void setTeams(List<Team> teams) { this.teams = teams; }
        
        public List<Player> getPlayers() { return players; }
        public void setPlayers(List<Player> players) { this.players = players; }
        
        public List<Game> getGames() { return games; }
        public void setGames(List<Game> games) { this.games = games; }
    }
    
    public static class LoadResult {
        private int teamsLoaded;
        private int playersLoaded;
        private int gamesLoaded;
        private boolean success;
        private String errorMessage;
        
        // Getters and setters
        public int getTeamsLoaded() { return teamsLoaded; }
        public void setTeamsLoaded(int teamsLoaded) { this.teamsLoaded = teamsLoaded; }
        
        public int getPlayersLoaded() { return playersLoaded; }
        public void setPlayersLoaded(int playersLoaded) { this.playersLoaded = playersLoaded; }
        
        public int getGamesLoaded() { return gamesLoaded; }
        public void setGamesLoaded(int gamesLoaded) { this.gamesLoaded = gamesLoaded; }
        
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    }
    
    public static class PipelineResult {
        private final String pipelineId;
        private final LocalDateTime startTime;
        private LocalDateTime endTime;
        private boolean success;
        private String errorMessage;
        private ExtractedData extractedData;
        private TransformedData transformedData;
        private LoadResult loadResult;
        private QualityReport qualityReport;
        
        public PipelineResult(String pipelineId, LocalDateTime startTime) {
            this.pipelineId = pipelineId;
            this.startTime = startTime;
        }
        
        public long getDurationMs() {
            if (endTime != null) {
                return java.time.Duration.between(startTime, endTime).toMillis();
            }
            return 0;
        }
        
        // Getters and setters
        public String getPipelineId() { return pipelineId; }
        public LocalDateTime getStartTime() { return startTime; }
        public LocalDateTime getEndTime() { return endTime; }
        public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }
        
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public ExtractedData getExtractedData() { return extractedData; }
        public void setExtractedData(ExtractedData extractedData) { this.extractedData = extractedData; }
        
        public TransformedData getTransformedData() { return transformedData; }
        public void setTransformedData(TransformedData transformedData) { this.transformedData = transformedData; }
        
        public LoadResult getLoadResult() { return loadResult; }
        public void setLoadResult(LoadResult loadResult) { this.loadResult = loadResult; }
        
        public QualityReport getQualityReport() { return qualityReport; }
        public void setQualityReport(QualityReport qualityReport) { this.qualityReport = qualityReport; }
    }
} 