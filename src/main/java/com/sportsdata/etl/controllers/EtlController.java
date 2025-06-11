package com.sportsdata.etl.controllers;

import com.sportsdata.etl.services.loaders.DatabaseLoader;
import com.sportsdata.etl.services.pipeline.EtlPipeline;
import com.sportsdata.etl.services.quality.DataQualityChecker;
import com.sportsdata.etl.services.quality.QualityReport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/etl")
@CrossOrigin(origins = "*")
public class EtlController {
    
    private static final Logger logger = LoggerFactory.getLogger(EtlController.class);
    
    @Autowired
    private EtlPipeline etlPipeline;
    
    @Autowired
    private DataQualityChecker qualityChecker;
    
    @Autowired
    private DatabaseLoader databaseLoader;
    
    @PostMapping("/execute")
    public ResponseEntity<EtlPipeline.PipelineResult> executePipeline(
            @RequestBody(required = false) EtlPipeline.PipelineConfig config) {
        
        logger.info("ETL pipeline execution requested via REST API");
        
        try {
            // Use default config if none provided
            if (config == null) {
                config = new EtlPipeline.PipelineConfig(
                    "src/main/resources/sample-data/teams.csv",
                    "src/main/resources/sample-data/players.json",
                    "src/main/resources/sample-data/games.xml"
                );
            }
            
            EtlPipeline.PipelineResult result = etlPipeline.executeFullPipeline(config);
            
            if (result.isSuccess()) {
                logger.info("ETL pipeline completed successfully via REST API: {}", result.getPipelineId());
                return ResponseEntity.ok(result);
            } else {
                logger.error("ETL pipeline failed via REST API: {}", result.getErrorMessage());
                return ResponseEntity.internalServerError().body(result);
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error during ETL pipeline execution", e);
            
            EtlPipeline.PipelineResult errorResult = new EtlPipeline.PipelineResult("ERROR", java.time.LocalDateTime.now());
            errorResult.setSuccess(false);
            errorResult.setErrorMessage(e.getMessage());
            errorResult.setEndTime(java.time.LocalDateTime.now());
            
            return ResponseEntity.internalServerError().body(errorResult);
        }
    }
    
    @GetMapping("/quality-report")
    public ResponseEntity<QualityReport> getQualityReport() {
        logger.info("Data quality report requested via REST API");
        
        try {
            QualityReport report = qualityChecker.generateQualityReport();
            return ResponseEntity.ok(report);
        } catch (Exception e) {
            logger.error("Error generating quality report", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getPipelineStatus() {
        logger.info("Pipeline status requested via REST API");
        
        try {
            Map<String, Object> status = new HashMap<>();
            
            // Get data counts
            long teamCount = databaseLoader.getTeamCount();
            long playerCount = databaseLoader.getPlayerCount();
            long gameCount = databaseLoader.getGameCount();
            
            status.put("status", "READY");
            status.put("database", Map.of(
                "teams", teamCount,
                "players", playerCount,
                "games", gameCount,
                "total", teamCount + playerCount + gameCount
            ));
            
            // Add system info
            status.put("system", Map.of(
                "javaVersion", System.getProperty("java.version"),
                "availableProcessors", Runtime.getRuntime().availableProcessors(),
                "maxMemory", Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB",
                "freeMemory", Runtime.getRuntime().freeMemory() / (1024 * 1024) + " MB"
            ));
            
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            logger.error("Error getting pipeline status", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    @DeleteMapping("/data")
    public ResponseEntity<Map<String, String>> clearAllData() {
        logger.warn("Data clearing requested via REST API");
        
        try {
            databaseLoader.clearAllData();
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("message", "All data cleared from database");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error clearing data", e);
            
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", e.getMessage());
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> health = new HashMap<>();
        health.put("status", "UP");
        health.put("application", "Sports Data ETL Pipeline");
        health.put("timestamp", java.time.LocalDateTime.now().toString());
        
        return ResponseEntity.ok(health);
    }
} 