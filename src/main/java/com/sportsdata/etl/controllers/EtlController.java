package com.sportsdata.etl.controllers;

import com.sportsdata.etl.services.loaders.S3DataLoader;
import com.sportsdata.etl.services.pipeline.EtlPipeline;
import com.sportsdata.etl.services.quality.S3QualityChecker;
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
    private S3QualityChecker qualityChecker;
    
    @Autowired
    private S3DataLoader s3DataLoader;
    
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
            
            // Check S3 connection
            boolean s3Connected = s3DataLoader.checkS3Connection();
            QualityReport report = qualityChecker.generateQualityReport();
            
            status.put("status", s3Connected ? "READY" : "S3_CONNECTION_ERROR");
            status.put("s3Storage", Map.of(
                "connected", s3Connected,
                "teams", report.getTeamCount(),
                "players", report.getPlayerCount(),
                "games", report.getGameCount(),
                "total", report.getTeamCount() + report.getPlayerCount() + report.getGameCount(),
                "qualityScore", report.getOverallQualityScore(),
                "qualityStatus", report.getQualityStatus()
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
        logger.warn("Data clearing requested via REST API - Note: S3 data cannot be cleared via this endpoint");
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "INFO");
        response.put("message", "S3 data storage does not support clearing data via API. Please manage S3 objects directly through AWS console or CLI.");
        
        return ResponseEntity.ok(response);
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