package com.sportsdata.etl.utils;

import com.sportsdata.etl.pipeline.EtlPipeline;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MetricsCollector {
    
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final MeterRegistry meterRegistry;
    
    // Counters for tracking record counts
    private final Counter teamsExtractedCounter;
    private final Counter playersExtractedCounter;
    private final Counter gamesExtractedCounter;
    
    private final Counter teamsLoadedCounter;
    private final Counter playersLoadedCounter;
    private final Counter gamesLoadedCounter;
    
    // Timers for performance tracking
    private final Timer extractionTimer;
    private final Timer transformationTimer;
    private final Timer loadTimer;
    
    // Custom metrics
    private final ConcurrentHashMap<String, AtomicLong> customMetrics = new ConcurrentHashMap<>();
    
    @Autowired
    public MetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize counters
        this.teamsExtractedCounter = Counter.builder("etl.teams.extracted")
            .description("Number of teams extracted")
            .register(meterRegistry);
            
        this.playersExtractedCounter = Counter.builder("etl.players.extracted")
            .description("Number of players extracted")
            .register(meterRegistry);
            
        this.gamesExtractedCounter = Counter.builder("etl.games.extracted")
            .description("Number of games extracted")
            .register(meterRegistry);
            
        this.teamsLoadedCounter = Counter.builder("etl.teams.loaded")
            .description("Number of teams loaded")
            .register(meterRegistry);
            
        this.playersLoadedCounter = Counter.builder("etl.players.loaded")
            .description("Number of players loaded")
            .register(meterRegistry);
            
        this.gamesLoadedCounter = Counter.builder("etl.games.loaded")
            .description("Number of games loaded")
            .register(meterRegistry);
        
        // Initialize timers
        this.extractionTimer = Timer.builder("etl.extraction.duration")
            .description("Time taken for data extraction")
            .register(meterRegistry);
            
        this.transformationTimer = Timer.builder("etl.transformation.duration")
            .description("Time taken for data transformation")
            .register(meterRegistry);
            
        this.loadTimer = Timer.builder("etl.load.duration")
            .description("Time taken for data loading")
            .register(meterRegistry);
    }
    
    public void recordExtractionMetrics(EtlPipeline.ExtractedData extractedData) {
        if (extractedData == null) {
            return;
        }
        
        if (extractedData.getTeams() != null) {
            int teamCount = extractedData.getTeams().size();
            teamsExtractedCounter.increment(teamCount);
            logger.debug("Recorded extraction metrics: {} teams", teamCount);
        }
        
        if (extractedData.getPlayers() != null) {
            int playerCount = extractedData.getPlayers().size();
            playersExtractedCounter.increment(playerCount);
            logger.debug("Recorded extraction metrics: {} players", playerCount);
        }
        
        if (extractedData.getGames() != null) {
            int gameCount = extractedData.getGames().size();
            gamesExtractedCounter.increment(gameCount);
            logger.debug("Recorded extraction metrics: {} games", gameCount);
        }
    }
    
    public void recordTransformationMetrics(EtlPipeline.TransformedData transformedData) {
        if (transformedData == null) {
            return;
        }
        
        // Record transformation quality metrics
        recordCustomMetric("transformation.teams.processed", 
            transformedData.getTeams() != null ? transformedData.getTeams().size() : 0);
        recordCustomMetric("transformation.players.processed", 
            transformedData.getPlayers() != null ? transformedData.getPlayers().size() : 0);
        recordCustomMetric("transformation.games.processed", 
            transformedData.getGames() != null ? transformedData.getGames().size() : 0);
        
        logger.debug("Recorded transformation metrics");
    }
    
    public void recordLoadMetrics(EtlPipeline.LoadResult loadResult) {
        if (loadResult == null) {
            return;
        }
        
        teamsLoadedCounter.increment(loadResult.getTeamsLoaded());
        playersLoadedCounter.increment(loadResult.getPlayersLoaded());
        gamesLoadedCounter.increment(loadResult.getGamesLoaded());
        
        // Record success/failure metrics
        if (loadResult.isSuccess()) {
            recordCustomMetric("load.success", 1);
        } else {
            recordCustomMetric("load.failure", 1);
        }
        
        logger.debug("Recorded load metrics: {} teams, {} players, {} games loaded", 
            loadResult.getTeamsLoaded(), loadResult.getPlayersLoaded(), loadResult.getGamesLoaded());
    }
    
    public Timer.Sample startExtractionTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopExtractionTimer(Timer.Sample sample) {
        sample.stop(extractionTimer);
    }
    
    public Timer.Sample startTransformationTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopTransformationTimer(Timer.Sample sample) {
        sample.stop(transformationTimer);
    }
    
    public Timer.Sample startLoadTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopLoadTimer(Timer.Sample sample) {
        sample.stop(loadTimer);
    }
    
    public void recordCustomMetric(String metricName, long value) {
        customMetrics.computeIfAbsent(metricName, k -> {
            AtomicLong counter = new AtomicLong(0);
            meterRegistry.gauge("etl.custom." + k, counter, AtomicLong::get);
            return counter;
        }).set(value);
    }
    
    public void incrementCustomMetric(String metricName) {
        customMetrics.computeIfAbsent(metricName, k -> {
            AtomicLong counter = new AtomicLong(0);
            meterRegistry.gauge("etl.custom." + k, counter, AtomicLong::get);
            return counter;
        }).incrementAndGet();
    }
    
    public void recordPipelineExecution(String pipelineId, LocalDateTime startTime, LocalDateTime endTime, boolean success) {
        Duration duration = Duration.between(startTime, endTime);
        
        Timer pipelineTimer = Timer.builder("etl.pipeline.duration")
            .description("Total pipeline execution time")
            .tag("success", String.valueOf(success))
            .register(meterRegistry);
            
        pipelineTimer.record(duration);
        
        if (success) {
            incrementCustomMetric("pipeline.success");
        } else {
            incrementCustomMetric("pipeline.failure");
        }
        
        logger.info("Recorded pipeline execution metrics: {} ({}ms, success={})", 
            pipelineId, duration.toMillis(), success);
    }
    
    public void recordDataQualityScore(double qualityScore) {
        recordCustomMetric("data.quality.score", Math.round(qualityScore * 100));
    }
    
    public long getCustomMetric(String metricName) {
        AtomicLong metric = customMetrics.get(metricName);
        return metric != null ? metric.get() : 0L;
    }
    
    public void resetCustomMetrics() {
        customMetrics.clear();
        logger.info("Custom metrics reset");
    }
} 