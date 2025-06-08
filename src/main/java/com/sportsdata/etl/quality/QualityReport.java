package com.sportsdata.etl.quality;

import java.time.LocalDateTime;
import java.util.Map;

public class QualityReport {
    
    private LocalDateTime generatedAt;
    private long teamCount;
    private long playerCount;
    private long gameCount;
    private Map<String, Double> qualityMetrics;
    private double overallQualityScore;
    private String qualityStatus;
    
    public QualityReport() {}
    
    public QualityReport(LocalDateTime generatedAt, long teamCount, long playerCount, long gameCount,
                        Map<String, Double> qualityMetrics, double overallQualityScore, String qualityStatus) {
        this.generatedAt = generatedAt;
        this.teamCount = teamCount;
        this.playerCount = playerCount;
        this.gameCount = gameCount;
        this.qualityMetrics = qualityMetrics;
        this.overallQualityScore = overallQualityScore;
        this.qualityStatus = qualityStatus;
    }
    
    // Getters and Setters
    public LocalDateTime getGeneratedAt() {
        return generatedAt;
    }
    
    public void setGeneratedAt(LocalDateTime generatedAt) {
        this.generatedAt = generatedAt;
    }
    
    public long getTeamCount() {
        return teamCount;
    }
    
    public void setTeamCount(long teamCount) {
        this.teamCount = teamCount;
    }
    
    public long getPlayerCount() {
        return playerCount;
    }
    
    public void setPlayerCount(long playerCount) {
        this.playerCount = playerCount;
    }
    
    public long getGameCount() {
        return gameCount;
    }
    
    public void setGameCount(long gameCount) {
        this.gameCount = gameCount;
    }
    
    public Map<String, Double> getQualityMetrics() {
        return qualityMetrics;
    }
    
    public void setQualityMetrics(Map<String, Double> qualityMetrics) {
        this.qualityMetrics = qualityMetrics;
    }
    
    public double getOverallQualityScore() {
        return overallQualityScore;
    }
    
    public void setOverallQualityScore(double overallQualityScore) {
        this.overallQualityScore = overallQualityScore;
    }
    
    public String getQualityStatus() {
        return qualityStatus;
    }
    
    public void setQualityStatus(String qualityStatus) {
        this.qualityStatus = qualityStatus;
    }
    
    public long getTotalRecords() {
        return teamCount + playerCount + gameCount;
    }
    
    @Override
    public String toString() {
        return "QualityReport{" +
                "generatedAt=" + generatedAt +
                ", teamCount=" + teamCount +
                ", playerCount=" + playerCount +
                ", gameCount=" + gameCount +
                ", overallQualityScore=" + overallQualityScore +
                ", qualityStatus='" + qualityStatus + '\'' +
                '}';
    }
} 