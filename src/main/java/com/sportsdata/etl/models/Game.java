package com.sportsdata.etl.models;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Table(name = "games")
public class Game {
    
    @Id
    @Column(name = "game_id")
    private String gameId;
    
    @NotBlank
    @Column(name = "home_team_id", nullable = false)
    private String homeTeamId;
    
    @NotBlank
    @Column(name = "away_team_id", nullable = false)
    private String awayTeamId;
    
    @NotNull
    @Column(name = "date", nullable = false)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime date;
    
    @PositiveOrZero
    @Column(name = "home_score")
    private Integer homeScore;
    
    @PositiveOrZero
    @Column(name = "away_score")
    private Integer awayScore;
    
    @NotBlank
    @Column(name = "status", nullable = false)
    private String status;
    
    // Constructors
    public Game() {}
    
    public Game(String gameId, String homeTeamId, String awayTeamId, LocalDateTime date, 
                Integer homeScore, Integer awayScore, String status) {
        this.gameId = gameId;
        this.homeTeamId = homeTeamId;
        this.awayTeamId = awayTeamId;
        this.date = date;
        this.homeScore = homeScore;
        this.awayScore = awayScore;
        this.status = status;
    }
    
    public String getGameId() {
        return gameId;
    }
    
    public void setGameId(String gameId) {
        this.gameId = gameId;
    }
    
    public String getHomeTeamId() {
        return homeTeamId;
    }
    
    public void setHomeTeamId(String homeTeamId) {
        this.homeTeamId = homeTeamId;
    }
    
    public String getAwayTeamId() {
        return awayTeamId;
    }
    
    public void setAwayTeamId(String awayTeamId) {
        this.awayTeamId = awayTeamId;
    }
    
    public LocalDateTime getDate() {
        return date;
    }
    
    public void setDate(LocalDateTime date) {
        this.date = date;
    }
    
    public Integer getHomeScore() {
        return homeScore;
    }
    
    public void setHomeScore(Integer homeScore) {
        this.homeScore = homeScore;
    }
    
    public Integer getAwayScore() {
        return awayScore;
    }
    
    public void setAwayScore(Integer awayScore) {
        this.awayScore = awayScore;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Game game = (Game) o;
        return Objects.equals(gameId, game.gameId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(gameId);
    }
    
    @Override
    public String toString() {
        return "Game{" +
                "gameId='" + gameId + '\'' +
                ", homeTeamId='" + homeTeamId + '\'' +
                ", awayTeamId='" + awayTeamId + '\'' +
                ", date=" + date +
                ", homeScore=" + homeScore +
                ", awayScore=" + awayScore +
                ", status='" + status + '\'' +
                '}';
    }
} 