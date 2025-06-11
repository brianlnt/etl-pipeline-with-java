package com.sportsdata.etl.models;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.Objects;

public class Player {
    
    private String playerId;
    
    @NotBlank
    private String name;
    
    @NotBlank
    private String teamId;
    
    @NotBlank
    private String position;
    
    @NotNull
    @Positive
    private Integer age;
    
    private PlayerStatistics statistics;
    
    // Constructors
    public Player() {
        this.statistics = new PlayerStatistics();
    }
    
    public Player(String playerId, String name, String teamId, String position, Integer age, PlayerStatistics statistics) {
        this.playerId = playerId;
        this.name = name;
        this.teamId = teamId;
        this.position = position;
        this.age = age;
        this.statistics = statistics != null ? statistics : new PlayerStatistics();
    }
    
    public String getPlayerId() {
        return playerId;
    }
    
    public void setPlayerId(String playerId) {
        this.playerId = playerId;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getTeamId() {
        return teamId;
    }
    
    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }
    
    public String getPosition() {
        return position;
    }
    
    public void setPosition(String position) {
        this.position = position;
    }
    
    public Integer getAge() {
        return age;
    }
    
    public void setAge(Integer age) {
        this.age = age;
    }
    
    public PlayerStatistics getStatistics() {
        return statistics;
    }
    
    public void setStatistics(PlayerStatistics statistics) {
        this.statistics = statistics;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Player player = (Player) o;
        return Objects.equals(playerId, player.playerId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(playerId);
    }
    
    @Override
    public String toString() {
        return "Player{" +
                "playerId='" + playerId + '\'' +
                ", name='" + name + '\'' +
                ", teamId='" + teamId + '\'' +
                ", position='" + position + '\'' +
                ", age=" + age +
                ", statistics=" + statistics +
                '}';
    }
    
    public static class PlayerStatistics {
        
        @PositiveOrZero
        private Integer gamesPlayed = 0;
        
        @PositiveOrZero
        private Integer points = 0;
        
        @PositiveOrZero
        private Integer assists = 0;
        
        public PlayerStatistics() {}
        
        public PlayerStatistics(Integer gamesPlayed, Integer points, Integer assists) {
            this.gamesPlayed = gamesPlayed;
            this.points = points;
            this.assists = assists;
        }
        
        public Integer getGamesPlayed() {
            return gamesPlayed;
        }
        
        public void setGamesPlayed(Integer gamesPlayed) {
            this.gamesPlayed = gamesPlayed;
        }
        
        public Integer getPoints() {
            return points;
        }
        
        public void setPoints(Integer points) {
            this.points = points;
        }
        
        public Integer getAssists() {
            return assists;
        }
        
        public void setAssists(Integer assists) {
            this.assists = assists;
        }
        
        @Override
        public String toString() {
            return "PlayerStatistics{" +
                    "gamesPlayed=" + gamesPlayed +
                    ", points=" + points +
                    ", assists=" + assists +
                    '}';
        }
    }
} 