package com.sportsdata.etl.transformers;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DataStandardizer {
    
    private static final Logger logger = LoggerFactory.getLogger(DataStandardizer.class);
    
    // Position standardization mapping
    private static final Map<String, String> POSITION_MAPPING = new HashMap<>();
    
    static {
        // Guard positions
        POSITION_MAPPING.put("PG", "Point Guard");
        POSITION_MAPPING.put("Point Guard", "Point Guard");
        POSITION_MAPPING.put("SG", "Shooting Guard");
        POSITION_MAPPING.put("Shooting Guard", "Shooting Guard");
        POSITION_MAPPING.put("Guard", "Guard");
        POSITION_MAPPING.put("G", "Guard");
        
        // Forward positions
        POSITION_MAPPING.put("SF", "Small Forward");
        POSITION_MAPPING.put("Small Forward", "Small Forward");
        POSITION_MAPPING.put("PF", "Power Forward");
        POSITION_MAPPING.put("Power Forward", "Power Forward");
        POSITION_MAPPING.put("Forward", "Forward");
        POSITION_MAPPING.put("F", "Forward");
        
        // Center position
        POSITION_MAPPING.put("C", "Center");
        POSITION_MAPPING.put("Center", "Center");
    }
    
    // League standardization mapping
    private static final Map<String, String> LEAGUE_MAPPING = new HashMap<>();
    
    static {
        LEAGUE_MAPPING.put("NBA", "NBA");
        LEAGUE_MAPPING.put("National Basketball Association", "NBA");
        LEAGUE_MAPPING.put("WNBA", "WNBA");
        LEAGUE_MAPPING.put("Women's National Basketball Association", "WNBA");
        LEAGUE_MAPPING.put("NCAA", "NCAA");
        LEAGUE_MAPPING.put("College Basketball", "NCAA");
    }
    
    // Game status standardization mapping
    private static final Map<String, String> STATUS_MAPPING = new HashMap<>();
    
    static {
        STATUS_MAPPING.put("Scheduled", "Scheduled");
        STATUS_MAPPING.put("SCHEDULED", "Scheduled");
        STATUS_MAPPING.put("upcoming", "Scheduled");
        STATUS_MAPPING.put("Live", "Live");
        STATUS_MAPPING.put("LIVE", "Live");
        STATUS_MAPPING.put("in-progress", "Live");
        STATUS_MAPPING.put("Final", "Final");
        STATUS_MAPPING.put("FINAL", "Final");
        STATUS_MAPPING.put("completed", "Final");
        STATUS_MAPPING.put("finished", "Final");
        STATUS_MAPPING.put("Postponed", "Postponed");
        STATUS_MAPPING.put("POSTPONED", "Postponed");
        STATUS_MAPPING.put("delayed", "Postponed");
        STATUS_MAPPING.put("Cancelled", "Cancelled");
        STATUS_MAPPING.put("CANCELLED", "Cancelled");
        STATUS_MAPPING.put("canceled", "Cancelled");
    }
    
    public List<Team> standardizeTeams(List<Team> teams) {
        if (teams == null || teams.isEmpty()) {
            logger.info("No teams to standardize");
            return new ArrayList<>();
        }
        
        logger.info("Starting standardization for {} teams", teams.size());
        
        List<Team> standardizedTeams = new ArrayList<>();
        
        for (Team team : teams) {
            Team standardizedTeam = standardizeTeam(team);
            if (standardizedTeam != null) {
                standardizedTeams.add(standardizedTeam);
            }
        }
        
        logger.info("Team standardization completed: {} teams processed", standardizedTeams.size());
        
        return standardizedTeams;
    }
    
    private Team standardizeTeam(Team team) {
        if (team == null) {
            return null;
        }
        
        Team standardizedTeam = new Team();
        standardizedTeam.setTeamId(team.getTeamId());
        standardizedTeam.setName(standardizeName(team.getName()));
        standardizedTeam.setCity(standardizeName(team.getCity()));
        standardizedTeam.setLeague(standardizeLeague(team.getLeague()));
        standardizedTeam.setFounded(team.getFounded());
        standardizedTeam.setVenue(standardizeName(team.getVenue()));
        
        return standardizedTeam;
    }
    
    public List<Player> standardizePlayers(List<Player> players) {
        if (players == null || players.isEmpty()) {
            logger.info("No players to standardize");
            return new ArrayList<>();
        }
        
        logger.info("Starting standardization for {} players", players.size());
        
        List<Player> standardizedPlayers = new ArrayList<>();
        
        for (Player player : players) {
            Player standardizedPlayer = standardizePlayer(player);
            if (standardizedPlayer != null) {
                standardizedPlayers.add(standardizedPlayer);
            }
        }
        
        logger.info("Player standardization completed: {} players processed", standardizedPlayers.size());
        
        return standardizedPlayers;
    }
    
    private Player standardizePlayer(Player player) {
        if (player == null) {
            return null;
        }
        
        Player standardizedPlayer = new Player();
        standardizedPlayer.setPlayerId(player.getPlayerId());
        standardizedPlayer.setName(standardizeName(player.getName()));
        standardizedPlayer.setTeamId(player.getTeamId());
        standardizedPlayer.setPosition(standardizePosition(player.getPosition()));
        standardizedPlayer.setAge(player.getAge());
        standardizedPlayer.setStatistics(player.getStatistics());
        
        return standardizedPlayer;
    }
    
    public List<Game> standardizeGames(List<Game> games) {
        if (games == null || games.isEmpty()) {
            logger.info("No games to standardize");
            return new ArrayList<>();
        }
        
        logger.info("Starting standardization for {} games", games.size());
        
        List<Game> standardizedGames = new ArrayList<>();
        
        for (Game game : games) {
            Game standardizedGame = standardizeGame(game);
            if (standardizedGame != null) {
                standardizedGames.add(standardizedGame);
            }
        }
        
        logger.info("Game standardization completed: {} games processed", standardizedGames.size());
        
        return standardizedGames;
    }
    
    private Game standardizeGame(Game game) {
        if (game == null) {
            return null;
        }
        
        Game standardizedGame = new Game();
        standardizedGame.setGameId(game.getGameId());
        standardizedGame.setHomeTeamId(game.getHomeTeamId());
        standardizedGame.setAwayTeamId(game.getAwayTeamId());
        standardizedGame.setDate(game.getDate());
        standardizedGame.setHomeScore(game.getHomeScore());
        standardizedGame.setAwayScore(game.getAwayScore());
        standardizedGame.setStatus(standardizeGameStatus(game.getStatus()));
        
        return standardizedGame;
    }
    
    private String standardizeName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return name;
        }
        
        // Convert to title case
        String[] words = name.trim().toLowerCase().split("\\s+");
        StringBuilder standardized = new StringBuilder();
        
        for (int i = 0; i < words.length; i++) {
            if (i > 0) {
                standardized.append(" ");
            }
            
            String word = words[i];
            if (!word.isEmpty()) {
                standardized.append(Character.toUpperCase(word.charAt(0)));
                if (word.length() > 1) {
                    standardized.append(word.substring(1));
                }
            }
        }
        
        return standardized.toString();
    }
    
    private String standardizePosition(String position) {
        if (position == null || position.trim().isEmpty()) {
            return position;
        }
        
        String trimmed = position.trim();
        String standardized = POSITION_MAPPING.get(trimmed);
        
        if (standardized != null) {
            return standardized;
        }
        
        // If no mapping found, try case-insensitive search
        for (Map.Entry<String, String> entry : POSITION_MAPPING.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(trimmed)) {
                return entry.getValue();
            }
        }
        
        // Return original if no standardization found
        logger.debug("No standardization found for position: {}", position);
        return trimmed;
    }
    
    private String standardizeLeague(String league) {
        if (league == null || league.trim().isEmpty()) {
            return league;
        }
        
        String trimmed = league.trim();
        String standardized = LEAGUE_MAPPING.get(trimmed);
        
        if (standardized != null) {
            return standardized;
        }
        
        // If no mapping found, try case-insensitive search
        for (Map.Entry<String, String> entry : LEAGUE_MAPPING.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(trimmed)) {
                return entry.getValue();
            }
        }
        
        // Return original if no standardization found
        logger.debug("No standardization found for league: {}", league);
        return trimmed;
    }
    
    private String standardizeGameStatus(String status) {
        if (status == null || status.trim().isEmpty()) {
            return status;
        }
        
        String trimmed = status.trim();
        String standardized = STATUS_MAPPING.get(trimmed);
        
        if (standardized != null) {
            return standardized;
        }
        
        // If no mapping found, try case-insensitive search
        for (Map.Entry<String, String> entry : STATUS_MAPPING.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(trimmed)) {
                return entry.getValue();
            }
        }
        
        // Return original if no standardization found
        logger.debug("No standardization found for game status: {}", status);
        return trimmed;
    }
} 