package com.sportsdata.etl.transformers;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Component
public class DataCleaner {
    
    private static final Logger logger = LoggerFactory.getLogger(DataCleaner.class);
    
    public List<Team> cleanTeams(List<Team> teams) {
        if (teams == null || teams.isEmpty()) {
            logger.info("No teams to clean");
            return new ArrayList<>();
        }
        
        logger.info("Starting data cleaning for {} teams", teams.size());
        
        List<Team> cleanedTeams = new ArrayList<>();
        Set<String> seenTeamIds = new LinkedHashSet<>();
        int duplicateCount = 0;
        
        for (Team team : teams) {
            if (team == null) {
                continue;
            }
            
            // Remove duplicates based on team ID
            if (seenTeamIds.contains(team.getTeamId())) {
                duplicateCount++;
                logger.debug("Duplicate team found and removed: {}", team.getTeamId());
                continue;
            }
            
            // Clean team data
            Team cleanedTeam = cleanTeam(team);
            if (cleanedTeam != null) {
                cleanedTeams.add(cleanedTeam);
                seenTeamIds.add(cleanedTeam.getTeamId());
            }
        }
        
        logger.info("Team cleaning completed: {} cleaned, {} duplicates removed", 
            cleanedTeams.size(), duplicateCount);
        
        return cleanedTeams;
    }
    
    private Team cleanTeam(Team team) {
        if (team == null) {
            return null;
        }
        
        // Clean string fields
        String cleanedName = cleanStringField(team.getName());
        String cleanedCity = cleanStringField(team.getCity());
        String cleanedLeague = cleanStringField(team.getLeague());
        String cleanedVenue = cleanStringField(team.getVenue());
        
        // Create cleaned team
        Team cleanedTeam = new Team();
        cleanedTeam.setTeamId(team.getTeamId());
        cleanedTeam.setName(cleanedName);
        cleanedTeam.setCity(cleanedCity);
        cleanedTeam.setLeague(cleanedLeague);
        cleanedTeam.setFounded(team.getFounded());
        cleanedTeam.setVenue(cleanedVenue);
        
        return cleanedTeam;
    }
    
    public List<Player> cleanPlayers(List<Player> players) {
        if (players == null || players.isEmpty()) {
            logger.info("No players to clean");
            return new ArrayList<>();
        }
        
        logger.info("Starting data cleaning for {} players", players.size());
        
        List<Player> cleanedPlayers = new ArrayList<>();
        Set<String> seenPlayerIds = new LinkedHashSet<>();
        int duplicateCount = 0;
        
        for (Player player : players) {
            if (player == null) {
                continue;
            }
            
            // Remove duplicates based on player ID
            if (seenPlayerIds.contains(player.getPlayerId())) {
                duplicateCount++;
                logger.debug("Duplicate player found and removed: {}", player.getPlayerId());
                continue;
            }
            
            // Clean player data
            Player cleanedPlayer = cleanPlayer(player);
            if (cleanedPlayer != null) {
                cleanedPlayers.add(cleanedPlayer);
                seenPlayerIds.add(cleanedPlayer.getPlayerId());
            }
        }
        
        logger.info("Player cleaning completed: {} cleaned, {} duplicates removed", 
            cleanedPlayers.size(), duplicateCount);
        
        return cleanedPlayers;
    }
    
    private Player cleanPlayer(Player player) {
        if (player == null) {
            return null;
        }
        
        // Clean string fields
        String cleanedName = cleanStringField(player.getName());
        String cleanedPosition = cleanStringField(player.getPosition());
        
        // Create cleaned player
        Player cleanedPlayer = new Player();
        cleanedPlayer.setPlayerId(player.getPlayerId());
        cleanedPlayer.setName(cleanedName);
        cleanedPlayer.setTeamId(player.getTeamId());
        cleanedPlayer.setPosition(cleanedPosition);
        cleanedPlayer.setAge(player.getAge());
        cleanedPlayer.setStatistics(player.getStatistics());
        
        return cleanedPlayer;
    }
    
    public List<Game> cleanGames(List<Game> games) {
        if (games == null || games.isEmpty()) {
            logger.info("No games to clean");
            return new ArrayList<>();
        }
        
        logger.info("Starting data cleaning for {} games", games.size());
        
        List<Game> cleanedGames = new ArrayList<>();
        Set<String> seenGameIds = new LinkedHashSet<>();
        int duplicateCount = 0;
        
        for (Game game : games) {
            if (game == null) {
                continue;
            }
            
            // Remove duplicates based on game ID
            if (seenGameIds.contains(game.getGameId())) {
                duplicateCount++;
                logger.debug("Duplicate game found and removed: {}", game.getGameId());
                continue;
            }
            
            // Clean game data
            Game cleanedGame = cleanGame(game);
            if (cleanedGame != null) {
                cleanedGames.add(cleanedGame);
                seenGameIds.add(cleanedGame.getGameId());
            }
        }
        
        logger.info("Game cleaning completed: {} cleaned, {} duplicates removed", 
            cleanedGames.size(), duplicateCount);
        
        return cleanedGames;
    }
    
    private Game cleanGame(Game game) {
        if (game == null) {
            return null;
        }
        
        // Clean string fields
        String cleanedStatus = cleanStringField(game.getStatus());
        
        // Create cleaned game
        Game cleanedGame = new Game();
        cleanedGame.setGameId(game.getGameId());
        cleanedGame.setHomeTeamId(game.getHomeTeamId());
        cleanedGame.setAwayTeamId(game.getAwayTeamId());
        cleanedGame.setDate(game.getDate());
        cleanedGame.setHomeScore(game.getHomeScore());
        cleanedGame.setAwayScore(game.getAwayScore());
        cleanedGame.setStatus(cleanedStatus);
        
        return cleanedGame;
    }
    
    private String cleanStringField(String value) {
        if (value == null) {
            return null;
        }
        
        // Trim whitespace and normalize
        String cleaned = StringUtils.trim(value);
        
        // Remove extra whitespace between words
        cleaned = StringUtils.normalizeSpace(cleaned);
        
        // Return null if empty after cleaning
        return StringUtils.isEmpty(cleaned) ? null : cleaned;
    }
} 