package com.sportsdata.etl.transformers;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import com.sportsdata.etl.quality.ValidationRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class DataValidator {
    
    private static final Logger logger = LoggerFactory.getLogger(DataValidator.class);
    
    @Autowired
    private ValidationRules validationRules;
    
    public List<Team> validateTeams(List<Team> teams) {
        if (teams == null || teams.isEmpty()) {
            logger.info("No teams to validate");
            return new ArrayList<>();
        }
        
        List<Team> validTeams = new ArrayList<>();
        int errorCount = 0;
        int warningCount = 0;
        
        logger.info("Starting validation of {} teams", teams.size());
        
        for (Team team : teams) {
            ValidationRules.ValidationResult result = validationRules.validateTeam(team);
            
            if (result.isValid()) {
                validTeams.add(team);
                if (result.hasWarnings()) {
                    warningCount += result.getWarningCount();
                    logger.warn("Team {} has warnings: {}", team.getTeamId(), result.getWarnings());
                }
            } else {
                errorCount += result.getErrorCount();
                logger.error("Team {} failed validation: {}", team.getTeamId(), result.getErrors());
            }
        }
        
        logger.info("Team validation completed: {} valid, {} errors, {} warnings", 
            validTeams.size(), errorCount, warningCount);
        
        return validTeams;
    }
    
    public List<Player> validatePlayers(List<Player> players) {
        if (players == null || players.isEmpty()) {
            logger.info("No players to validate");
            return new ArrayList<>();
        }
        
        List<Player> validPlayers = new ArrayList<>();
        int errorCount = 0;
        int warningCount = 0;
        
        logger.info("Starting validation of {} players", players.size());
        
        for (Player player : players) {
            ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
            
            if (result.isValid()) {
                validPlayers.add(player);
                if (result.hasWarnings()) {
                    warningCount += result.getWarningCount();
                    logger.warn("Player {} has warnings: {}", player.getPlayerId(), result.getWarnings());
                }
            } else {
                errorCount += result.getErrorCount();
                logger.error("Player {} failed validation: {}", player.getPlayerId(), result.getErrors());
            }
        }
        
        logger.info("Player validation completed: {} valid, {} errors, {} warnings", 
            validPlayers.size(), errorCount, warningCount);
        
        return validPlayers;
    }
    
    public List<Game> validateGames(List<Game> games) {
        if (games == null || games.isEmpty()) {
            logger.info("No games to validate");
            return new ArrayList<>();
        }
        
        List<Game> validGames = new ArrayList<>();
        int errorCount = 0;
        int warningCount = 0;
        
        logger.info("Starting validation of {} games", games.size());
        
        for (Game game : games) {
            ValidationRules.ValidationResult result = validationRules.validateGame(game);
            
            if (result.isValid()) {
                validGames.add(game);
                if (result.hasWarnings()) {
                    warningCount += result.getWarningCount();
                    logger.warn("Game {} has warnings: {}", game.getGameId(), result.getWarnings());
                }
            } else {
                errorCount += result.getErrorCount();
                logger.error("Game {} failed validation: {}", game.getGameId(), result.getErrors());
            }
        }
        
        logger.info("Game validation completed: {} valid, {} errors, {} warnings", 
            validGames.size(), errorCount, warningCount);
        
        return validGames;
    }
} 