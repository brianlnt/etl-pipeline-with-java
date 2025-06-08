package com.sportsdata.etl.loaders;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import com.sportsdata.etl.pipeline.EtlPipeline;
import com.sportsdata.etl.repositories.GameRepository;
import com.sportsdata.etl.repositories.PlayerRepository;
import com.sportsdata.etl.repositories.TeamRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class DatabaseLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseLoader.class);
    
    @Autowired
    private TeamRepository teamRepository;
    
    @Autowired
    private PlayerRepository playerRepository;
    
    @Autowired
    private GameRepository gameRepository;
    
    @Transactional
    public EtlPipeline.LoadResult loadAllData(EtlPipeline.TransformedData transformedData) {
        EtlPipeline.LoadResult result = new EtlPipeline.LoadResult();
        
        try {
            logger.info("Starting database loading process");
            
            // Load teams first (as they may be referenced by other entities)
            int teamsLoaded = loadTeams(transformedData.getTeams());
            result.setTeamsLoaded(teamsLoaded);
            
            // Load players (they reference teams)
            int playersLoaded = loadPlayers(transformedData.getPlayers());
            result.setPlayersLoaded(playersLoaded);
            
            // Load games (they reference teams)
            int gamesLoaded = loadGames(transformedData.getGames());
            result.setGamesLoaded(gamesLoaded);
            
            result.setSuccess(true);
            
            logger.info("Database loading completed successfully: {} teams, {} players, {} games", 
                teamsLoaded, playersLoaded, gamesLoaded);
            
        } catch (Exception e) {
            logger.error("Database loading failed", e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
            throw e; // Re-throw to trigger transaction rollback
        }
        
        return result;
    }
    
    private int loadTeams(List<Team> teams) {
        if (teams == null || teams.isEmpty()) {
            logger.info("No teams to load");
            return 0;
        }
        
        logger.info("Loading {} teams into database", teams.size());
        
        try {
            List<Team> savedTeams = teamRepository.saveAll(teams);
            logger.info("Successfully loaded {} teams", savedTeams.size());
            return savedTeams.size();
        } catch (Exception e) {
            logger.error("Failed to load teams", e);
            throw new RuntimeException("Team loading failed", e);
        }
    }
    
    private int loadPlayers(List<Player> players) {
        if (players == null || players.isEmpty()) {
            logger.info("No players to load");
            return 0;
        }
        
        logger.info("Loading {} players into database", players.size());
        
        try {
            List<Player> savedPlayers = playerRepository.saveAll(players);
            logger.info("Successfully loaded {} players", savedPlayers.size());
            return savedPlayers.size();
        } catch (Exception e) {
            logger.error("Failed to load players", e);
            throw new RuntimeException("Player loading failed", e);
        }
    }
    
    private int loadGames(List<Game> games) {
        if (games == null || games.isEmpty()) {
            logger.info("No games to load");
            return 0;
        }
        
        logger.info("Loading {} games into database", games.size());
        
        try {
            List<Game> savedGames = gameRepository.saveAll(games);
            logger.info("Successfully loaded {} games", savedGames.size());
            return savedGames.size();
        } catch (Exception e) {
            logger.error("Failed to load games", e);
            throw new RuntimeException("Game loading failed", e);
        }
    }
    
    @Transactional
    public void loadTeamsOnly(List<Team> teams) {
        loadTeams(teams);
    }
    
    @Transactional
    public void loadPlayersOnly(List<Player> players) {
        loadPlayers(players);
    }
    
    @Transactional
    public void loadGamesOnly(List<Game> games) {
        loadGames(games);
    }
    
    public long getTeamCount() {
        return teamRepository.count();
    }
    
    public long getPlayerCount() {
        return playerRepository.count();
    }
    
    public long getGameCount() {
        return gameRepository.count();
    }
    
    @Transactional
    public void clearAllData() {
        logger.warn("Clearing all data from database");
        
        // Delete in reverse order of dependencies
        gameRepository.deleteAll();
        playerRepository.deleteAll();
        teamRepository.deleteAll();
        
        logger.info("All data cleared from database");
    }
} 