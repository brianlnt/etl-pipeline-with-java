package com.sportsdata.etl.quality;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import com.sportsdata.etl.services.quality.ValidationRules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class ValidationRulesTest {
    
    private ValidationRules validationRules;
    
    @BeforeEach
    void setUp() {
        validationRules = new ValidationRules();
    }
    
    @Test
    void testValidateTeam_ValidTeam() {
        Team team = new Team("LAL", "Los Angeles Lakers", "Los Angeles", "NBA", 
                           LocalDate.of(1947, 1, 1), "Crypto.com Arena");
        
        ValidationRules.ValidationResult result = validationRules.validateTeam(team);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(0, result.getWarningCount());
    }
    
    @Test
    void testValidateTeam_NullTeam() {
        ValidationRules.ValidationResult result = validationRules.validateTeam(null);
        
        assertFalse(result.isValid());
        assertEquals(1, result.getErrorCount());
        assertTrue(result.getErrors().contains("Team object is null"));
    }
    
    @Test
    void testValidateTeam_MissingRequiredFields() {
        Team team = new Team(null, "", "Los Angeles", "NBA", 
                           LocalDate.of(1947, 1, 1), "Crypto.com Arena");
        
        ValidationRules.ValidationResult result = validationRules.validateTeam(team);
        
        assertFalse(result.isValid());
        assertEquals(2, result.getErrorCount());
        assertTrue(result.getErrors().contains("Team ID is required"));
        assertTrue(result.getErrors().contains("Team name is required"));
    }
    
    @Test
    void testValidateTeam_FutureFoundedDate() {
        Team team = new Team("LAL", "Los Angeles Lakers", "Los Angeles", "NBA", 
                           LocalDate.now().plusDays(1), "Crypto.com Arena");
        
        ValidationRules.ValidationResult result = validationRules.validateTeam(team);
        
        assertFalse(result.isValid());
        assertEquals(1, result.getErrorCount());
        assertTrue(result.getErrors().contains("Team founded date cannot be in the future"));
    }
    
    @Test
    void testValidateTeam_VeryEarlyFoundedDate() {
        Team team = new Team("LAL", "Los Angeles Lakers", "Los Angeles", "NBA", 
                           LocalDate.of(1800, 1, 1), "Crypto.com Arena");
        
        ValidationRules.ValidationResult result = validationRules.validateTeam(team);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Team founded date seems unusually early"));
    }
    
    @Test
    void testValidatePlayer_ValidPlayer() {
        Player.PlayerStatistics stats = new Player.PlayerStatistics(74, 2168, 515);
        Player player = new Player("P001", "LeBron James", "LAL", "Small Forward", 39, stats);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(0, result.getWarningCount());
    }
    
    @Test
    void testValidatePlayer_NullPlayer() {
        ValidationRules.ValidationResult result = validationRules.validatePlayer(null);
        
        assertFalse(result.isValid());
        assertEquals(1, result.getErrorCount());
        assertTrue(result.getErrors().contains("Player object is null"));
    }
    
    @Test
    void testValidatePlayer_MissingRequiredFields() {
        Player player = new Player(null, "", "LAL", "", null, null);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertFalse(result.isValid());
        assertEquals(4, result.getErrorCount());
        assertTrue(result.getErrors().contains("Player ID is required"));
        assertTrue(result.getErrors().contains("Player name is required"));
        assertTrue(result.getErrors().contains("Player position is required"));
        assertTrue(result.getErrors().contains("Player age is required"));
    }
    
    @Test
    void testValidatePlayer_InvalidAge() {
        Player player = new Player("P001", "Young Player", "LAL", "Guard", 15, null);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertFalse(result.isValid());
        assertEquals(1, result.getErrorCount());
        assertTrue(result.getErrors().contains("Player age is too young: 15"));
    }
    
    @Test
    void testValidatePlayer_HighAge() {
        Player player = new Player("P001", "Old Player", "LAL", "Guard", 55, null);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Player age seems unusually high: 55"));
    }
    
    @Test
    void testValidatePlayer_UnknownPosition() {
        Player player = new Player("P001", "Player", "LAL", "Unknown Position", 25, null);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Unknown player position: Unknown Position"));
    }
    
    @Test
    void testValidatePlayer_NegativeStatistics() {
        Player.PlayerStatistics stats = new Player.PlayerStatistics(-1, -10, -5);
        Player player = new Player("P001", "Player", "LAL", "Guard", 25, stats);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertFalse(result.isValid());
        assertEquals(3, result.getErrorCount());
        assertTrue(result.getErrors().contains("Games played cannot be negative"));
        assertTrue(result.getErrors().contains("Points cannot be negative"));
        assertTrue(result.getErrors().contains("Assists cannot be negative"));
    }
    
    @Test
    void testValidatePlayer_StatisticsWithoutGames() {
        Player.PlayerStatistics stats = new Player.PlayerStatistics(0, 100, 50);
        Player player = new Player("P001", "Player", "LAL", "Guard", 25, stats);
        
        ValidationRules.ValidationResult result = validationRules.validatePlayer(player);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Player has statistics but no games played"));
    }
    
    @Test
    void testValidateGame_ValidGame() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           118, 124, "Final");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(0, result.getWarningCount());
    }
    
    @Test
    void testValidateGame_NullGame() {
        ValidationRules.ValidationResult result = validationRules.validateGame(null);
        
        assertFalse(result.isValid());
        assertEquals(1, result.getErrorCount());
        assertTrue(result.getErrors().contains("Game object is null"));
    }
    
    @Test
    void testValidateGame_MissingRequiredFields() {
        Game game = new Game(null, "", "GSW", null, 118, 124, "");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertFalse(result.isValid());
        assertEquals(4, result.getErrorCount());
        assertTrue(result.getErrors().contains("Game ID is required"));
        assertTrue(result.getErrors().contains("Home team ID is required"));
        assertTrue(result.getErrors().contains("Game date is required"));
        assertTrue(result.getErrors().contains("Game status is required"));
    }
    
    @Test
    void testValidateGame_SameTeams() {
        Game game = new Game("G001", "LAL", "LAL", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           118, 124, "Final");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertFalse(result.isValid());
        assertEquals(1, result.getErrorCount());
        assertTrue(result.getErrors().contains("Home team and away team cannot be the same"));
    }
    
    @Test
    void testValidateGame_NegativeScores() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           -5, -10, "Final");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertFalse(result.isValid());
        assertEquals(2, result.getErrorCount());
        assertTrue(result.getErrors().contains("Home score cannot be negative"));
        assertTrue(result.getErrors().contains("Away score cannot be negative"));
    }
    
    @Test
    void testValidateGame_UnusuallyHighScores() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           250, 300, "Final");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(2, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Home score seems unusually high: 250"));
        assertTrue(result.getWarnings().get(1).contains("Away score seems unusually high: 300"));
    }
    
    @Test
    void testValidateGame_FinalGameWithoutScores() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           null, null, "Final");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Final game should have both scores recorded"));
    }
    
    @Test
    void testValidateGame_ScheduledGameWithScores() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           118, 124, "Scheduled");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Scheduled game should not have scores"));
    }
    
    @Test
    void testValidateGame_UnknownStatus() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(2024, 1, 15, 19, 30), 
                           118, 124, "Unknown Status");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Unknown game status: Unknown Status"));
    }
    
    @Test
    void testValidateGame_VeryEarlyDate() {
        Game game = new Game("G001", "LAL", "GSW", LocalDateTime.of(1800, 1, 1, 12, 0), 
                           118, 124, "Final");
        
        ValidationRules.ValidationResult result = validationRules.validateGame(game);
        
        assertTrue(result.isValid());
        assertEquals(0, result.getErrorCount());
        assertEquals(1, result.getWarningCount());
        assertTrue(result.getWarnings().get(0).contains("Game date seems unusually early"));
    }
} 