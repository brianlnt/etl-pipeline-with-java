package com.sportsdata.etl.services.quality;

import com.sportsdata.etl.models.Game;
import com.sportsdata.etl.models.Player;
import com.sportsdata.etl.models.Team;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class ValidationRules {
    
    private static final Set<String> VALID_POSITIONS = Set.of(
        "Point Guard", "PG", "Shooting Guard", "SG", "Small Forward", "SF", 
        "Power Forward", "PF", "Center", "C", "Forward", "Guard"
    );
    
    private static final Set<String> VALID_GAME_STATUSES = Set.of(
        "Scheduled", "Live", "Final", "Postponed", "Cancelled"
    );
    
    private static final int MIN_PLAYER_AGE = 16;
    private static final int MAX_PLAYER_AGE = 50;
    private static final int MAX_REASONABLE_SCORE = 200;
    
    public ValidationResult validateTeam(Team team) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        
        if (team == null) {
            errors.add("Team object is null");
            return new ValidationResult(false, errors, warnings);
        }
        
        // Required field validations
        if (team.getTeamId() == null || team.getTeamId().trim().isEmpty()) {
            errors.add("Team ID is required");
        }
        
        if (team.getName() == null || team.getName().trim().isEmpty()) {
            errors.add("Team name is required");
        }
        
        if (team.getCity() == null || team.getCity().trim().isEmpty()) {
            errors.add("Team city is required");
        }
        
        if (team.getLeague() == null || team.getLeague().trim().isEmpty()) {
            errors.add("Team league is required");
        }
        
        // Business rule validations
        if (team.getFounded() != null) {
            if (team.getFounded().isAfter(LocalDate.now())) {
                errors.add("Team founded date cannot be in the future");
            } else if (team.getFounded().isBefore(LocalDate.of(1850, 1, 1))) {
                warnings.add("Team founded date seems unusually early: " + team.getFounded());
            }
        }
        
        // Format validations
        if (team.getName() != null && team.getName().length() > 100) {
            warnings.add("Team name is unusually long");
        }
        
        if (team.getCity() != null && team.getCity().length() > 50) {
            warnings.add("City name is unusually long");
        }
        
        return new ValidationResult(errors.isEmpty(), errors, warnings);
    }
    
    public ValidationResult validatePlayer(Player player) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        
        if (player == null) {
            errors.add("Player object is null");
            return new ValidationResult(false, errors, warnings);
        }
        
        // Required field validations
        if (player.getPlayerId() == null || player.getPlayerId().trim().isEmpty()) {
            errors.add("Player ID is required");
        }
        
        if (player.getName() == null || player.getName().trim().isEmpty()) {
            errors.add("Player name is required");
        }
        
        if (player.getTeamId() == null || player.getTeamId().trim().isEmpty()) {
            errors.add("Player team ID is required");
        }
        
        if (player.getPosition() == null || player.getPosition().trim().isEmpty()) {
            errors.add("Player position is required");
        } else if (!VALID_POSITIONS.contains(player.getPosition().trim())) {
            warnings.add("Unknown player position: " + player.getPosition());
        }
        
        if (player.getAge() == null) {
            errors.add("Player age is required");
        } else {
            if (player.getAge() < MIN_PLAYER_AGE) {
                errors.add("Player age is too young: " + player.getAge());
            } else if (player.getAge() > MAX_PLAYER_AGE) {
                warnings.add("Player age seems unusually high: " + player.getAge());
            }
        }
        
        // Statistics validation
        if (player.getStatistics() != null) {
            Player.PlayerStatistics stats = player.getStatistics();
            
            if (stats.getGamesPlayed() != null && stats.getGamesPlayed() < 0) {
                errors.add("Games played cannot be negative");
            }
            
            if (stats.getPoints() != null && stats.getPoints() < 0) {
                errors.add("Points cannot be negative");
            }
            
            if (stats.getAssists() != null && stats.getAssists() < 0) {
                errors.add("Assists cannot be negative");
            }
            
            // Cross-field validations
            if (stats.getGamesPlayed() != null && stats.getGamesPlayed() == 0) {
                if ((stats.getPoints() != null && stats.getPoints() > 0) ||
                    (stats.getAssists() != null && stats.getAssists() > 0)) {
                    warnings.add("Player has statistics but no games played");
                }
            }
        }
        
        return new ValidationResult(errors.isEmpty(), errors, warnings);
    }
    
    public ValidationResult validateGame(Game game) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        
        if (game == null) {
            errors.add("Game object is null");
            return new ValidationResult(false, errors, warnings);
        }
        
        // Required field validations
        if (game.getGameId() == null || game.getGameId().trim().isEmpty()) {
            errors.add("Game ID is required");
        }
        
        if (game.getHomeTeamId() == null || game.getHomeTeamId().trim().isEmpty()) {
            errors.add("Home team ID is required");
        }
        
        if (game.getAwayTeamId() == null || game.getAwayTeamId().trim().isEmpty()) {
            errors.add("Away team ID is required");
        }
        
        if (game.getDate() == null) {
            errors.add("Game date is required");
        } else if (game.getDate().isBefore(LocalDateTime.of(1900, 1, 1, 0, 0))) {
            warnings.add("Game date seems unusually early: " + game.getDate());
        }
        
        if (game.getStatus() == null || game.getStatus().trim().isEmpty()) {
            errors.add("Game status is required");
        } else if (!VALID_GAME_STATUSES.contains(game.getStatus().trim())) {
            warnings.add("Unknown game status: " + game.getStatus());
        }
        
        // Business rule validations
        if (game.getHomeTeamId() != null && game.getAwayTeamId() != null &&
            game.getHomeTeamId().equals(game.getAwayTeamId())) {
            errors.add("Home team and away team cannot be the same");
        }
        
        // Score validations
        if (game.getHomeScore() != null && game.getHomeScore() < 0) {
            errors.add("Home score cannot be negative");
        }
        
        if (game.getAwayScore() != null && game.getAwayScore() < 0) {
            errors.add("Away score cannot be negative");
        }
        
        if (game.getHomeScore() != null && game.getHomeScore() > MAX_REASONABLE_SCORE) {
            warnings.add("Home score seems unusually high: " + game.getHomeScore());
        }
        
        if (game.getAwayScore() != null && game.getAwayScore() > MAX_REASONABLE_SCORE) {
            warnings.add("Away score seems unusually high: " + game.getAwayScore());
        }
        
        // Status-score consistency validation
        if ("Final".equals(game.getStatus())) {
            if (game.getHomeScore() == null || game.getAwayScore() == null) {
                warnings.add("Final game should have both scores recorded");
            }
        } else if ("Scheduled".equals(game.getStatus())) {
            if (game.getHomeScore() != null || game.getAwayScore() != null) {
                warnings.add("Scheduled game should not have scores");
            }
        }
        
        return new ValidationResult(errors.isEmpty(), errors, warnings);
    }
    
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;
        private final List<String> warnings;
        
        public ValidationResult(boolean valid, List<String> errors, List<String> warnings) {
            this.valid = valid;
            this.errors = new ArrayList<>(errors);
            this.warnings = new ArrayList<>(warnings);
        }
        
        public boolean isValid() {
            return valid;
        }
        
        public List<String> getErrors() {
            return new ArrayList<>(errors);
        }
        
        public List<String> getWarnings() {
            return new ArrayList<>(warnings);
        }
        
        public boolean hasWarnings() {
            return !warnings.isEmpty();
        }
        
        public int getErrorCount() {
            return errors.size();
        }
        
        public int getWarningCount() {
            return warnings.size();
        }
        
        @Override
        public String toString() {
            return "ValidationResult{" +
                    "valid=" + valid +
                    ", errors=" + errors.size() +
                    ", warnings=" + warnings.size() +
                    '}';
        }
    }
} 