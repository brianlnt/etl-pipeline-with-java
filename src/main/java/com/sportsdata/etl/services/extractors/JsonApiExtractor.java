package com.sportsdata.etl.services.extractors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportsdata.etl.models.Player;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class JsonApiExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(JsonApiExtractor.class);
    private final ObjectMapper objectMapper;
    
    public JsonApiExtractor() {
        this.objectMapper = new ObjectMapper();
    }
    
    public List<Player> extractPlayers(String filePath) {
        List<Player> players = new ArrayList<>();
        
        try {
            JsonNode rootNode = objectMapper.readTree(new File(filePath));
            logger.info("Processing JSON file: {}", filePath);
            
            if (rootNode.isArray()) {
                // Handle array of players
                for (JsonNode playerNode : rootNode) {
                    Player player = parsePlayerNode(playerNode);
                    if (player != null) {
                        players.add(player);
                    }
                }
            } else if (rootNode.has("players")) {
                // Handle object with players array
                JsonNode playersNode = rootNode.get("players");
                if (playersNode.isArray()) {
                    for (JsonNode playerNode : playersNode) {
                        Player player = parsePlayerNode(playerNode);
                        if (player != null) {
                            players.add(player);
                        }
                    }
                }
            } else {
                // Handle single player object
                Player player = parsePlayerNode(rootNode);
                if (player != null) {
                    players.add(player);
                }
            }
            
            logger.info("Successfully extracted {} players from JSON file: {}", players.size(), filePath);
            
        } catch (IOException e) {
            logger.error("Error reading JSON file: {}", filePath, e);
        } catch (Exception e) {
            logger.error("Unexpected error processing JSON file: {}", filePath, e);
        }
        
        return players;
    }
    
    private Player parsePlayerNode(JsonNode playerNode) {
        try {
            // Extract basic player information
            String playerId = getTextValue(playerNode, "playerId");
            String name = getTextValue(playerNode, "name");
            String teamId = getTextValue(playerNode, "teamId");
            String position = getTextValue(playerNode, "position");
            Integer age = getIntValue(playerNode, "age");
            
            // Validate required fields
            if (playerId == null || name == null || teamId == null || position == null || age == null) {
                logger.warn("Missing required fields in player record: playerId={}, name={}, teamId={}, position={}, age={}", 
                    playerId, name, teamId, position, age);
                return null;
            }
            
            // Extract statistics
            Player.PlayerStatistics statistics = parsePlayerStatistics(playerNode.get("statistics"));
            
            return new Player(playerId, name, teamId, position, age, statistics);
            
        } catch (Exception e) {
            logger.error("Error parsing player node: {}", e.getMessage());
            return null;
        }
    }
    
    private Player.PlayerStatistics parsePlayerStatistics(JsonNode statisticsNode) {
        if (statisticsNode == null || statisticsNode.isNull()) {
            return new Player.PlayerStatistics();
        }
        
        try {
            Integer gamesPlayed = getIntValue(statisticsNode, "gamesPlayed", 0);
            Integer points = getIntValue(statisticsNode, "points", 0);
            Integer assists = getIntValue(statisticsNode, "assists", 0);
            
            return new Player.PlayerStatistics(gamesPlayed, points, assists);
            
        } catch (Exception e) {
            logger.warn("Error parsing player statistics, using defaults: {}", e.getMessage());
            return new Player.PlayerStatistics();
        }
    }
    
    private String getTextValue(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }
        String value = fieldNode.asText().trim();
        return value.isEmpty() ? null : value;
    }
    
    private Integer getIntValue(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }
        return fieldNode.asInt();
    }
    
    private Integer getIntValue(JsonNode node, String fieldName, int defaultValue) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull()) {
            return defaultValue;
        }
        return fieldNode.asInt(defaultValue);
    }
    
    public boolean validateJsonStructure(String filePath) {
        try {
            JsonNode rootNode = objectMapper.readTree(new File(filePath));
            
            // Check if it's a valid JSON structure for players
            if (rootNode.isArray()) {
                return !rootNode.isEmpty();
            } else if (rootNode.has("players")) {
                JsonNode playersNode = rootNode.get("players");
                return playersNode.isArray() && !playersNode.isEmpty();
            } else {
                // Check if it's a single player object
                return rootNode.has("playerId") && rootNode.has("name");
            }
            
        } catch (IOException e) {
            logger.error("Error validating JSON file structure: {}", filePath, e);
            return false;
        }
    }
} 