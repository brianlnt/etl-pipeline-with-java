package com.sportsdata.etl.services.extractors;

import com.sportsdata.etl.models.Game;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

@Component
public class XmlFeedExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(XmlFeedExtractor.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public List<Game> extractGames(String filePath) {
        List<Game> games = new ArrayList<>();
        
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File(filePath));
            document.getDocumentElement().normalize();
            
            logger.info("Processing XML file: {}", filePath);
            
            NodeList gameNodes = document.getElementsByTagName("game");
            
            for (int i = 0; i < gameNodes.getLength(); i++) {
                Node gameNode = gameNodes.item(i);
                
                if (gameNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element gameElement = (Element) gameNode;
                    Game game = parseGameElement(gameElement, i + 1);
                    if (game != null) {
                        games.add(game);
                    }
                }
            }
            
            logger.info("Successfully extracted {} games from XML file: {}", games.size(), filePath);
            
        } catch (ParserConfigurationException e) {
            logger.error("Parser configuration error for XML file: {}", filePath, e);
        } catch (SAXException e) {
            logger.error("SAX parsing error for XML file: {}", filePath, e);
        } catch (IOException e) {
            logger.error("IO error reading XML file: {}", filePath, e);
        } catch (Exception e) {
            logger.error("Unexpected error processing XML file: {}", filePath, e);
        }
        
        return games;
    }
    
    private Game parseGameElement(Element gameElement, int gameNumber) {
        try {
            String gameId = getElementTextContent(gameElement, "gameId");
            String homeTeamId = getElementTextContent(gameElement, "homeTeamId");
            String awayTeamId = getElementTextContent(gameElement, "awayTeamId");
            String dateStr = getElementTextContent(gameElement, "date");
            String homeScoreStr = getElementTextContent(gameElement, "homeScore");
            String awayScoreStr = getElementTextContent(gameElement, "awayScore");
            String status = getElementTextContent(gameElement, "status");
            
            // Validate required fields
            if (gameId == null || homeTeamId == null || awayTeamId == null || 
                dateStr == null || status == null) {
                logger.warn("Missing required fields in game record {}: gameId={}, homeTeamId={}, awayTeamId={}, date={}, status={}", 
                    gameNumber, gameId, homeTeamId, awayTeamId, dateStr, status);
                return null;
            }
            
            // Validate that home and away teams are different
            if (homeTeamId.equals(awayTeamId)) {
                logger.warn("Game {} has same home and away team: {}", gameNumber, homeTeamId);
                return null;
            }
            
            // Parse date
            LocalDateTime date;
            try {
                date = LocalDateTime.parse(dateStr, DATE_TIME_FORMATTER);
            } catch (DateTimeParseException e) {
                logger.warn("Invalid date format in game {}: {}", gameNumber, dateStr);
                return null;
            }
            
            // Parse scores (can be null for scheduled games)
            Integer homeScore = null;
            Integer awayScore = null;
            
            if (homeScoreStr != null && !homeScoreStr.trim().isEmpty()) {
                try {
                    homeScore = Integer.parseInt(homeScoreStr.trim());
                    if (homeScore < 0) {
                        logger.warn("Invalid home score in game {}: {}", gameNumber, homeScore);
                        return null;
                    }
                } catch (NumberFormatException e) {
                    logger.warn("Invalid home score format in game {}: {}", gameNumber, homeScoreStr);
                    return null;
                }
            }
            
            if (awayScoreStr != null && !awayScoreStr.trim().isEmpty()) {
                try {
                    awayScore = Integer.parseInt(awayScoreStr.trim());
                    if (awayScore < 0) {
                        logger.warn("Invalid away score in game {}: {}", gameNumber, awayScore);
                        return null;
                    }
                } catch (NumberFormatException e) {
                    logger.warn("Invalid away score format in game {}: {}", gameNumber, awayScoreStr);
                    return null;
                }
            }
            
            return new Game(gameId, homeTeamId, awayTeamId, date, homeScore, awayScore, status);
            
        } catch (Exception e) {
            logger.error("Error parsing game element {}: {}", gameNumber, e.getMessage());
            return null;
        }
    }
    
    private String getElementTextContent(Element parent, String tagName) {
        NodeList nodeList = parent.getElementsByTagName(tagName);
        if (nodeList.getLength() > 0) {
            String content = nodeList.item(0).getTextContent();
            return content != null && !content.trim().isEmpty() ? content.trim() : null;
        }
        return null;
    }
    
    public boolean validateXmlStructure(String filePath) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File(filePath));
            document.getDocumentElement().normalize();
            
            // Check if the document has game elements
            NodeList gameNodes = document.getElementsByTagName("game");
            
            if (gameNodes.getLength() == 0) {
                logger.error("XML file has no game elements: {}", filePath);
                return false;
            }
            
            // Validate first game element structure
            Node firstGame = gameNodes.item(0);
            if (firstGame.getNodeType() == Node.ELEMENT_NODE) {
                Element gameElement = (Element) firstGame;
                
                // Check for required elements
                String[] requiredElements = {"gameId", "homeTeamId", "awayTeamId", "date", "status"};
                for (String elementName : requiredElements) {
                    if (getElementTextContent(gameElement, elementName) == null) {
                        logger.error("XML game element missing required field: {}", elementName);
                        return false;
                    }
                }
            }
            
            return true;
            
        } catch (ParserConfigurationException | SAXException | IOException e) {
            logger.error("Error validating XML file structure: {}", filePath, e);
            return false;
        }
    }
} 