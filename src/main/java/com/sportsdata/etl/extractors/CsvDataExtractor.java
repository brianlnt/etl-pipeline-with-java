package com.sportsdata.etl.extractors;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.sportsdata.etl.models.Team;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

@Component
public class CsvDataExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvDataExtractor.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public List<Team> extractTeams(String filePath) {
        List<Team> teams = new ArrayList<>();
        
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = csvReader.readAll();
            
            // Skip header row
            if (records.isEmpty()) {
                logger.warn("CSV file is empty: {}", filePath);
                return teams;
            }
            
            String[] headers = records.get(0);
            logger.info("Processing CSV file: {} with headers: {}", filePath, String.join(", ", headers));
            
            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);
                try {
                    Team team = parseTeamRecord(record, i + 1);
                    if (team != null) {
                        teams.add(team);
                    }
                } catch (Exception e) {
                    logger.error("Error parsing team record at line {}: {}", i + 1, e.getMessage());
                }
            }
            
            logger.info("Successfully extracted {} teams from CSV file: {}", teams.size(), filePath);
            
        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", filePath, e);
        } catch (CsvException e) {
            logger.error("Error parsing CSV file: {}", filePath, e);
        }
        
        return teams;
    }
    
    private Team parseTeamRecord(String[] record, int lineNumber) {
        if (record.length < 6) {
            logger.warn("Insufficient columns in record at line {}: expected 6, got {}", lineNumber, record.length);
            return null;
        }
        
        try {
            String teamId = record[0].trim();
            String name = record[1].trim();
            String city = record[2].trim();
            String league = record[3].trim();
            String foundedStr = record[4].trim();
            String venue = record[5].trim();
            
            // Validate required fields
            if (teamId.isEmpty() || name.isEmpty() || city.isEmpty() || league.isEmpty()) {
                logger.warn("Missing required fields in record at line {}", lineNumber);
                return null;
            }
            
            LocalDate founded = null;
            if (!foundedStr.isEmpty()) {
                try {
                    founded = LocalDate.parse(foundedStr, DATE_FORMATTER);
                } catch (DateTimeParseException e) {
                    logger.warn("Invalid date format in record at line {}: {}", lineNumber, foundedStr);
                    return null;
                }
            }
            
            return new Team(teamId, name, city, league, founded, venue.isEmpty() ? null : venue);
            
        } catch (Exception e) {
            logger.error("Unexpected error parsing record at line {}: {}", lineNumber, e.getMessage());
            return null;
        }
    }
    
    public boolean validateCsvStructure(String filePath) {
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            List<String[]> records = csvReader.readAll();
            
            if (records.isEmpty()) {
                logger.error("CSV file is empty: {}", filePath);
                return false;
            }
            
            String[] headers = records.get(0);
            if (headers.length < 6) {
                logger.error("Invalid CSV structure: expected at least 6 columns, got {}", headers.length);
                return false;
            }
            
            return true;
            
        } catch (IOException | CsvException e) {
            logger.error("Error validating CSV file structure: {}", filePath, e);
            return false;
        }
    }
} 