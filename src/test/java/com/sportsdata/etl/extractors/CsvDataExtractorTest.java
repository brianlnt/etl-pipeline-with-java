package com.sportsdata.etl.extractors;

import com.sportsdata.etl.models.Team;
import com.sportsdata.etl.services.extractors.CsvDataExtractor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvDataExtractorTest {
    
    private CsvDataExtractor csvExtractor;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        csvExtractor = new CsvDataExtractor();
    }
    
    @Test
    void testExtractTeams_ValidCsv() throws IOException {
        // Create a valid CSV file
        String csvContent = """
            teamId,name,city,league,founded,venue
            LAL,Los Angeles Lakers,Los Angeles,NBA,1947-01-01,Crypto.com Arena
            GSW,Golden State Warriors,San Francisco,NBA,1946-01-01,Chase Center
            """;
        
        Path csvFile = tempDir.resolve("teams.csv");
        Files.writeString(csvFile, csvContent);
        
        List<Team> teams = csvExtractor.extractTeams(csvFile.toString());
        
        assertEquals(2, teams.size());
        
        Team lakers = teams.get(0);
        assertEquals("LAL", lakers.getTeamId());
        assertEquals("Los Angeles Lakers", lakers.getName());
        assertEquals("Los Angeles", lakers.getCity());
        assertEquals("NBA", lakers.getLeague());
        assertEquals(LocalDate.of(1947, 1, 1), lakers.getFounded());
        assertEquals("Crypto.com Arena", lakers.getVenue());
        
        Team warriors = teams.get(1);
        assertEquals("GSW", warriors.getTeamId());
        assertEquals("Golden State Warriors", warriors.getName());
    }
    
    @Test
    void testExtractTeams_EmptyFile() throws IOException {
        Path csvFile = tempDir.resolve("empty.csv");
        Files.writeString(csvFile, "");
        
        List<Team> teams = csvExtractor.extractTeams(csvFile.toString());
        
        assertTrue(teams.isEmpty());
    }
    
    @Test
    void testExtractTeams_HeaderOnly() throws IOException {
        String csvContent = "teamId,name,city,league,founded,venue\n";
        
        Path csvFile = tempDir.resolve("header_only.csv");
        Files.writeString(csvFile, csvContent);
        
        List<Team> teams = csvExtractor.extractTeams(csvFile.toString());
        
        assertTrue(teams.isEmpty());
    }
    
    @Test
    void testExtractTeams_InvalidDate() throws IOException {
        String csvContent = """
            teamId,name,city,league,founded,venue
            LAL,Los Angeles Lakers,Los Angeles,NBA,invalid-date,Crypto.com Arena
            """;
        
        Path csvFile = tempDir.resolve("invalid_date.csv");
        Files.writeString(csvFile, csvContent);
        
        List<Team> teams = csvExtractor.extractTeams(csvFile.toString());
        
        assertTrue(teams.isEmpty()); // Should skip invalid records
    }
    
    @Test
    void testExtractTeams_MissingRequiredFields() throws IOException {
        String csvContent = """
            teamId,name,city,league,founded,venue
            ,Los Angeles Lakers,Los Angeles,NBA,1947-01-01,Crypto.com Arena
            LAL,,Los Angeles,NBA,1947-01-01,Crypto.com Arena
            """;
        
        Path csvFile = tempDir.resolve("missing_fields.csv");
        Files.writeString(csvFile, csvContent);
        
        List<Team> teams = csvExtractor.extractTeams(csvFile.toString());
        
        assertTrue(teams.isEmpty()); // Should skip records with missing required fields
    }
    
    @Test
    void testExtractTeams_InsufficientColumns() throws IOException {
        String csvContent = """
            teamId,name,city
            LAL,Los Angeles Lakers,Los Angeles
            """;
        
        Path csvFile = tempDir.resolve("insufficient_columns.csv");
        Files.writeString(csvFile, csvContent);
        
        List<Team> teams = csvExtractor.extractTeams(csvFile.toString());
        
        assertTrue(teams.isEmpty()); // Should skip records with insufficient columns
    }
    
    @Test
    void testExtractTeams_NonExistentFile() {
        List<Team> teams = csvExtractor.extractTeams("non_existent_file.csv");
        
        assertTrue(teams.isEmpty());
    }
    
    @Test
    void testValidateCsvStructure_ValidFile() throws IOException {
        String csvContent = """
            teamId,name,city,league,founded,venue
            LAL,Los Angeles Lakers,Los Angeles,NBA,1947-01-01,Crypto.com Arena
            """;
        
        Path csvFile = tempDir.resolve("valid.csv");
        Files.writeString(csvFile, csvContent);
        
        boolean isValid = csvExtractor.validateCsvStructure(csvFile.toString());
        
        assertTrue(isValid);
    }
    
    @Test
    void testValidateCsvStructure_InvalidFile() throws IOException {
        String csvContent = """
            teamId,name
            LAL,Los Angeles Lakers
            """;
        
        Path csvFile = tempDir.resolve("invalid.csv");
        Files.writeString(csvFile, csvContent);
        
        boolean isValid = csvExtractor.validateCsvStructure(csvFile.toString());
        
        assertFalse(isValid);
    }
    
    @Test
    void testValidateCsvStructure_EmptyFile() throws IOException {
        Path csvFile = tempDir.resolve("empty.csv");
        Files.writeString(csvFile, "");
        
        boolean isValid = csvExtractor.validateCsvStructure(csvFile.toString());
        
        assertFalse(isValid);
    }
} 