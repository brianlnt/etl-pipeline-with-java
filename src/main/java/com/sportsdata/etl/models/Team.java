package com.sportsdata.etl.models;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDate;
import java.util.Objects;

public class Team {
    
    private String teamId;
    
    @NotBlank
    private String name;
    
    @NotBlank
    private String city;
    
    @NotBlank
    private String league;
    
    @NotNull
    @PastOrPresent
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate founded;
    
    private String venue;
    
    // Constructors
    public Team() {}
    
    public Team(String teamId, String name, String city, String league, LocalDate founded, String venue) {
        this.teamId = teamId;
        this.name = name;
        this.city = city;
        this.league = league;
        this.founded = founded;
        this.venue = venue;
    }
    
    // Getters and Setters
    public String getTeamId() {
        return teamId;
    }
    
    public void setTeamId(String teamId) {
        this.teamId = teamId;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getCity() {
        return city;
    }
    
    public void setCity(String city) {
        this.city = city;
    }
    
    public String getLeague() {
        return league;
    }
    
    public void setLeague(String league) {
        this.league = league;
    }
    
    public LocalDate getFounded() {
        return founded;
    }
    
    public void setFounded(LocalDate founded) {
        this.founded = founded;
    }
    
    public String getVenue() {
        return venue;
    }
    
    public void setVenue(String venue) {
        this.venue = venue;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Team team = (Team) o;
        return Objects.equals(teamId, team.teamId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(teamId);
    }
    
    @Override
    public String toString() {
        return "Team{" +
                "teamId='" + teamId + '\'' +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                ", league='" + league + '\'' +
                ", founded=" + founded +
                ", venue='" + venue + '\'' +
                '}';
    }
} 