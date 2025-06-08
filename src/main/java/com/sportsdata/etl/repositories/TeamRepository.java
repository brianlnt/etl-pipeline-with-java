package com.sportsdata.etl.repositories;

import com.sportsdata.etl.models.Team;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TeamRepository extends JpaRepository<Team, String> {
    
    Optional<Team> findByName(String name);
    
    List<Team> findByLeague(String league);
    
    List<Team> findByCity(String city);
    
    @Query("SELECT t FROM Team t WHERE t.league = :league AND t.city = :city")
    List<Team> findByLeagueAndCity(@Param("league") String league, @Param("city") String city);
    
    @Query("SELECT COUNT(t) FROM Team t WHERE t.league = :league")
    long countByLeague(@Param("league") String league);
} 