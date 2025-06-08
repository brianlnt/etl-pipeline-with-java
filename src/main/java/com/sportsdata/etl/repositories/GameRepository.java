package com.sportsdata.etl.repositories;

import com.sportsdata.etl.models.Game;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface GameRepository extends JpaRepository<Game, String> {
    
    @Query("SELECT g FROM Game g WHERE g.homeTeamId = :teamId OR g.awayTeamId = :teamId")
    List<Game> findByTeamId(@Param("teamId") String teamId);
    
    List<Game> findByStatus(String status);
    
    @Query("SELECT g FROM Game g WHERE g.date BETWEEN :startDate AND :endDate")
    List<Game> findByDateRange(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);
    
    @Query("SELECT g FROM Game g WHERE g.homeTeamId = :homeTeamId AND g.awayTeamId = :awayTeamId")
    List<Game> findByTeams(@Param("homeTeamId") String homeTeamId, @Param("awayTeamId") String awayTeamId);
    
    @Query("SELECT COUNT(g) FROM Game g WHERE g.homeTeamId = :teamId OR g.awayTeamId = :teamId")
    long countByTeamId(@Param("teamId") String teamId);
} 