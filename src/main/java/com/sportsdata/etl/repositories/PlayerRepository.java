package com.sportsdata.etl.repositories;

import com.sportsdata.etl.models.Player;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PlayerRepository extends JpaRepository<Player, String> {
    
    List<Player> findByTeamId(String teamId);
    
    List<Player> findByPosition(String position);
    
    List<Player> findByName(String name);
    
    @Query("SELECT p FROM Player p WHERE p.teamId = :teamId AND p.position = :position")
    List<Player> findByTeamIdAndPosition(@Param("teamId") String teamId, @Param("position") String position);
    
    @Query("SELECT p FROM Player p WHERE p.statistics.points > :minPoints ORDER BY p.statistics.points DESC")
    List<Player> findTopScorers(@Param("minPoints") Integer minPoints);
    
    @Query("SELECT COUNT(p) FROM Player p WHERE p.teamId = :teamId")
    long countByTeamId(@Param("teamId") String teamId);
} 