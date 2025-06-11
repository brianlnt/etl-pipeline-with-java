package com.sportsdata.etl;

import com.sportsdata.etl.pipeline.EtlPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ScheduledETLRunner implements ApplicationRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(ScheduledETLRunner.class);
    
    @Autowired
    private EtlPipeline etlPipeline;
    
    @Autowired
    private Environment environment;
    
    @Override
    public void run(ApplicationArguments args) throws Exception {
        String etlMode = environment.getProperty("ETL_MODE", "NORMAL");
        
        if ("SCHEDULED".equals(etlMode)) {
            logger.info("🚀 Starting ETL pipeline in SCHEDULED mode for interview demo");
            
            try {
                // Create pipeline configuration
                EtlPipeline.PipelineConfig config = new EtlPipeline.PipelineConfig(
                    "src/main/resources/sample-data/teams.csv",
                    "src/main/resources/sample-data/players.json",
                    "src/main/resources/sample-data/games.xml"
                );
                
                // Execute the pipeline
                EtlPipeline.PipelineResult result = etlPipeline.executeFullPipeline(config);
                
                if (result.isSuccess()) {
                    logger.info("✅ ETL pipeline completed successfully!");
                    logger.info("📊 Pipeline ID: {}", result.getPipelineId());
                    logger.info("⏱️  Execution time: {} ms", result.getDurationMs());
                    
                    // Log summary for demo
                    if (result.getLoadResult() != null) {
                        logger.info("📈 Data loaded - Teams: {}, Players: {}, Games: {}", 
                            result.getLoadResult().getTeamsLoaded(),
                            result.getLoadResult().getPlayersLoaded(),
                            result.getLoadResult().getGamesLoaded());
                    }
                    
                    logger.info("🎉 Interview demo completed successfully!");
                    
                    // Exit after scheduled run
                    System.exit(0);
                } else {
                    logger.error("❌ ETL pipeline failed: {}", result.getErrorMessage());
                    System.exit(1);
                }
                
            } catch (Exception e) {
                logger.error("💥 Error running ETL pipeline", e);
                System.exit(1);
            }
        } else {
            logger.info("🌐 Starting ETL pipeline in NORMAL mode (web server)");
            logger.info("🔗 REST API available at: http://localhost:8080/api/v1/etl/");
            logger.info("🏥 Health check: http://localhost:8080/api/v1/etl/health");
        }
    }
} 