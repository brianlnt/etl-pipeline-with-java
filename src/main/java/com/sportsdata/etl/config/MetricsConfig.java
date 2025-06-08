package com.sportsdata.etl.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public MeterRegistry meterRegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().commonTags("application", "sports-etl-pipeline");
        return registry;
    }
} 