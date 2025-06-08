# Sports Data ETL Pipeline

A comprehensive Extract, Transform, Load (ETL) pipeline for processing sports data from multiple sources. This Spring Boot application demonstrates enterprise-grade data processing with robust error handling, data quality validation, and monitoring capabilities.

## 🏗️ Architecture Overview

The ETL pipeline follows a modular architecture with clear separation of concerns:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EXTRACTION    │    │ TRANSFORMATION  │    │    LOADING      │
│                 │    │                 │    │                 │
│ • CSV Extractor │───▶│ • Data Validator│───▶│ • Database      │
│ • JSON Extractor│    │ • Data Cleaner  │    │   Loader        │
│ • XML Extractor │    │ • Data Standard │    │ • Batch Insert  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ QUALITY CONTROL │
                       │                 │
                       │ • Validation    │
                       │ • Quality Report│
                       │ • Metrics       │
                       └─────────────────┘
```

## 🚀 Features

### Core ETL Capabilities
- **Multi-format Data Extraction**: CSV, JSON, and XML support
- **Comprehensive Data Validation**: Business rules and data quality checks
- **Data Transformation**: Cleaning, standardization, and normalization
- **Transactional Loading**: Safe database operations with rollback support
- **Error Handling**: Robust error recovery and logging

### Data Quality & Monitoring
- **Real-time Quality Assessment**: Automated data quality scoring
- **Comprehensive Metrics**: Performance and data quality metrics
- **Detailed Logging**: Structured logging with multiple levels
- **Health Monitoring**: Application health checks and status endpoints

### REST API
- **Pipeline Execution**: Trigger ETL processes via REST endpoints
- **Status Monitoring**: Real-time pipeline status and metrics
- **Quality Reports**: On-demand data quality assessments
- **Data Management**: Clear and reset database operations

## 📊 Data Models

### Team
- **teamId**: Unique identifier
- **name**: Team name
- **city**: Team location
- **league**: League affiliation (NBA, NCAA, etc.)
- **founded**: Foundation date
- **venue**: Home venue

### Player
- **playerId**: Unique identifier
- **name**: Player name
- **teamId**: Associated team
- **position**: Playing position
- **age**: Player age
- **statistics**: Embedded statistics (games, points, assists)

### Game
- **gameId**: Unique identifier
- **homeTeamId**: Home team
- **awayTeamId**: Away team
- **date**: Game date and time
- **homeScore**: Home team score
- **awayScore**: Away team score
- **status**: Game status (Scheduled, Live, Final, etc.)

## 🛠️ Technology Stack

- **Framework**: Spring Boot 3.2.0
- **Language**: Java 17
- **Database**: H2 (development), PostgreSQL (production)
- **Data Processing**: OpenCSV, Jackson JSON, DOM XML Parser
- **Testing**: JUnit 5, Spring Boot Test
- **Monitoring**: Micrometer, Spring Actuator
- **Build Tool**: Maven

## 📋 Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- Git

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <repository-url>
cd etl-pipeline-with-java
```

### 2. Build the Project
```bash
mvn clean compile
```

### 3. Run Tests
```bash
mvn test
```

### 4. Start the Application
```bash
mvn spring-boot:run
```

The application will start on `http://localhost:8080/api/v1`

### 5. Execute ETL Pipeline
```bash
# Using default sample data
curl -X POST http://localhost:8080/api/v1/etl/execute

# Or with custom data sources
curl -X POST http://localhost:8080/api/v1/etl/execute \
  -H "Content-Type: application/json" \
  -d '{
    "teamsCsvPath": "path/to/teams.csv",
    "playersJsonPath": "path/to/players.json",
    "gamesXmlPath": "path/to/games.xml"
  }'
```

## 📁 Project Structure

```
src/
├── main/
│   ├── java/com/sportsdata/etl/
│   │   ├── controllers/          # REST API controllers
│   │   ├── extractors/           # Data extraction components
│   │   ├── loaders/              # Database loading components
│   │   ├── models/               # Data models and entities
│   │   ├── pipeline/             # ETL pipeline orchestration
│   │   ├── quality/              # Data quality and validation
│   │   ├── repositories/         # Data access layer
│   │   ├── transformers/         # Data transformation logic
│   │   └── utils/                # Utility classes
│   └── resources/
│       ├── sample-data/          # Sample CSV, JSON, XML files
│       └── application.yml       # Configuration
└── test/                         # Comprehensive test suite
```

## 🔧 Configuration

### Application Profiles

#### Development (default)
- H2 in-memory database
- Detailed logging
- H2 console enabled at `/h2-console`

#### Production
```bash
mvn spring-boot:run -Dspring.profiles.active=production
```
- PostgreSQL database
- Optimized logging
- Production-ready settings

#### Test
```bash
mvn test -Dspring.profiles.active=test
```
- Isolated test database
- Debug logging

### Environment Variables

For production deployment:
```bash
export DB_USERNAME=your_db_user
export DB_PASSWORD=your_db_password
export SPRING_PROFILES_ACTIVE=production
```

## 📊 API Endpoints

### ETL Operations
- `POST /api/v1/etl/execute` - Execute ETL pipeline
- `GET /api/v1/etl/status` - Get pipeline status
- `GET /api/v1/etl/quality-report` - Generate quality report
- `DELETE /api/v1/etl/data` - Clear all data
- `GET /api/v1/etl/health` - Health check

### Monitoring
- `GET /api/v1/actuator/health` - Application health
- `GET /api/v1/actuator/metrics` - Application metrics
- `GET /api/v1/actuator/prometheus` - Prometheus metrics

## 📈 Sample Data

The project includes sample data files:

### Teams (CSV)
```csv
teamId,name,city,league,founded,venue
LAL,Los Angeles Lakers,Los Angeles,NBA,1947-01-01,Crypto.com Arena
GSW,Golden State Warriors,San Francisco,NBA,1946-01-01,Chase Center
```

### Players (JSON)
```json
[
  {
    "playerId": "P001",
    "name": "LeBron James",
    "teamId": "LAL",
    "position": "Small Forward",
    "age": 39,
    "statistics": {
      "gamesPlayed": 71,
      "points": 1714,
      "assists": 654
    }
  }
]
```

### Games (XML)
```xml
<games>
  <game>
    <gameId>G001</gameId>
    <homeTeamId>LAL</homeTeamId>
    <awayTeamId>GSW</awayTeamId>
    <date>2024-01-15 19:30:00</date>
    <homeScore>118</homeScore>
    <awayScore>124</awayScore>
    <status>Final</status>
  </game>
</games>
```

## 🧪 Testing

### Run All Tests
```bash
mvn test
```

### Run Specific Test Classes
```bash
mvn test -Dtest=CsvDataExtractorTest
mvn test -Dtest=ValidationRulesTest
```

### Test Coverage
```bash
mvn jacoco:report
```
View coverage report at `target/site/jacoco/index.html`

## 📊 Data Quality Features

### Validation Rules
- **Required Fields**: Ensures all mandatory fields are present
- **Data Types**: Validates correct data types and formats
- **Business Rules**: Enforces domain-specific constraints
- **Referential Integrity**: Validates relationships between entities

### Quality Metrics
- **Completeness**: Percentage of complete records
- **Consistency**: Data consistency across sources
- **Accuracy**: Data accuracy validation
- **Distribution**: Data distribution analysis

### Quality Scoring
- **Excellent**: 90-100% quality score
- **Good**: 80-89% quality score
- **Fair**: 70-79% quality score
- **Poor**: Below 70% quality score

## 🔍 Monitoring & Observability

### Metrics Collection
- **Pipeline Performance**: Execution times and throughput
- **Data Quality**: Quality scores and validation results
- **System Health**: Memory usage, CPU, and database metrics
- **Error Tracking**: Error rates and failure analysis

### Logging
- **Structured Logging**: JSON-formatted logs for easy parsing
- **Multiple Levels**: DEBUG, INFO, WARN, ERROR
- **Contextual Information**: Request IDs, timestamps, and metadata

## 🚀 Deployment

### Docker Deployment
```dockerfile
FROM openjdk:17-jdk-slim
COPY target/etl-pipeline-1.0.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

### Build and Run
```bash
mvn clean package
docker build -t sports-etl-pipeline .
docker run -p 8080:8080 sports-etl-pipeline
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:
- Create an issue in the GitHub repository
- Check the documentation in the `/docs` folder
- Review the test cases for usage examples

## 🔮 Future Enhancements

- **Real-time Streaming**: Apache Kafka integration
- **Advanced Analytics**: Machine learning-based quality assessment
- **Web UI**: React-based dashboard for pipeline management
- **Multi-tenant Support**: Support for multiple organizations
- **Cloud Integration**: AWS/Azure/GCP deployment templates 