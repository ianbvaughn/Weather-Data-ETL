# Weather Data ETL

A containerized ETL pipeline that collects hourly weather data for San Francisco using Apache Airflow and PostgreSQL.

## Overview

This project implements an automated Extract, Transform, Load (ETL) pipeline that fetches weather data from the OpenWeather API, processes it using pandas, and stores it in a PostgreSQL database. The pipeline runs daily via Apache Airflow, making it easy to track weather patterns over time.

## Features

- **Automated Data Collection**: Fetches hourly weather data daily for San Francisco
- **Data Processing**: Cleans and transforms raw weather data using pandas
- **Persistent Storage**: Stores processed data in PostgreSQL for analysis
- **Containerized**: Fully containerized with Docker for easy deployment
- **Orchestrated**: Uses Apache Airflow for reliable task scheduling and monitoring

## Architecture

The pipeline consists of three main stages:

1. **Extract**: Retrieves hourly weather data from the OpenWeather API
2. **Transform**: Uses pandas to extract relevant fields:
   - Datetime (hourly intervals)
   - Current temperature (Celsius)
   - "Feels like" temperature (Celsius)
3. **Load**: Writes the processed DataFrame to PostgreSQL

## Prerequisites

- Docker and Docker Compose
- OpenWeather API account (free tier)
- Git

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/ianbvaughn/Weather-Data-ETL.git
cd Weather-Data-ETL
```

### 2. Configure Environment Variables

Create a `.env` file in the project root directory with the following variables:

```env
OPEN_WEATHER_API='<YOUR_API_KEY>'
AIRFLOW__API_AUTH__JWT_SECRET='<YOUR_JWT_SECRET>'
```

#### Getting Your API Keys

**OpenWeather API Key:**
1. Sign up at [OpenWeather](https://openweathermap.org/)
2. Subscribe to the free tier (allows sufficient API calls per month)
3. Copy your API key from the dashboard

**Airflow JWT Secret:**
1. Build the Docker image for the first time (see Installation section)
2. Locate the `airflow.cfg` file that gets generated
3. Copy the JWT secret value from the configuration file

### 3. Build and Run

Build and start the containers:

```bash
docker-compose up -d
```

### 4. Access Airflow

Once the containers are running, access the Airflow web UI at:

```
http://localhost:8080
```

Default credentials can be found in your `docker-compose.yaml` file.

## Usage

The ETL pipeline runs automatically on a daily schedule. You can also:

- **Trigger manually**: Use the Airflow UI to trigger the DAG on demand
- **Monitor progress**: View task execution logs and status in the Airflow dashboard
- **Query data**: Connect to the PostgreSQL database to analyze collected weather data

## Project Structure

```
Weather-Data-ETL/
├── dags/                   # Airflow DAG definitions
├── docker-compose.yaml     # Docker services configuration
├── Dockerfile              # Custom Airflow image
├── .gitignore              # Git ignore rules
├── .env                    # Environment variables (create this)
└── README.md               # Project documentation
```

## Technologies Used

- **Apache Airflow**: Workflow orchestration and scheduling
- **PostgreSQL**: Data storage and persistence
- **Python**: ETL logic and data processing
- **Pandas**: Data transformation and cleaning
- **Docker**: Containerization and deployment
- **OpenWeather API**: Weather data source

## Data Schema

The pipeline collects the following data points:

| Field | Type | Description |
|-------|------|-------------|
| datetime | timestamp | Hourly timestamp of the weather reading |
| temperature | float | Current temperature in Celsius |
| feels_like | float | "Feels like" temperature in Celsius |

## Troubleshooting

**Containers won't start:**
- Ensure Docker is running
- Check that ports 8080 and 5432 are not in use
- Verify your `.env` file is properly configured

**API calls failing:**
- Verify your OpenWeather API key is valid
- Check that you haven't exceeded your API call limit
- Ensure your API key has proper permissions

**Database connection errors:**
- Confirm PostgreSQL container is running
- Check database credentials in `docker-compose.yaml`
- Verify network connectivity between containers

## Future Enhancements

Potential improvements for future versions:

- Support for multiple cities
- Additional weather metrics (humidity, pressure, wind speed)
- Data visualization dashboard
- Alert system for extreme weather conditions
- Historical data backfill functionality
- API rate limiting and retry logic

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

## Version History

- **v1.0** - Initial release with basic ETL functionality for San Francisco weather data

## Contact

For questions or support, please open an issue on the GitHub repository.
