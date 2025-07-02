# Trino Setup Guide

This guide provides multiple options for setting up Trino for the data quality system.

## Option 1: Docker Compose (Recommended for Development)

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB of available RAM

### Quick Start

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Verify Trino is running:**
   ```bash
   # Check if Trino is accessible
   curl http://localhost:8080/v1/info
   ```

3. **Access Trino Web UI:**
   Open your browser to `http://localhost:8080`

4. **Test database connections:**
   ```bash
   # Connect to Trino CLI (if you have it installed)
   trino --server localhost:8080 --catalog trade_system
   
   # Or use the Python client
   python -c "
import trino
conn = trino.dbapi.connect(host='localhost', port=8080, user='admin', catalog='trade_system', schema='trade_system')
cursor = conn.cursor()
cursor.execute('SHOW TABLES')
print(cursor.fetchall())
"
   ```

### Services Included

- **Trino**: Port 8080 (Web UI and API) - connects to your existing MySQL databases

### Prerequisites

**MySQL Databases (Your Existing Setup):**
You need to have the following databases already set up on localhost:3306:
- `trade_system` database with `trade` table
- `settlement_system` database with `trade` table  
- `reporting_system` database with `trade` table
- Username: `root`
- Password: `Ridgewood2024`

**Neo4j (Your Existing Setup):**
You need to have Neo4j running locally on:
- URI: `bolt://localhost:7687`
- Username: `neo4j`
- Password: `testtest`
- Database: `neo4j`

### Stopping Services

```bash
# Stop Trino service
docker-compose down
```

## Option 2: Local Installation

### Prerequisites
- Java 17+ installed
- MySQL Server running
- Neo4j running

### Steps

1. **Download Trino:**
   ```bash
   wget https://repo1.maven.org/maven2/io/trino/trino-server/435/trino-server-435.tar.gz
   tar -xzf trino-server-435.tar.gz
   cd trino-server-435
   ```

2. **Configure Trino:**
   Copy the configuration files from `trino-config/` to your Trino installation's `etc/` directory.

3. **Update catalog configurations:**
   Edit the catalog files in `etc/catalog/` to match your MySQL connection details:
   ```properties
   # etc/catalog/trade_system.properties
   connector.name=mysql
   connection-url=jdbc:mysql://localhost:3306/trade_system
   connection-user=your_mysql_user
   connection-password=your_mysql_password
   ```

4. **Start Trino:**
   ```bash
   bin/launcher run
   ```

## Option 3: Kubernetes Deployment

### Prerequisites
- Kubernetes cluster
- kubectl configured
- Helm installed (optional)

### Using Helm (Recommended)

1. **Add Trino Helm repository:**
   ```bash
   helm repo add trino https://trinodb.github.io/charts
   helm repo update
   ```

2. **Create values file:**
   ```yaml
   # values.yaml
   server:
     workers: 2
     
   coordinator:
     jvm:
       maxHeapSize: "2G"
       
   worker:
     jvm:
       maxHeapSize: "2G"
   
   additionalCatalogs:
     trade_system: |
       connector.name=mysql
       connection-url=jdbc:mysql://mysql-trade:3306/trade_system
       connection-user=trino_user
       connection-password=trino_pass
     
     settlement_system: |
       connector.name=mysql
       connection-url=jdbc:mysql://mysql-settlement:3306/settlement_system
       connection-user=trino_user
       connection-password=trino_pass
       
     reporting_system: |
       connector.name=mysql
       connection-url=jdbc:mysql://mysql-reporting:3306/reporting_system
       connection-user=trino_user
       connection-password=trino_pass
   ```

3. **Deploy Trino:**
   ```bash
   helm install trino trino/trino -f values.yaml
   ```

## Configuration for the Data Quality System

### Update your .env file

After setting up Trino, update your `.env` file with the correct connection details:

```env
# Trino Configuration
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=admin
TRINO_CATALOG=trade_system
TRINO_SCHEMA=trade_system

# MySQL Database Settings (Your Existing Databases)
TRADE_DB_HOST=localhost
TRADE_DB_PORT=3306
TRADE_DB_NAME=trade_system
TRADE_DB_USER=root
TRADE_DB_PASSWORD=Ridgewood2024
TRADE_DB_SCHEMA=trade_system

SETTLEMENT_DB_HOST=localhost
SETTLEMENT_DB_PORT=3306
SETTLEMENT_DB_NAME=settlement_system
SETTLEMENT_DB_USER=root
SETTLEMENT_DB_PASSWORD=Ridgewood2024
SETTLEMENT_DB_SCHEMA=settlement_system

REPORTING_DB_HOST=localhost
REPORTING_DB_PORT=3306
REPORTING_DB_NAME=reporting_system
REPORTING_DB_USER=root
REPORTING_DB_PASSWORD=Ridgewood2024
REPORTING_DB_SCHEMA=reporting_system

# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=testtest
NEO4J_DATABASE=neo4j
```

## Testing the Setup

### 1. Test Trino Connectivity

```python
import trino

# Test connection
conn = trino.Connection(
    host='localhost',
    port=8080,
    user='admin',
    catalog='trade_system'
)

cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM trade_system.trade_system.trade')
result = cursor.fetchone()
print(f"Trade records in trade system: {result[0]}")
```

### 2. Test Cross-Database Queries

```sql
-- Query across all three systems
SELECT 
    t.uitid,
    t.symbol as trade_symbol,
    s.symbol as settlement_symbol,
    r.symbol as reporting_symbol
FROM trade_system.trade_system.trade t
LEFT JOIN settlement_system.settlement_system.trade s ON t.uitid = s.uitid
LEFT JOIN reporting_system.reporting_system.trade r ON t.uitid = r.uitid;
```

### 3. Run the Data Quality System

```bash
# Test the quick start
python quick_start.py

# Run the full workflow
python sample_dq_workflow.py
```

## Troubleshooting

### Common Issues

1. **Trino won't start:**
   - Check Java version (requires Java 17+)
   - Verify memory settings in `jvm.config`
   - Check port 8080 is not in use

2. **Can't connect to MySQL:**
   - Verify MySQL containers are running: `docker-compose ps`
   - Check network connectivity between containers
   - Verify credentials in catalog configurations

3. **Out of memory errors:**
   - Increase memory limits in `docker-compose.yml`
   - Adjust JVM heap size in `jvm.config`

4. **Query failures:**
   - Check Trino logs: `docker-compose logs trino`
   - Verify table schemas match between systems
   - Check data types compatibility

### Useful Commands

```bash
# View Trino logs
docker-compose logs -f trino

# Connect to your local MySQL databases
mysql -u root -pRidgewood2024 trade_system
mysql -u root -pRidgewood2024 settlement_system
mysql -u root -pRidgewood2024 reporting_system

# Access Neo4j browser (your local instance)
# Open http://localhost:7474 in your browser

# Check Trino container status
docker-compose ps

# Restart Trino service
docker-compose restart trino
```

## Performance Tuning

### For Production Use

1. **Increase memory allocation:**
   ```properties
   # config.properties
   query.max-memory=8GB
   query.max-memory-per-node=4GB
   query.max-total-memory-per-node=4GB
   ```

2. **Add worker nodes:**
   ```yaml
   # docker-compose.yml - add worker nodes
   trino-worker-1:
     image: trinodb/trino:latest
     volumes:
       - ./trino-config:/etc/trino
     environment:
       - TRINO_ENVIRONMENT=production
     depends_on:
       - trino
   ```

3. **Optimize MySQL connections:**
   ```properties
   # catalog configuration
   connection-url=jdbc:mysql://host:3306/db?useSSL=false&allowPublicKeyRetrieval=true
   mysql.connection-pool.max-size=50
   ```

This setup provides a complete Trino environment for the data quality system with sample data that includes intentional violations for testing the DQ rules. 