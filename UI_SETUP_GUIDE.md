# Data Quality Management UI - Setup Guide

This guide will help you set up and run the complete Data Quality Management System with its new React-based user interface.

## 🎯 Overview

The system now includes:
- **FastAPI Backend**: RESTful API for data quality operations and natural language to Cypher conversion
- **React Frontend**: Modern web interface for GraphDB querying and DQ violations management
- **Existing Infrastructure**: Neo4j, Trino, and MySQL databases

## 🛠️ Prerequisites

Before starting, ensure you have:

1. **Python 3.8+** - Backend API server
2. **Node.js 16+** - Frontend development (optional, only needed for React UI)
3. **Local Database Services** - Neo4j, Trino, and MySQL running locally
4. **Git** - Version control

### Verification Commands
```bash
python --version    # Should be 3.8+
node --version      # Should be 16+ (optional)
npm --version       # Comes with Node.js (optional)
```

## 📦 Installation Steps

### 1. Verify Database Services are Running

Make sure your local services are running:
- **Neo4j**: Should be accessible at `http://localhost:7474`
- **Trino**: Should be accessible at `http://localhost:8080`
- **MySQL**: Should be accessible for Trino to connect to

### Verification Commands
```bash
# Test Neo4j connection
curl -u neo4j:password http://localhost:7474/db/data/

# Test Trino connection
curl http://localhost:8080/v1/info

# Check if services are listening on expected ports
netstat -an | grep -E "(7474|8080|3306)"
```

### 2. Set Up Backend Environment

Create a `.env` file in the project root:
```bash
# Database configurations (adjust to match your local setup)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password
TRINO_HOST=localhost
TRINO_PORT=8080

# OpenAI API (for natural language processing)
OPENAI_API_KEY=your_openai_api_key_here

# Optional: Logging level
LOG_LEVEL=INFO
```

**Important**: Update the Neo4j password to match your local installation. The default password is often `password` or `neo4j`, but you should use whatever you set during Neo4j installation.

### 3. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set Up Frontend Environment

Navigate to frontend directory and install dependencies:
```bash
cd frontend
npm install
```

Create `.env` file in the frontend directory:
```
REACT_APP_API_URL=http://localhost:8000
GENERATE_SOURCEMAP=false
SKIP_PREFLIGHT_CHECK=true
```

## 🚀 Running the System

### Option 1: Automated Startup (Recommended)
```bash
# From project root
python start_dq_system.py
```

This script will:
- Check prerequisites
- Install dependencies
- Start both backend and frontend servers
- Provide status information

### Option 2: Manual Startup

**Terminal 1 - Backend:**
```bash
cd /path/to/project
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

**Terminal 2 - Frontend (Optional):**
```bash
cd /path/to/project/frontend
npm start
```

### Option 3: Backend Only (No Node.js Required)

If you don't have Node.js installed, you can run just the backend:
```bash
# Start backend only
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

# Test functionality with demo script
python demo_ui_features.py

# Access API documentation
# Open browser to: http://localhost:8000/docs
```

## 🌐 Access URLs

### With Full Setup (Backend + Frontend):
- **Main UI**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs

### With Backend Only:
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs (Interactive API testing)
- **Demo Script**: Run `python demo_ui_features.py` for functionality testing

### Local Database Services:
- **Neo4j Browser**: http://localhost:7474
- **Trino Web UI**: http://localhost:8080

### Available Without React UI:
Even without the React frontend, you can:
- Use the interactive API documentation at http://localhost:8000/docs
- Run the demo script to test all features
- Query the API directly with tools like curl or Postman
- Access natural language to Cypher conversion
- Execute GraphDB queries and get JSON responses
- Run data quality analysis and get violation reports

## 📱 Using the Web Interface

### Dashboard Page
- Overview of system health and metrics
- Key statistics on violations, rules, and CDEs
- Quick actions for common tasks
- Charts showing violation distribution

### Graph Query Page
- **Natural Language Input**: Enter queries like "Show me all data quality rules"
- **Cypher Generation**: Automatically converts to Cypher syntax
- **Query Execution**: Runs against Neo4j database
- **Results Display**: Formatted table view of results
- **Sample Queries**: Pre-built examples to get started
- **Schema Information**: View database structure

### DQ Violations Page
- **Violations Table**: Sortable, filterable list of all violations
- **Summary Statistics**: Count by severity, system, rule type
- **Advanced Filtering**: Search by text, filter by severity/system/rule type
- **Violation Details**: Modal with complete violation information
- **Run Analysis**: Trigger new data quality analysis

## 🔧 Configuration

### Backend Configuration

Edit `backend/main.py` to modify:
- CORS settings for different frontend URLs
- API endpoint configurations
- Logging levels

### Frontend Configuration

Edit `frontend/src/services/apiService.ts` to:
- Change API base URL
- Modify request/response interceptors
- Add authentication headers

### Adding New API Endpoints

1. Add endpoint to `backend/main.py`
2. Update `backend/services/` for business logic
3. Add corresponding service method in `frontend/src/services/apiService.ts`
4. Use in React components

## 🎨 Customization

### Styling
- Global styles: `frontend/src/index.css`
- Component styles: `frontend/src/App.css`
- Ant Design theme: `frontend/src/App.tsx`

### Adding New Pages
1. Create component in `frontend/src/pages/`
2. Add route in `frontend/src/App.tsx`
3. Update navigation menu

### Custom Components
- Create in `frontend/src/components/`
- Import and use in pages
- Follow Ant Design patterns

## 🔍 API Endpoints

### GraphDB Endpoints
- `POST /api/graphdb/nl-to-cypher` - Convert natural language to Cypher
- `POST /api/graphdb/execute-cypher` - Execute Cypher query
- `GET /api/graphdb/schema` - Get database schema
- `GET /api/graphdb/sample-queries` - Get sample queries

### Data Quality Endpoints
- `POST /api/dq/analyze` - Run DQ analysis
- `GET /api/dq/violations` - Get violations
- `GET /api/dq/rules` - Get all rules
- `GET /api/dq/cdes` - Get all CDEs
- `GET /api/dq/systems` - Get systems info

## 🔧 Troubleshooting

### Backend Issues
```bash
# Check if backend is running
curl http://localhost:8000/health

# View backend logs
tail -f backend.log
```

### Frontend Issues
```bash
# Clear npm cache
npm cache clean --force

# Reinstall dependencies
rm -rf node_modules package-lock.json
npm install

# Check for port conflicts
lsof -i :3000
```

### Database Connection Issues
```bash
# Check if Neo4j is running
curl -u neo4j:password http://localhost:7474/db/data/

# Check if Trino is running
curl http://localhost:8080/v1/info

# Check what's listening on database ports
netstat -an | grep -E "(7474|7687|8080|3306)"

# Check Neo4j status (if installed as service)
# On Windows: Get-Service Neo4j*
# On macOS/Linux: sudo systemctl status neo4j
```

### Common Issues

1. **Port Already in Use**
   - Kill process using the port: `kill -9 $(lsof -ti:8000)`
   - Or change port in configuration

2. **OpenAI API Key Missing**
   - Add to `.env` file
   - Natural language conversion will fail without it

3. **CORS Errors**
   - Check frontend URL in backend CORS settings
   - Ensure both servers are running

4. **Database Connection Errors**
   - Verify local services are running (Neo4j, Trino, MySQL)
   - Check database credentials in configuration
   - Ensure Neo4j password matches your local installation
   - Verify Trino can connect to MySQL databases

## 📊 Performance Tips

### Backend
- Use async/await for database operations
- Implement caching for schema information
- Use connection pooling for databases

### Frontend
- Implement pagination for large datasets
- Use React.memo for expensive components
- Lazy load pages with React.lazy

## 🔐 Security Considerations

### Production Deployment
- Change default passwords
- Use environment variables for secrets
- Implement authentication/authorization
- Enable HTTPS
- Add rate limiting

### Development
- Never commit API keys
- Use local environment files
- Validate all inputs
- Sanitize database queries

## 🤝 Contributing

### Adding Features
1. Create feature branch
2. Implement backend changes
3. Add frontend components
4. Update documentation
5. Submit pull request

### Code Style
- Follow PEP 8 for Python
- Use ESLint/Prettier for TypeScript
- Add type hints and JSDoc comments
- Write unit tests

## 📚 Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [React Documentation](https://reactjs.org/docs)
- [Ant Design Components](https://ant.design/components)
- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/)
- [Trino Documentation](https://trino.io/docs/)

## 🆘 Support

For issues and questions:
1. Check this documentation
2. Review error logs
3. Search existing issues
4. Create new issue with:
   - Error messages
   - Steps to reproduce
   - Environment details
   - Screenshots if applicable 