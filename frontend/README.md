# Data Quality Management UI

A React TypeScript application for managing data quality rules and violations with natural language GraphDB querying capabilities.

## Features

- **Natural Language GraphDB Query**: Convert natural language queries to Cypher and execute them against Neo4j
- **Data Quality Violations Dashboard**: View and analyze DQ violations with filtering and sorting
- **Interactive Dashboard**: Overview of system health, metrics, and recent activity
- **Modern UI**: Built with Ant Design for a professional look and feel

## Prerequisites

- Node.js 16 or higher
- npm or yarn
- Backend API running on port 8000

## Installation

1. Install dependencies:
```bash
npm install
```

2. Create a `.env` file in the frontend directory:
```
REACT_APP_API_URL=http://localhost:8000
GENERATE_SOURCEMAP=false
SKIP_PREFLIGHT_CHECK=true
```

3. Start the development server:
```bash
npm start
```

The application will be available at `http://localhost:3000`.

## Environment Variables

- `REACT_APP_API_URL`: Backend API URL (default: http://localhost:8000)

## Available Scripts

- `npm start`: Start the development server
- `npm build`: Build for production
- `npm test`: Run tests
- `npm run eject`: Eject from Create React App

## Project Structure

```
src/
├── components/          # Reusable components
├── pages/              # Page components
│   ├── DashboardPage.tsx
│   ├── GraphQueryPage.tsx
│   └── DQViolationsPage.tsx
├── services/           # API services
│   └── apiService.ts
├── App.tsx             # Main app component
├── index.tsx           # App entry point
└── index.css           # Global styles
```

## Key Components

### GraphQueryPage
- Natural language to Cypher conversion
- Cypher query editor with syntax highlighting
- Query execution and result display
- Sample queries and schema information

### DQViolationsPage
- Data quality violations table
- Advanced filtering and sorting
- Violation details modal
- Summary statistics

### DashboardPage
- Key metrics overview
- Health indicators
- Charts and visualizations
- Quick actions panel

## API Integration

The app communicates with the FastAPI backend through the `apiService` which handles:
- Natural language to Cypher conversion
- GraphDB query execution
- DQ violations management
- System information retrieval

## Styling

- Uses Ant Design component library
- Custom CSS for DQ-specific styling
- Responsive design for mobile devices
- Professional color scheme

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## Development

To add new features:
1. Create components in `src/components/`
2. Add pages in `src/pages/`
3. Update API service in `src/services/apiService.ts`
4. Update routing in `src/App.tsx` 