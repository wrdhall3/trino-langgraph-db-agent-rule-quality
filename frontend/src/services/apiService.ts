import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
apiClient.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to: ${config.url}`);
    return config;
  },
  (error) => {
    console.error('Request error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => {
    return response.data;
  },
  (error) => {
    console.error('Response error:', error);
    
    if (error.response) {
      // Server responded with error status
      const errorMessage = error.response.data?.detail || 
                          error.response.data?.message || 
                          `Server error: ${error.response.status}`;
      throw new Error(errorMessage);
    } else if (error.request) {
      // Request was made but no response received
      throw new Error('No response from server. Please check if the backend is running.');
    } else {
      // Something else happened
      throw new Error('Request failed: ' + error.message);
    }
  }
);

export const apiService = {
  // Health check
  healthCheck: () => apiClient.get('/health'),

  // GraphDB endpoints
  convertNaturalLanguageToCypher: (query: string, context?: string) =>
    apiClient.post('/api/graphdb/nl-to-cypher', { query, context }),

  executeCypher: (cypher: string) =>
    apiClient.post('/api/graphdb/execute-cypher', { cypher }),

  getGraphSchema: () =>
    apiClient.get('/api/graphdb/schema'),

  getSampleQueries: () =>
    apiClient.get('/api/graphdb/sample-queries'),

  // Data Quality endpoints
  analyzeDQ: (uitids?: string[]) =>
    apiClient.post('/api/dq/analyze', { uitids }),

  getViolations: () =>
    apiClient.get('/api/dq/violations'),

  getDQRules: () =>
    apiClient.get('/api/dq/rules'),

  getCDEs: () =>
    apiClient.get('/api/dq/cdes'),

  getSystemsInfo: () =>
    apiClient.get('/api/dq/systems'),
};

export default apiService; 