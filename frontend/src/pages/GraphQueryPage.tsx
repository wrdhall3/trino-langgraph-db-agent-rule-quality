import React, { useState, useEffect } from 'react';
import { 
  Row, 
  Col, 
  Card, 
  Input, 
  Button, 
  Space, 
  Table, 
  Typography, 
  Alert, 
  Spin,
  Tabs,
  Tag
} from 'antd';
import { 
  SearchOutlined, 
  PlayCircleOutlined, 
  DatabaseOutlined,
  CopyOutlined
} from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import { apiService } from '../services/apiService';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;
const { TabPane } = Tabs;

interface SampleQuery {
  natural_language: string;
  cypher: string;
  description: string;
}

const GraphQueryPage: React.FC = () => {
  const [naturalLanguageQuery, setNaturalLanguageQuery] = useState('');
  const [cypherQuery, setCypherQuery] = useState('');
  const [queryResults, setQueryResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [convertLoading, setConvertLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [sampleQueries, setSampleQueries] = useState<SampleQuery[]>([]);
  const [schemaInfo, setSchemaInfo] = useState<any>(null);

  useEffect(() => {
    loadSampleQueries();
    loadSchemaInfo();
  }, []);

  const loadSampleQueries = async () => {
    try {
      const response = await apiService.getSampleQueries();
      setSampleQueries(response.data);
    } catch (err) {
      console.error('Error loading sample queries:', err);
    }
  };

  const loadSchemaInfo = async () => {
    try {
      const response = await apiService.getGraphSchema();
      setSchemaInfo(response.data);
    } catch (err) {
      console.error('Error loading schema info:', err);
    }
  };

  const convertToQuery = async () => {
    if (!naturalLanguageQuery.trim()) return;
    
    setConvertLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await apiService.convertNaturalLanguageToCypher(naturalLanguageQuery);
      setCypherQuery(response.data.cypher);
      setSuccess('Query converted successfully!');
    } catch (err: any) {
      setError(err.message || 'Error converting query');
    } finally {
      setConvertLoading(false);
    }
  };

  const executeQuery = async () => {
    if (!cypherQuery.trim()) return;
    
    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await apiService.executeCypher(cypherQuery);
      setQueryResults(response.data.results || []);
      setSuccess(`Query executed successfully! Retrieved ${response.data.results?.length || 0} results.`);
    } catch (err: any) {
      setError(err.message || 'Error executing query');
    } finally {
      setLoading(false);
    }
  };

  const selectSampleQuery = (sample: SampleQuery) => {
    setNaturalLanguageQuery(sample.natural_language);
    setCypherQuery(sample.cypher);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    setSuccess('Copied to clipboard!');
  };

  const renderQueryResults = () => {
    if (!queryResults.length) return null;

    // Try to create a table structure from the results
    const firstResult = queryResults[0];
    const columns = Object.keys(firstResult).map(key => ({
      title: key,
      dataIndex: key,
      key: key,
      render: (value: any) => {
        if (typeof value === 'object' && value !== null) {
          if (value.labels) {
            // Neo4j Node
            return (
              <div>
                <div>
                  {value.labels.map((label: string) => (
                    <Tag key={label} color="blue">{label}</Tag>
                  ))}
                </div>
                <div style={{ marginTop: 4 }}>
                  {Object.entries(value.properties || {}).map(([k, v]) => (
                    <div key={k}><strong>{k}:</strong> {String(v)}</div>
                  ))}
                </div>
              </div>
            );
          } else if (value.type) {
            // Neo4j Relationship
            return (
              <div>
                <Tag color="green">{value.type}</Tag>
                <div style={{ marginTop: 4 }}>
                  {Object.entries(value.properties || {}).map(([k, v]) => (
                    <div key={k}><strong>{k}:</strong> {String(v)}</div>
                  ))}
                </div>
              </div>
            );
          } else {
            return <pre>{JSON.stringify(value, null, 2)}</pre>;
          }
        }
        return String(value);
      }
    }));

    return (
      <Table
        columns={columns}
        dataSource={queryResults.map((result, index) => ({ ...result, key: index }))}
        pagination={{ pageSize: 10 }}
        scroll={{ x: 'max-content' }}
        size="small"
      />
    );
  };

  return (
    <div>
      <Title level={2}>
        <DatabaseOutlined /> Natural Language Graph Query
      </Title>
      <Paragraph>
        Enter natural language queries to explore the GraphDB. The system will convert your query to Cypher and execute it against the Neo4j database.
      </Paragraph>

      {error && (
        <Alert
          message="Error"
          description={error}
          type="error"
          closable
          onClose={() => setError(null)}
          style={{ marginBottom: 16 }}
        />
      )}

      {success && (
        <Alert
          message="Success"
          description={success}
          type="success"
          closable
          onClose={() => setSuccess(null)}
          style={{ marginBottom: 16 }}
        />
      )}

      <Row gutter={[16, 16]}>
        <Col span={24}>
          <Card title="Natural Language Input">
            <TextArea
              rows={3}
              placeholder="Enter your natural language query here (e.g., 'Show me all data quality rules for the trade system')"
              value={naturalLanguageQuery}
              onChange={(e) => setNaturalLanguageQuery(e.target.value)}
              style={{ marginBottom: 16 }}
            />
            <Space>
              <Button
                type="primary"
                icon={<SearchOutlined />}
                onClick={convertToQuery}
                loading={convertLoading}
                disabled={!naturalLanguageQuery.trim()}
              >
                Convert to Cypher
              </Button>
              <Button
                icon={<CopyOutlined />}
                onClick={() => copyToClipboard(naturalLanguageQuery)}
                disabled={!naturalLanguageQuery.trim()}
              >
                Copy
              </Button>
            </Space>
          </Card>
        </Col>

        <Col span={24}>
          <Card 
            title="Generated Cypher Query"
            extra={
              <Space>
                <Button
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  onClick={executeQuery}
                  loading={loading}
                  disabled={!cypherQuery.trim()}
                >
                  Execute Query
                </Button>
                <Button
                  icon={<CopyOutlined />}
                  onClick={() => copyToClipboard(cypherQuery)}
                  disabled={!cypherQuery.trim()}
                >
                  Copy
                </Button>
              </Space>
            }
          >
            <Editor
              height="200px"
              language="cypher"
              value={cypherQuery}
              onChange={(value) => setCypherQuery(value || '')}
              theme="vs-light"
              options={{
                minimap: { enabled: false },
                fontSize: 14,
                lineNumbers: 'on',
                roundedSelection: false,
                scrollBeyondLastLine: false,
                readOnly: false,
                folding: false,
                automaticLayout: true
              }}
            />
          </Card>
        </Col>

        <Col span={24}>
          <Card title="Query Results">
            {loading ? (
              <div style={{ textAlign: 'center', padding: '40px' }}>
                <Spin size="large" />
                <div style={{ marginTop: 16 }}>Executing query...</div>
              </div>
            ) : queryResults.length > 0 ? (
              renderQueryResults()
            ) : (
              <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>
                No results to display. Execute a query to see results here.
              </div>
            )}
          </Card>
        </Col>

        <Col span={24}>
          <Tabs defaultActiveKey="1">
            <TabPane tab="Sample Queries" key="1">
              <div style={{ marginBottom: 16 }}>
                <Text strong>Click on any sample query to use it:</Text>
              </div>
              <Row gutter={[16, 16]}>
                {sampleQueries.map((sample, index) => (
                  <Col span={24} key={index}>
                    <Card 
                      size="small"
                      hoverable
                      onClick={() => selectSampleQuery(sample)}
                      style={{ cursor: 'pointer' }}
                    >
                      <div style={{ marginBottom: 8 }}>
                        <Text strong>{sample.natural_language}</Text>
                      </div>
                      <div style={{ marginBottom: 8, fontSize: '12px', color: '#666' }}>
                        <code>{sample.cypher}</code>
                      </div>
                      <div style={{ fontSize: '12px', color: '#999' }}>
                        {sample.description}
                      </div>
                    </Card>
                  </Col>
                ))}
              </Row>
            </TabPane>

            <TabPane tab="Schema Information" key="2">
              {schemaInfo ? (
                <Row gutter={[16, 16]}>
                  <Col span={8}>
                    <Card title="Node Labels" size="small">
                      {schemaInfo.node_labels?.map((label: string) => (
                        <Tag key={label} color="blue" style={{ marginBottom: 4 }}>
                          {label}
                        </Tag>
                      ))}
                    </Card>
                  </Col>
                  <Col span={8}>
                    <Card title="Relationship Types" size="small">
                      {schemaInfo.relationship_types?.map((type: string) => (
                        <Tag key={type} color="green" style={{ marginBottom: 4 }}>
                          {type}
                        </Tag>
                      ))}
                    </Card>
                  </Col>
                  <Col span={8}>
                    <Card title="Property Keys" size="small">
                      {schemaInfo.property_keys?.map((key: string) => (
                        <Tag key={key} color="orange" style={{ marginBottom: 4 }}>
                          {key}
                        </Tag>
                      ))}
                    </Card>
                  </Col>
                </Row>
              ) : (
                <div style={{ textAlign: 'center', padding: '40px' }}>
                  <Spin />
                  <div style={{ marginTop: 16 }}>Loading schema information...</div>
                </div>
              )}
            </TabPane>
          </Tabs>
        </Col>
      </Row>
    </div>
  );
};

export default GraphQueryPage; 