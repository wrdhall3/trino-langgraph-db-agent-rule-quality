import React, { useState, useEffect } from 'react';
import { 
  Row, 
  Col, 
  Card, 
  Table, 
  Typography, 
  Alert, 
  Button, 
  Space, 
  Tag, 
  Statistic,
  Input,
  Select,
  Modal,
  Descriptions
} from 'antd';
import { 
  WarningOutlined, 
  ReloadOutlined, 
  ExclamationCircleOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  EyeOutlined
} from '@ant-design/icons';
import { ColumnsType } from 'antd/es/table';
import { apiService } from '../services/apiService';

const { Title, Text } = Typography;
const { Search } = Input;
const { Option } = Select;

interface DQViolation {
  rule_name: string;
  rule_type: string;
  severity: string;
  system: string;
  table: string;
  column: string;
  uitid: string;
  value: any;
  message: string;
  timestamp: string;
}

interface ViolationSummary {
  total_violations: number;
  by_severity: Record<string, number>;
  by_rule_type: Record<string, number>;
  by_system: Record<string, number>;
  by_rule: Record<string, number>;
}

const DQViolationsPage: React.FC = () => {
  const [violations, setViolations] = useState<DQViolation[]>([]);
  const [filteredViolations, setFilteredViolations] = useState<DQViolation[]>([]);
  const [loading, setLoading] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [summary, setSummary] = useState<ViolationSummary | null>(null);
  const [selectedViolation, setSelectedViolation] = useState<DQViolation | null>(null);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [severityFilter, setSeverityFilter] = useState<string>('all');
  const [systemFilter, setSystemFilter] = useState<string>('all');
  const [ruleTypeFilter, setRuleTypeFilter] = useState<string>('all');

  useEffect(() => {
    loadViolations();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    filterViolations();
  }, [violations, searchText, severityFilter, systemFilter, ruleTypeFilter]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadViolations = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await apiService.getViolations();
      setViolations(response.data || []);
      generateSummary(response.data || []);
    } catch (err: any) {
      setError(err.message || 'Error loading violations');
    } finally {
      setLoading(false);
    }
  };

  const runAnalysis = async () => {
    setAnalyzing(true);
    setError(null);
    setSuccess(null);

    try {
      const response = await apiService.analyzeDQ();
      setViolations(response.data.violations || []);
      setSummary(response.data.summary);
      setSuccess(`Analysis completed! Found ${response.data.total_violations} violations.`);
    } catch (err: any) {
      setError(err.message || 'Error running analysis');
    } finally {
      setAnalyzing(false);
    }
  };

  const generateSummary = (violationData: DQViolation[]) => {
    if (!violationData.length) {
      setSummary({
        total_violations: 0,
        by_severity: {},
        by_rule_type: {},
        by_system: {},
        by_rule: {}
      });
      return;
    }

    const summary: ViolationSummary = {
      total_violations: violationData.length,
      by_severity: {},
      by_rule_type: {},
      by_system: {},
      by_rule: {}
    };

    violationData.forEach(violation => {
      // Count by severity
      summary.by_severity[violation.severity] = (summary.by_severity[violation.severity] || 0) + 1;
      
      // Count by rule type
      summary.by_rule_type[violation.rule_type] = (summary.by_rule_type[violation.rule_type] || 0) + 1;
      
      // Count by system
      summary.by_system[violation.system] = (summary.by_system[violation.system] || 0) + 1;
      
      // Count by rule
      summary.by_rule[violation.rule_name] = (summary.by_rule[violation.rule_name] || 0) + 1;
    });

    setSummary(summary);
  };

  const filterViolations = () => {
    let filtered = violations;

    if (searchText) {
      filtered = filtered.filter(violation => 
        violation.rule_name.toLowerCase().includes(searchText.toLowerCase()) ||
        violation.message.toLowerCase().includes(searchText.toLowerCase()) ||
        violation.uitid.toLowerCase().includes(searchText.toLowerCase())
      );
    }

    if (severityFilter !== 'all') {
      filtered = filtered.filter(violation => violation.severity === severityFilter);
    }

    if (systemFilter !== 'all') {
      filtered = filtered.filter(violation => violation.system === systemFilter);
    }

    if (ruleTypeFilter !== 'all') {
      filtered = filtered.filter(violation => violation.rule_type === ruleTypeFilter);
    }

    setFilteredViolations(filtered);
  };

  const getSeverityColor = (severity: string) => {
    switch (severity?.toLowerCase()) {
      case 'high':
      case 'critical':
        return 'red';
      case 'medium':
        return 'orange';
      case 'low':
        return 'yellow';
      default:
        return 'default';
    }
  };

  const showViolationDetail = (violation: DQViolation) => {
    setSelectedViolation(violation);
    setDetailModalVisible(true);
  };

  const columns: ColumnsType<DQViolation> = [
    {
      title: 'Rule Name',
      dataIndex: 'rule_name',
      key: 'rule_name',
      sorter: (a, b) => a.rule_name.localeCompare(b.rule_name),
      render: (text: string) => <Text strong>{text}</Text>
    },
    {
      title: 'Severity',
      dataIndex: 'severity',
      key: 'severity',
      sorter: (a, b) => a.severity.localeCompare(b.severity),
      render: (severity: string) => (
        <Tag color={getSeverityColor(severity)}>{severity?.toUpperCase()}</Tag>
      )
    },
    {
      title: 'System',
      dataIndex: 'system',
      key: 'system',
      sorter: (a, b) => a.system.localeCompare(b.system),
      render: (system: string) => <Tag color="blue">{system}</Tag>
    },
    {
      title: 'Rule Type',
      dataIndex: 'rule_type',
      key: 'rule_type',
      sorter: (a, b) => a.rule_type.localeCompare(b.rule_type),
      render: (type: string) => <Tag color="green">{type}</Tag>
    },
    {
      title: 'UITID',
      dataIndex: 'uitid',
      key: 'uitid',
      sorter: (a, b) => a.uitid.localeCompare(b.uitid),
      render: (uitid: string) => <Text code>{uitid}</Text>
    },
    {
      title: 'Column',
      dataIndex: 'column',
      key: 'column',
      sorter: (a, b) => a.column.localeCompare(b.column),
    },
    {
      title: 'Value',
      dataIndex: 'value',
      key: 'value',
      render: (value: any) => (
        <Text code style={{ color: '#ff4d4f' }}>
          {value === null ? 'NULL' : String(value)}
        </Text>
      )
    },
    {
      title: 'Message',
      dataIndex: 'message',
      key: 'message',
      ellipsis: true,
      render: (message: string) => <Text type="danger">{message}</Text>
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_, record) => (
        <Button
          type="link"
          icon={<EyeOutlined />}
          onClick={() => showViolationDetail(record)}
        >
          Details
        </Button>
      )
    }
  ];

  const uniqueSeverities = Array.from(new Set(violations.map(v => v.severity)));
  const uniqueSystems = Array.from(new Set(violations.map(v => v.system)));
  const uniqueRuleTypes = Array.from(new Set(violations.map(v => v.rule_type)));

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={2}>
          <WarningOutlined /> Data Quality Violations
        </Title>
        <Space>
          <Button
            type="primary"
            icon={<ReloadOutlined />}
            onClick={runAnalysis}
            loading={analyzing}
          >
            Run New Analysis
          </Button>
          <Button
            icon={<ReloadOutlined />}
            onClick={loadViolations}
            loading={loading}
          >
            Refresh
          </Button>
        </Space>
      </div>

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

      {/* Summary Cards */}
      {summary && (
        <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
          <Col span={6}>
            <Card>
              <Statistic
                title="Total Violations"
                value={summary.total_violations}
                prefix={<ExclamationCircleOutlined />}
                valueStyle={{ color: '#cf1322' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="Critical/High"
                value={
                  (summary.by_severity['CRITICAL'] || 0) + 
                  (summary.by_severity['HIGH'] || 0)
                }
                prefix={<CloseCircleOutlined />}
                valueStyle={{ color: '#ff4d4f' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="Medium"
                value={summary.by_severity['MEDIUM'] || 0}
                prefix={<WarningOutlined />}
                valueStyle={{ color: '#fa8c16' }}
              />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic
                title="Low"
                value={summary.by_severity['LOW'] || 0}
                prefix={<CheckCircleOutlined />}
                valueStyle={{ color: '#52c41a' }}
              />
            </Card>
          </Col>
        </Row>
      )}

      {/* Filters */}
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Search
              placeholder="Search violations..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              style={{ width: '100%' }}
            />
          </Col>
          <Col span={5}>
            <Select
              value={severityFilter}
              onChange={setSeverityFilter}
              style={{ width: '100%' }}
              placeholder="Filter by severity"
            >
              <Option value="all">All Severities</Option>
              {uniqueSeverities.map(severity => (
                <Option key={severity} value={severity}>{severity}</Option>
              ))}
            </Select>
          </Col>
          <Col span={5}>
            <Select
              value={systemFilter}
              onChange={setSystemFilter}
              style={{ width: '100%' }}
              placeholder="Filter by system"
            >
              <Option value="all">All Systems</Option>
              {uniqueSystems.map(system => (
                <Option key={system} value={system}>{system}</Option>
              ))}
            </Select>
          </Col>
          <Col span={6}>
            <Select
              value={ruleTypeFilter}
              onChange={setRuleTypeFilter}
              style={{ width: '100%' }}
              placeholder="Filter by rule type"
            >
              <Option value="all">All Rule Types</Option>
              {uniqueRuleTypes.map(type => (
                <Option key={type} value={type}>{type}</Option>
              ))}
            </Select>
          </Col>
        </Row>
      </Card>

      {/* Violations Table */}
      <Card>
        <Table
          columns={columns}
          dataSource={filteredViolations.map((violation, index) => ({
            ...violation,
            key: `${violation.uitid}-${violation.rule_name}-${index}`
          }))}
          loading={loading}
          pagination={{
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} violations`
          }}
          scroll={{ x: 'max-content' }}
          size="small"
        />
      </Card>

      {/* Detail Modal */}
      <Modal
        title="Violation Details"
        visible={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailModalVisible(false)}>
            Close
          </Button>
        ]}
        width={800}
      >
        {selectedViolation && (
          <Descriptions column={1} bordered>
            <Descriptions.Item label="Rule Name">
              <Text strong>{selectedViolation.rule_name}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="Severity">
              <Tag color={getSeverityColor(selectedViolation.severity)}>
                {selectedViolation.severity?.toUpperCase()}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="System">
              <Tag color="blue">{selectedViolation.system}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Rule Type">
              <Tag color="green">{selectedViolation.rule_type}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="UITID">
              <Text code>{selectedViolation.uitid}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="Table">
              {selectedViolation.table}
            </Descriptions.Item>
            <Descriptions.Item label="Column">
              {selectedViolation.column}
            </Descriptions.Item>
            <Descriptions.Item label="Value">
              <Text code style={{ color: '#ff4d4f' }}>
                {selectedViolation.value === null ? 'NULL' : String(selectedViolation.value)}
              </Text>
            </Descriptions.Item>
            <Descriptions.Item label="Message">
              <Text type="danger">{selectedViolation.message}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="Timestamp">
              {selectedViolation.timestamp ? new Date(selectedViolation.timestamp).toLocaleString() : 'N/A'}
            </Descriptions.Item>
          </Descriptions>
        )}
      </Modal>
    </div>
  );
};

export default DQViolationsPage; 