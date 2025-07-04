import React, { useState, useEffect } from 'react';
import { 
  Row, 
  Col, 
  Card, 
  Typography, 
  Statistic, 
  Progress, 
  Alert, 
  Spin,
  Button,
  Space,
  Tag,
  List
} from 'antd';
import { 
  BarChartOutlined, 
  DatabaseOutlined, 
  WarningOutlined, 
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { apiService } from '../services/apiService';

const { Title, Text, Paragraph } = Typography;

interface DashboardData {
  violations: any[];
  rules: any[];
  cdes: any[];
  systems: any[];
  summary: any;
}

const DashboardPage: React.FC = () => {
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadDashboardData();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const loadDashboardData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [violationsRes, rulesRes, cdesRes, systemsRes] = await Promise.all([
        apiService.getViolations(),
        apiService.getDQRules(),
        apiService.getCDEs(),
        apiService.getSystemsInfo()
      ]);

      const violations = violationsRes.data || [];
      const rules = rulesRes.data || [];
      const cdes = cdesRes.data || [];
      const systems = systemsRes.data || [];

      // Generate summary
      const summary = generateSummary(violations, rules, cdes, systems);

      setDashboardData({
        violations,
        rules,
        cdes,
        systems,
        summary
      });
    } catch (err: any) {
      setError(err.message || 'Error loading dashboard data');
    } finally {
      setLoading(false);
    }
  };

  const generateSummary = (violations: any[], rules: any[], cdes: any[], systems: any[]) => {
    const severityCount = violations.reduce((acc, v) => {
      acc[v.severity] = (acc[v.severity] || 0) + 1;
      return acc;
    }, {});

    const systemCount = violations.reduce((acc, v) => {
      acc[v.system] = (acc[v.system] || 0) + 1;
      return acc;
    }, {});

    const ruleTypeCount = violations.reduce((acc, v) => {
      acc[v.rule_type] = (acc[v.rule_type] || 0) + 1;
      return acc;
    }, {});

    return {
      totalViolations: violations.length,
      totalRules: rules.length,
      totalCDEs: cdes.length,
      totalSystems: systems.length,
      severityCount,
      systemCount,
      ruleTypeCount
    };
  };

  const getSeverityColor = (severity: string) => {
    switch (severity?.toLowerCase()) {
      case 'high':
      case 'critical':
        return '#ff4d4f';
      case 'medium':
        return '#fa8c16';
      case 'low':
        return '#52c41a';
      default:
        return '#d9d9d9';
    }
  };

  const chartColors = ['#1890ff', '#52c41a', '#fa8c16', '#ff4d4f', '#722ed1'];

  const renderSeverityChart = () => {
    if (!dashboardData?.summary?.severityCount) return null;

    const data = Object.entries(dashboardData.summary.severityCount).map(([severity, count]) => ({
      severity,
      count,
      fill: getSeverityColor(severity)
    }));

    return (
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="severity" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="count" fill="#1890ff" />
        </BarChart>
      </ResponsiveContainer>
    );
  };

  const renderSystemChart = () => {
    if (!dashboardData?.summary?.systemCount) return null;

    const data = Object.entries(dashboardData.summary.systemCount).map(([system, count], index) => ({
      system,
      count,
      fill: chartColors[index % chartColors.length]
    }));

    return (
      <ResponsiveContainer width="100%" height={200}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            labelLine={false}
            label={({ system, count }) => `${system}: ${count}`}
            outerRadius={80}
            fill="#8884d8"
            dataKey="count"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.fill} />
            ))}
          </Pie>
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    );
  };

  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <Spin size="large" />
        <div style={{ marginLeft: 16 }}>Loading dashboard data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <Alert
        message="Error Loading Dashboard"
        description={error}
        type="error"
        showIcon
        action={
          <Button size="small" onClick={loadDashboardData}>
            Retry
          </Button>
        }
      />
    );
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <Title level={2}>
          <BarChartOutlined /> Dashboard
        </Title>
        <Button
          icon={<ReloadOutlined />}
          onClick={loadDashboardData}
          loading={loading}
        >
          Refresh
        </Button>
      </div>

      <Paragraph>
        Welcome to the Data Quality Management System dashboard. Get an overview of your data quality metrics, 
        violations, and system health.
      </Paragraph>

      {/* Key Metrics */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="Total Violations"
              value={dashboardData?.summary?.totalViolations || 0}
              prefix={<ExclamationCircleOutlined />}
              valueStyle={{ color: '#cf1322' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Active Rules"
              value={dashboardData?.summary?.totalRules || 0}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="CDEs Monitored"
              value={dashboardData?.summary?.totalCDEs || 0}
              prefix={<DatabaseOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Systems Connected"
              value={dashboardData?.summary?.totalSystems || 0}
              prefix={<BarChartOutlined />}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Health Status */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={12}>
          <Card title="Data Quality Health">
            <div style={{ marginBottom: 16 }}>
              <Text strong>Overall Health Score</Text>
              <Progress 
                percent={dashboardData?.summary?.totalViolations ? 
                  Math.max(0, 100 - (dashboardData.summary.totalViolations * 10)) : 100
                } 
                status={dashboardData?.summary?.totalViolations > 5 ? 'exception' : 'success'}
              />
            </div>
            <div style={{ marginBottom: 16 }}>
              <Text strong>Critical Issues</Text>
              <Progress 
                percent={dashboardData?.summary?.severityCount?.CRITICAL ? 
                  Math.min(100, (dashboardData.summary.severityCount.CRITICAL * 20)) : 0
                } 
                status="exception"
              />
            </div>
            <div>
              <Text strong>Rules Coverage</Text>
              <Progress 
                percent={dashboardData?.summary?.totalRules ? 
                  Math.min(100, (dashboardData.summary.totalRules * 16.67)) : 0
                } 
                status="success"
              />
            </div>
          </Card>
        </Col>
        <Col span={12}>
          <Card title="Quick Actions">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Button type="primary" block href="/graph-query">
                <DatabaseOutlined /> Query GraphDB
              </Button>
              <Button type="default" block href="/dq-violations">
                <WarningOutlined /> View Violations
              </Button>
              <Button type="default" block onClick={() => window.location.href = 'http://localhost:7474'}>
                <DatabaseOutlined /> Neo4j Browser
              </Button>
              <Button type="default" block onClick={() => window.location.href = 'http://localhost:8080'}>
                <BarChartOutlined /> Trino Web UI
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>

      {/* Charts */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={12}>
          <Card title="Violations by Severity">
            {renderSeverityChart()}
          </Card>
        </Col>
        <Col span={12}>
          <Card title="Violations by System">
            {renderSystemChart()}
          </Card>
        </Col>
      </Row>

      {/* Recent Activity */}
      <Row gutter={[16, 16]}>
        <Col span={12}>
          <Card title="Recent Violations" size="small">
            <List
              size="small"
              dataSource={dashboardData?.violations?.slice(0, 5) || []}
              renderItem={(item: any) => (
                <List.Item>
                  <div style={{ width: '100%' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Text strong>{item.rule_name}</Text>
                      <Tag color={getSeverityColor(item.severity)}>{item.severity}</Tag>
                    </div>
                    <div style={{ marginTop: 4 }}>
                      <Text type="secondary">{item.system} - {item.uitid}</Text>
                    </div>
                  </div>
                </List.Item>
              )}
            />
            {dashboardData?.violations?.length === 0 && (
              <div style={{ textAlign: 'center', padding: '20px', color: '#999' }}>
                No violations found. Great job!
              </div>
            )}
          </Card>
        </Col>
        <Col span={12}>
          <Card title="System Overview" size="small">
            <List
              size="small"
              dataSource={dashboardData?.systems || []}
              renderItem={(item: any) => (
                <List.Item>
                  <div style={{ width: '100%' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Text strong>{item.name}</Text>
                      <Tag color="blue">{item.cdes?.length || 0} CDEs</Tag>
                    </div>
                    <div style={{ marginTop: 4 }}>
                      <Text type="secondary">{item.description}</Text>
                    </div>
                  </div>
                </List.Item>
              )}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default DashboardPage; 