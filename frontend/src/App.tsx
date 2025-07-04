import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Layout, Menu, Typography, ConfigProvider } from 'antd';
import { DatabaseOutlined, SearchOutlined, WarningOutlined, BarChartOutlined } from '@ant-design/icons';
import GraphQueryPage from './pages/GraphQueryPage';
import DQViolationsPage from './pages/DQViolationsPage';
import DashboardPage from './pages/DashboardPage';
import './App.css';

const { Header, Sider, Content } = Layout;
const { Title } = Typography;

function App() {
  const [collapsed, setCollapsed] = React.useState(false);

  const menuItems = [
    {
      key: 'dashboard',
      icon: <BarChartOutlined />,
      label: 'Dashboard',
      path: '/'
    },
    {
      key: 'graph-query',
      icon: <SearchOutlined />,
      label: 'Graph Query',
      path: '/graph-query'
    },
    {
      key: 'dq-violations',
      icon: <WarningOutlined />,
      label: 'DQ Violations',
      path: '/dq-violations'
    }
  ];

  return (
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: '#1890ff',
          colorInfo: '#1890ff',
        },
      }}
    >
      <Router>
        <Layout style={{ minHeight: '100vh' }}>
          <Header 
            style={{ 
              background: '#001529', 
              padding: '0 16px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between'
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <DatabaseOutlined style={{ fontSize: '24px', color: '#fff', marginRight: '12px' }} />
              <Title level={3} style={{ color: '#fff', margin: 0 }}>
                Data Quality Management System
              </Title>
            </div>
          </Header>
          
          <Layout>
            <Sider 
              collapsible 
              collapsed={collapsed} 
              onCollapse={setCollapsed}
              style={{ background: '#fff' }}
            >
              <Menu
                mode="inline"
                defaultSelectedKeys={['dashboard']}
                style={{ height: '100%', borderRight: 0 }}
              >
                {menuItems.map(item => (
                  <Menu.Item key={item.key} icon={item.icon}>
                    <a href={item.path} style={{ textDecoration: 'none' }}>
                      {item.label}
                    </a>
                  </Menu.Item>
                ))}
              </Menu>
            </Sider>
            
            <Content style={{ 
              margin: '16px', 
              padding: '24px',
              background: '#fff',
              borderRadius: '8px'
            }}>
              <Routes>
                <Route path="/" element={<DashboardPage />} />
                <Route path="/graph-query" element={<GraphQueryPage />} />
                <Route path="/dq-violations" element={<DQViolationsPage />} />
              </Routes>
            </Content>
          </Layout>
        </Layout>
      </Router>
    </ConfigProvider>
  );
}

export default App; 