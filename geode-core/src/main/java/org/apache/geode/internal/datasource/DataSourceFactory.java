/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.datasource;

/*
 * This class models a datasource factory.The datasource factory has funtions to create 3 types of
 * datasources. 1) Basic datasource without any connection pooling. 2) Datasource with pooled
 * connections. 3) Datasource with pooled connection and transaction capabilities.
 *
 * The invokeAllMethods was setting only some specific properties Modified the code so that any key
 * value mentioned in <property>tag is attempted for setting. If the property has a key as
 * serverName , then the setter method is invoked with the name setServerName & the value present in
 * value attribute is passed Made the Exception handling robust
 *
 * Changed invokeAllMethods wrt the change in cache.xml for vendor specific properties.
 *
 */
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.datasource.PooledDataSourceFactory;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.util.PasswordUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class DataSourceFactory {

  private static final Logger logger = LogService.getLogger();

  private static final String DEFAULT_CONNECTION_POOL_DS_CLASS =
      "org.apache.geode.connectors.jdbc.JdbcPooledDataSourceFactory";

  private static final String POOL_PREFIX = "pool.";

  /** Creates a new instance of DataSourceFactory */
  public DataSourceFactory() {}

  /**
   * This function returns the Basic datasource without any pooling.
   *
   * @param configMap a map containing configurations required for datasource.
   * @return ??
   */
  public DataSource getSimpleDataSource(Map configMap) throws DataSourceCreateException {
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    if (configs.getURL() == null) {
      logger.error("DataSourceFactory::getSimpleDataSource:URL String to Database is null");
      throw new DataSourceCreateException(
          "DataSourceFactory::getSimpleDataSource:URL String to Database is null");
    }
    try {
      return new GemFireBasicDataSource(configs);
    } catch (Exception ex) {
      logger.error(String.format(
          "DataSourceFactory::getSimpleDataSource:Exception while creating GemfireBasicDataSource.Exception String=%s",
          ex.getLocalizedMessage()),
          ex);
      throw new DataSourceCreateException(
          String.format(
              "DataSourceFactory::getSimpleDataSource:Exception while creating GemfireBasicDataSource.Exception String=%s",
              ex.getLocalizedMessage()),
          ex);
    }
  }

  /**
   * This function creates a data source from the ManagedConnectionFactory class using the
   * connectionManager.
   *
   * @return javax.sql.DataSource
   */
  public ClientConnectionFactoryWrapper getManagedDataSource(Map configMap,
      List<ConfigProperty> props) throws DataSourceCreateException {
    Object cf = null;
    ManagedConnectionFactory mcf = null;
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    if (configs.getMCFClass() == null) {
      logger.error(
          "DataSourceFactory::getManagedDataSource:Managed Connection factory class is not available");
      throw new DataSourceCreateException(
          "DataSourceFactory::getManagedDataSource:Managed Connection factory class is not available");
    } else {
      try {
        Class cl = ClassPathLoader.getLatest().forName(configs.getMCFClass());
        mcf = (ManagedConnectionFactory) cl.newInstance();
        invokeAllMethods(cl, mcf, props);
      } catch (Exception ex) {
        logger.error(String.format(
            "DataSourceFactory::getManagedDataSource: Exception in creating managed connection factory. Exception string, %s",
            ex));
        throw new DataSourceCreateException(
            String.format(
                "DataSourceFactory::getManagedDataSource: Exception in creating managed connection factory. Exception string, %s",
                ex));
      }
    }
    // Initialize the managed connection factory class
    // Instantiate a connection manager with the managed-conn-facory-class
    // and generate a connection pool.
    Object cm = null;
    if (configs.getMCFClass().equals("org.apache.persistence.connection.internal.ConnFactory")) {
      cm = new FacetsJCAConnectionManagerImpl(mcf, configs);
    } else {
      cm = new JCAConnectionManagerImpl(mcf, configs);
    }
    try {
      cf = mcf.createConnectionFactory((ConnectionManager) cm);
    } catch (Exception ex) {
      logger.error(
          "DataSourceFactory::getManagedDataSource: Exception in creating managed connection factory. Exception string, %s",
          ex.toString());
      throw new DataSourceCreateException(
          String.format(
              "DataSourceFactory::getManagedDataSource: Exception in creating managed connection factory. Exception string, %s",
              ex));
    }
    return new ClientConnectionFactoryWrapper(cf, cm);
  }

  /**
   * This function returns the datasource with connection pooling.
   */
  public DataSource getPooledDataSource(Map configMap, List<ConfigProperty> props)
      throws DataSourceCreateException {
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    String connpoolClassName = configs.getConnectionPoolDSClass();
    if (connpoolClassName == null) {
      connpoolClassName = DEFAULT_CONNECTION_POOL_DS_CLASS;
    }
    try {
      Properties poolProperties = createPoolProperties(configMap, props);
      Properties dataSourceProperties = createDataSourceProperties(props);
      Class<?> cl = ClassPathLoader.getLatest().forName(connpoolClassName);
      PooledDataSourceFactory factory = (PooledDataSourceFactory) cl.newInstance();
      return factory.createDataSource(poolProperties, dataSourceProperties);
    } catch (Exception ex) {
      String exception =
          String.format(
              "DataSourceFactory::getPooledDataSource:Exception creating ConnectionPoolDataSource.Exception string=%s",
              new Object[] {ex});
      logger.error(String.format(
          "DataSourceFactory::getPooledDataSource:Exception creating ConnectionPoolDataSource.Exception string=%s",
          ex),
          ex);
      throw new DataSourceCreateException(exception, ex);
    }
  }

  static Properties createPoolProperties(Map<String, String> configMap,
      List<ConfigProperty> props) {
    Properties result = new Properties();
    if (props != null) {
      for (ConfigProperty prop : props) {
        if (prop.getName().toLowerCase().startsWith(POOL_PREFIX)) {
          String poolName = prop.getName().substring(POOL_PREFIX.length());
          result.setProperty(poolName, prop.getValue());
        }
      }
    }
    if (configMap != null) {
      for (Map.Entry<String, String> entry : configMap.entrySet()) {
        if (entry.getValue() == null || entry.getValue().equals("")) {
          continue;
        }
        if (entry.getKey().equals("type")) {
          continue;
        }
        if (entry.getKey().equals("jdbc-driver-class")) {
          continue;
        }
        if (entry.getKey().equals("jndi-name")) {
          continue;
        }
        if (entry.getKey().equals("transaction-type")) {
          continue;
        }
        if (entry.getKey().equals("conn-pooled-datasource-class")) {
          continue;
        }
        if (entry.getKey().equals("managed-conn-factory-class")) {
          continue;
        }
        if (entry.getKey().equals("xa-datasource-class")) {
          continue;
        }
        result.setProperty(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  static Properties createDataSourceProperties(List<ConfigProperty> props) {
    Properties result = new Properties();
    if (props != null) {
      for (ConfigProperty prop : props) {
        if (prop.getName().toLowerCase().startsWith(POOL_PREFIX)) {
          continue;
        }
        result.setProperty(prop.getName(), prop.getValue());
      }
    }
    return result;
  }


  /**
   * This function returns the datasource with connection pooling and transaction participation
   * capabilities.
   *
   * @param configMap a map containing configurations required for datasource.
   * @return ???
   */
  public DataSource getTranxDataSource(Map configMap, List<ConfigProperty> props)
      throws DataSourceCreateException {
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    String xaClassName = configs.getXADSClass();
    if (xaClassName == null) {
      logger.error(
          "DataSourceFactory::getTranxDataSource:XADataSource class name for the ResourceManager is not available");
      throw new DataSourceCreateException(
          "DataSourceFactory::getTranxDataSource:XADataSource class name for the ResourceManager is not available");
    }
    if (TEST_CONNECTION_HOST != null) {
      props.add(new ConfigProperty("serverName", TEST_CONNECTION_HOST, "java.lang.String"));
    }
    if (TEST_CONNECTION_PORT != null) {
      props.add(new ConfigProperty("portNumber", TEST_CONNECTION_PORT, "int"));
    }
    try {
      Class cl = ClassPathLoader.getLatest().forName(xaClassName);
      Object Obj = cl.newInstance();
      invokeAllMethods(cl, Obj, props);
      return new GemFireTransactionDataSource((XADataSource) Obj, configs);
    } catch (Exception ex) {
      String exception =
          String.format(
              "DataSourceFactory::getTranxDataSource:Exception in creating GemFireTransactionDataSource. Exception string=%s",
              new Object[] {ex});
      logger.error(String.format(
          "DataSourceFactory::getTranxDataSource:Exception in creating GemFireTransactionDataSource. Exception string=%s",
          ex),
          ex);
      throw new DataSourceCreateException(exception, ex);
    }
  }

  /**
   * Creates ConfiguredDataSourceProperties from map.
   *
   * @param configMap a map containing configurations required for datasource.
   */
  private ConfiguredDataSourceProperties createDataSourceProperties(Map configMap) {
    ConfiguredDataSourceProperties configs = new ConfiguredDataSourceProperties();
    Iterator entries = configMap.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry entry = (Map.Entry) entries.next();
      String name = (String) entry.getKey();
      final Object obj = entry.getValue();
      if (name.equals("connection-url"))
        configs.setURL((String) obj);
      else if (name.equals("user-name"))
        configs.setUser((String) obj);
      else if (name.equals("password"))
        configs.setPassword(PasswordUtil.decrypt((String) obj));
      else if (name.equals("jdbc-driver-class"))
        configs.setJDBCDriver((String) obj);
      else if (name.equals("init-pool-size"))
        configs.setInitialPoolSize(Integer.parseInt((String) (obj == null
            ? String.valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_INIT_LIMIT) : obj)));
      else if (name.equals("max-pool-size"))
        configs.setMaxPoolSize(Integer.parseInt((String) (obj == null
            ? String.valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_MAX_LIMIT) : obj)));
      else if (name.equals("idle-timeout-seconds"))
        configs.setConnectionExpirationTime(Integer.parseInt((String) (obj == null
            ? String.valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_EXPIRATION_TIME) : obj)));
      else if (name.equals("blocking-timeout-seconds"))
        configs.setConnectionTimeOut(Integer.parseInt((String) (obj == null
            ? String.valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_ACTIVE_TIME_OUT) : obj)));
      else if (name.equals("login-timeout-seconds"))
        configs.setLoginTimeOut(Integer.parseInt((String) (obj == null
            ? String.valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_CLIENT_TIME_OUT) : obj)));
      else if (name.equals("conn-pooled-datasource-class"))
        configs.setConnectionPoolDSClass((String) obj);
      else if (name.equals("xa-datasource-class"))
        configs.setXADSClass((String) obj);
      else if (name.equals("managed-conn-factory-class"))
        configs.setMCFClass((String) obj);
      else if (name.equals("transaction-type"))
        configs.setTransactionType((String) obj);
    }

    /*
     * Test hook for replacing URL
     */
    if (TEST_CONNECTION_URL != null) {
      configs.setURL((String) TEST_CONNECTION_URL);
    }
    return configs;
  }

  /*
   * Test hook for replacing URL
   */
  @MutableForTesting
  private static String TEST_CONNECTION_URL = null;
  @MutableForTesting
  private static String TEST_CONNECTION_HOST = null;
  @MutableForTesting
  private static String TEST_CONNECTION_PORT = null;

  /*
   * Test hook for replacing URL
   */
  public static void setTestConnectionUrl(String url) {
    TEST_CONNECTION_URL = url;
  }

  /*
   * Test hook for replacing Host
   */
  public static void setTestConnectionHost(String host) {
    TEST_CONNECTION_HOST = host;
  }

  /*
   * Test hook for replacing Port
   */
  public static void setTestConnectionPort(String port) {
    TEST_CONNECTION_PORT = port;
  }


  /**
   *
   * dynamically invokes all the methods in the specified object.
   *
   * Asif: Rewrote the function as the number of properties to be set for a given datasource is
   * dynamic
   *
   * Rohit: Rewrote to accomodate the change in cache.xml. property tag is now replaced by
   * config-property for vendor specific data to get the Class name dynamically. CLass name is
   * specified in config-property-type tag and is stored in ConfigProperty Data Object after parsing
   * cache.xml.
   *
   * @param c class of the object
   * @param cpdsObj The object
   */
  private void invokeAllMethods(Class c, Object cpdsObj, List props)
      throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    String key = null;
    String value = null;
    String type = null;
    String methodName = null;
    Method m = null;
    for (Iterator it = props.iterator(); it.hasNext();) {
      ConfigProperty cp = (ConfigProperty) it.next();
      key = cp.getName();
      value = cp.getValue();
      type = cp.getType();
      if (key.indexOf("password") != -1) {
        value = PasswordUtil.decrypt(String.valueOf(value));
      }
      methodName = new StringBuffer("set").append(Character.toUpperCase(key.charAt(0)))
          .append(key.length() > 1 ? key.substring(1) : "").toString();
      try {
        Class cl = null;
        Class realClass = null;
        if ("int".equals(type)) {
          cl = int.class;
          realClass = java.lang.Integer.class;
        } else {
          cl = ClassPathLoader.getLatest().forName(type);
          realClass = cl;
        }
        Constructor cr = realClass.getConstructor(new Class[] {java.lang.String.class});
        Object ob = cr.newInstance(new Object[] {value});
        m = c.getMethod(methodName, new Class[] {cl});
        m.invoke(cpdsObj, new Object[] {ob});
      } catch (ClassNotFoundException ex) {
        String exception =
            String.format(
                "DataSourceFactory::invokeAllMethods: Exception in creating Class with the given config-property-type classname. Exception string=%s",
                ex.toString());
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      } catch (NoSuchMethodException ex) {
        String exception =
            String.format(
                "DataSourceFactory::invokeAllMethods: Exception in creating method using config-property-name property. Exception string=%s",
                ex.toString());
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      } catch (InstantiationException ex) {
        String exception =
            String.format(
                "DataSourceFactory::invokeAllMethods: Exception in creating instance of the class using the constructor with a String parameter. Exception string=%s",
                ex.toString());
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
    }
  }
}
