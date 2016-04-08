/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * This class models a datasource factory.The datasource factory has funtions to
 * create 3 types of datasources. 1) Basic datasource without any connection
 * pooling. 2) Datasource with pooled connections. 3) Datasource with pooled
 * connection and transaction capabilities.
 * 
 *         The invokeAllMethods was setting only some specific properties
 *         Modified the code so that any key value mentioned in <property>tag
 *         is attempted for setting. If the property has a key as serverName ,
 *         then the setter method is invoked with the name setServerName & the
 *         value present in value attribute is passed Made the Exception
 *         handling robust
 * 
 *         Changed invokeAllMethods wrt the change in cache.xml for
 *         vendor specific properties.
 *  
 */
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.PasswordUtil;

public class DataSourceFactory  {

  private static final Logger logger = LogService.getLogger();
  
  /** Creates a new instance of DataSourceFactory */
  public DataSourceFactory() {
  }

  /**
   * This function returns the Basic datasource without any pooling.
   * 
   * @param configMap a map containing configurations required for datasource.
   * @throws DataSourceCreateException
   * @return ??
   */
  public static DataSource getSimpleDataSource(Map configMap, List props)
      throws DataSourceCreateException {
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    if (configs.getJDBCDriver() == null) {
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETSIMPLEDATASOURCEJDBC_DRIVER_IS_NOT_AVAILABLE));
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETSIMPLEDATASOURCEJDBC_DRIVER_IS_NOT_AVAILABLE.toLocalizedString());
    }
    if (configs.getURL() == null) {
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETSIMPLEDATASOURCEURL_STRING_TO_DATABASE_IS_NULL));
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETSIMPLEDATASOURCEURL_STRING_TO_DATABASE_IS_NULL.toLocalizedString());
    }
    try {
      return new GemFireBasicDataSource(configs);
    }
    catch (Exception ex) {
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_GETSIMPLEDATASOURCE_EXCEPTION_WHILE_CREATING_GEMFIREBASICDATASOURCE_EXCEPTION_STRING_0, ex.getLocalizedMessage()), ex);
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_GETSIMPLEDATASOURCE_EXCEPTION_WHILE_CREATING_GEMFIREBASICDATASOURCE_EXCEPTION_STRING_0.toLocalizedString(ex.getLocalizedMessage()), ex);
    }
  }

  /**
   * This function creats a data dource from the ManagedConnectionFactory class
   * using the connectionManager.
   * 
   * @param configMap
   * @param props
   * @return javax.sql.DataSource
   * @throws DataSourceCreateException
   */
  public static ClientConnectionFactoryWrapper getManagedDataSource(
      Map configMap, List props) throws DataSourceCreateException {
    Object cf = null;
    ManagedConnectionFactory mcf = null;
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    if (configs.getMCFClass() == null) {
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCEMANAGED_CONNECTION_FACTORY_CLASS_IS_NOT_AVAILABLE));
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCEMANAGED_CONNECTION_FACTORY_CLASS_IS_NOT_AVAILABLE.toLocalizedString());
    }
    else {
      try {
        Class cl = ClassPathLoader.getLatest().forName(configs.getMCFClass());
        mcf = (ManagedConnectionFactory) cl.newInstance();
        invokeAllMethods(cl, mcf, props);
      }
      catch (Exception ex) {
        logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCE_EXCEPTION_IN_CREATING_MANAGED_CONNECTION_FACTORY_EXCEPTION_STRING_0, ex), null);
        throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCE_EXCEPTION_IN_CREATING_MANAGED_CONNECTION_FACTORY_EXCEPTION_STRING_0
            .toLocalizedString(ex));
      }
    }
    // Initialize the managed connection factory class
    // Instantiate a connection manager with the managed-conn-facory-class
    // and generate a connection pool.
    Object cm = null;
    if (configs.getMCFClass().equals(
        "com.gemstone.persistence.connection.internal.ConnFactory")) {
      cm = new FacetsJCAConnectionManagerImpl(mcf, configs);
    }
    else {
      cm = new JCAConnectionManagerImpl(mcf, configs);
    }
    try {
      cf = mcf.createConnectionFactory((ConnectionManager) cm);
    }
    catch (Exception ex) {  
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCE_EXCEPTION_IN_CREATING_MANAGED_CONNECTION_FACTORY_EXCEPTION_STRING_0,ex), null);
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCE_EXCEPTION_IN_CREATING_MANAGED_CONNECTION_FACTORY_EXCEPTION_STRING_0
          .toLocalizedString(ex));
    }
    return new ClientConnectionFactoryWrapper(cf, cm);
  }

  /**
   * This function returns the datasource with connection pooling.
   * 
   * @param configMap a map containing configurations required for datasource.
   * @throws DataSourceCreateException
   * @return ???
   */
  public static DataSource getPooledDataSource(Map configMap, List props)
      throws DataSourceCreateException {
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    String connpoolClassName = configs.getConnectionPoolDSClass();
    if (connpoolClassName == null) {
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETPOOLEDDATASOURCECONNECTIONPOOLDATASOURCE_CLASS_NAME_FOR_THE_RESOURCEMANAGER_IS_NOT_AVAILABLE));
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETPOOLEDDATASOURCECONNECTIONPOOLDATASOURCE_CLASS_NAME_FOR_THE_RESOURCEMANAGER_IS_NOT_AVAILABLE.toLocalizedString());
    }
    try {
      Class cl = ClassPathLoader.getLatest().forName(connpoolClassName);
      Object Obj = cl.newInstance();
      invokeAllMethods(cl, Obj, props);
      return new GemFireConnPooledDataSource((ConnectionPoolDataSource) Obj,
          configs);
    }
    catch (Exception ex) {
      String exception = LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_GETPOOLEDDATASOURCE_EXCEPTION_CREATING_CONNECTIONPOOLDATASOURCE_EXCEPTION_STRING_0.toLocalizedString(new Object[] {ex});
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_GETPOOLEDDATASOURCE_EXCEPTION_CREATING_CONNECTIONPOOLDATASOURCE_EXCEPTION_STRING_0, ex), ex);
      throw new DataSourceCreateException(exception, ex);
    }
  }

  /**
   * This function returns the datasource with connection pooling and
   * transaction participation capabilities.
   * 
   * @param configMap a map containing configurations required for datasource.
   * @throws DataSourceCreateException
   * @return ???
   */
  public static DataSource getTranxDataSource(Map configMap, List props)
      throws DataSourceCreateException {
    ConfiguredDataSourceProperties configs = createDataSourceProperties(configMap);
    String xaClassName = configs.getXADSClass();
    if (xaClassName == null) {
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETTRANXDATASOURCEXADATASOURCE_CLASS_NAME_FOR_THE_RESOURCEMANAGER_IS_NOT_AVAILABLE));
      throw new DataSourceCreateException(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORYGETTRANXDATASOURCEXADATASOURCE_CLASS_NAME_FOR_THE_RESOURCEMANAGER_IS_NOT_AVAILABLE.toLocalizedString());
    }
    if(TEST_CONNECTION_HOST!=null) {
      props.add(new ConfigProperty("serverName",TEST_CONNECTION_HOST,"java.lang.String"));
    }
    if(TEST_CONNECTION_PORT!=null) {
      props.add(new ConfigProperty("portNumber",TEST_CONNECTION_PORT,"int"));
    }
    try {
      Class cl = ClassPathLoader.getLatest().forName(xaClassName);
      Object Obj = cl.newInstance();
      invokeAllMethods(cl, Obj, props);
      return new GemFireTransactionDataSource((XADataSource) Obj, configs);
    }
    catch (Exception ex) {
      String exception = LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_GETTRANXDATASOURCE_EXCEPTION_IN_CREATING_GEMFIRETRANSACTIONDATASOURCE__EXCEPTION_STRING_0.toLocalizedString(new Object[] {ex});
      logger.error(LocalizedMessage.create(LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_GETTRANXDATASOURCE_EXCEPTION_IN_CREATING_GEMFIRETRANSACTIONDATASOURCE__EXCEPTION_STRING_0, ex), ex);
      throw new DataSourceCreateException(exception, ex);
    }
  }

  /**
   * Creates ConfiguredDataSourceProperties from map.
   * 
   * @param configMap a map containing configurations required for datasource.
   * @return ConfiguredDataSourceProperties
   */
  private static ConfiguredDataSourceProperties createDataSourceProperties(
      Map configMap) {
    ConfiguredDataSourceProperties configs = new ConfiguredDataSourceProperties();
    Iterator entries = configMap.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry entry = (Map.Entry)entries.next();
      String name = (String)entry.getKey();
      final Object obj = entry.getValue();
      if (name.equals("connection-url"))
        configs.setURL((String)obj);
      else if (name.equals("user-name"))
        configs.setUser((String)obj);
      else if (name.equals("password"))
        configs.setPassword(PasswordUtil.decrypt((String)obj));
      else if (name.equals("jdbc-driver-class"))
        configs.setJDBCDriver((String)obj);
      else if (name.equals("init-pool-size"))
        configs.setInitialPoolSize(Integer
            .parseInt((String)(obj == null ? String
                    .valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_INIT_LIMIT)
                    : obj)));
      else if (name.equals("max-pool-size"))
        configs.setMaxPoolSize(Integer.parseInt((String)(obj == null ? String
            .valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_MAX_LIMIT)
            : obj)));
      else if (name.equals("idle-timeout-seconds"))
        configs
            .setConnectionExpirationTime(Integer
                .parseInt((String)(obj == null ? String
                    .valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_EXPIRATION_TIME)
                    : obj)));
      else if (name.equals("blocking-timeout-seconds"))
        configs
            .setConnectionTimeOut(Integer
                .parseInt((String)(obj == null ? String
                    .valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_ACTIVE_TIME_OUT)
                    : obj)));
      else if (name.equals("login-timeout-seconds"))
        configs
            .setLoginTimeOut(Integer
                .parseInt((String)(obj == null ? String
                    .valueOf(DataSourceResources.CONNECTION_POOL_DEFAULT_CLIENT_TIME_OUT)
                    : obj)));
      else if (name.equals("conn-pooled-datasource-class"))
        configs.setConnectionPoolDSClass((String)obj);
      else if (name.equals("xa-datasource-class"))
        configs.setXADSClass((String)obj);
      else if (name.equals("managed-conn-factory-class"))
        configs.setMCFClass((String)obj);
      else if (name.equals("transaction-type"))
        configs.setTransactionType((String)obj);
    }
    
    /*
     * Test hook for replacing URL
     */
    if(TEST_CONNECTION_URL!=null) {
      configs.setURL((String)TEST_CONNECTION_URL);
    }
    return configs;
  }
  
  /*
   * Test hook for replacing URL
   */
  private static String TEST_CONNECTION_URL = null;
  private static String TEST_CONNECTION_HOST = null;
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
   * Asif: Rewrote the function as the number of properties to be set for a
   * given datasource is dynamic
   * 
   * Rohit: Rewrote to accomodate the change in cache.xml. property tag is now
   * replaced by config-property for vendor specific data to get the Class name
   * dynamically. CLass name is specified in config-property-type tag and is
   * stored in ConfigProperty Data Object after parsing cache.xml.
   * 
   * @param c class of the object
   * @param cpdsObj The object
   */
  private static void invokeAllMethods(Class c, Object cpdsObj, List props)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
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
      methodName = new StringBuffer("set").append(
          Character.toUpperCase(key.charAt(0))).append(
          key.length() > 1 ? key.substring(1) : "").toString();
      try {
        Class cl = null;
        Class realClass = null;
        if("int".equals(type)) {
          cl = int.class;
          realClass = java.lang.Integer.class;
        } else {
          cl = ClassPathLoader.getLatest().forName(type);
          realClass = cl;
        }
        Constructor cr = realClass
            .getConstructor(new Class[] { java.lang.String.class});
        Object ob = cr.newInstance(new Object[] { value});
        m = c.getMethod(methodName, new Class[] { cl});
        m.invoke(cpdsObj, new Object[] { ob});
      }
      catch (ClassNotFoundException ex) {
        String exception = LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_INVOKEALLMETHODS_EXCEPTION_IN_CREATING_CLASS_WITH_THE_GIVEN_CONFIGPROPERTYTYPE_CLASSNAME_EXCEPTION_STRING_0.toLocalizedString(ex.toString());
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
      catch (NoSuchMethodException ex) {
        String exception = LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_INVOKEALLMETHODS_EXCEPTION_IN_CREATING_METHOD_USING_CONFIGPROPERTYNAME_PROPERTY_EXCEPTION_STRING_0.toLocalizedString(ex.toString());
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
      catch (InstantiationException ex) {
        String exception = LocalizedStrings.DataSourceFactory_DATASOURCEFACTORY_INVOKEALLMETHODS_EXCEPTION_IN_CREATING_INSTANCE_OF_THE_CLASS_USING_THE_CONSTRUCTOR_WITH_A_STRING_PARAMETER_EXCEPTION_STRING_0.toLocalizedString(ex.toString());
        if (logger.isDebugEnabled()) {
          logger.debug(exception, ex);
        }
      }
    }
  }
}
