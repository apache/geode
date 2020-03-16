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
package org.apache.geode.internal.jndi;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import javax.sql.DataSource;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.datasource.ClientConnectionFactoryWrapper;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.internal.datasource.DataSourceCreateException;
import org.apache.geode.internal.datasource.DataSourceFactory;
import org.apache.geode.internal.datasource.GemFireTransactionDataSource;
import org.apache.geode.internal.jta.TransactionManagerImpl;
import org.apache.geode.internal.jta.TransactionUtils;
import org.apache.geode.internal.jta.UserTransactionImpl;
import org.apache.geode.internal.util.DriverJarUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * <p>
 * Utility class for binding DataSources and Transactional resources to JNDI Tree. If there is a
 * pre-existing JNDI tree in the system, the GemFire JNDI tree is not not generated.
 * </p>
 * <p>
 * Datasources are bound to jndi tree after getting initialised. The initialisation parameters are
 * read from cache.xml.
 * </p>
 * <p>
 * If there is a pre-existing TransactionManager/UserTransaction (possible in presence of
 * application server scenerio), the GemFire TransactionManager and UserTransaction will not be
 * initialised. In that case, application server TransactionManager/UserTransaction will handle the
 * transactional activity. But even in this case the datasource element will be bound to the
 * available JNDI tree. The transactional datasource (XADataSource) will make use of available
 * TransactionManager.
 * </p>
 *
 */
public class JNDIInvoker {

  private static final Logger logger = LogService.getLogger();

  // private static boolean DEBUG = false;
  /**
   * JNDI Context, this may refer to GemFire JNDI Context or external Context, in case the external
   * JNDI tree exists.
   */
  @MakeNotStatic
  private static Context ctx;
  /**
   * transactionManager TransactionManager, this refers to GemFire TransactionManager only.
   */
  @MakeNotStatic
  private static TransactionManager transactionManager;
  // most of the following came from the javadocs at:
  // http://static.springsource.org/spring/docs/2.5.x/api/org/springframework/transaction/jta/JtaTransactionManager.html
  private static final String[][] knownJNDIManagers = {{"java:/TransactionManager", "JBoss"},
      {"java:comp/TransactionManager", "Cosminexus"}, // and many others
      {"java:appserver/TransactionManager", "GlassFish"}, {"java:pm/TransactionManager", "SunONE"},
      {"java:comp/UserTransaction", "Orion, JTOM, BEA WebLogic"},
      // not sure about the following but leaving it for backwards compat
      {"javax.transaction.TransactionManager", "BEA WebLogic"}};
  /* ************************************************* */
  /**
   * WebSphere 5.1 TransactionManagerFactory
   */
  private static final String WS_FACTORY_CLASS_5_1 =
      "com.ibm.ws.Transaction.TransactionManagerFactory";
  /**
   * WebSphere 5.0 TransactionManagerFactory
   */
  private static final String WS_FACTORY_CLASS_5_0 =
      "com.ibm.ejs.jts.jta.TransactionManagerFactory";
  /**
   * WebSphere 4.0 TransactionManagerFactory
   */
  private static final String WS_FACTORY_CLASS_4 = "com.ibm.ejs.jts.jta.JTSXA";
  /**
   * Maps data source name to the data source instance itself.
   */
  @MakeNotStatic
  private static final ConcurrentMap<String, Object> dataSourceMap = new ConcurrentHashMap<>();

  /**
   * If this system property is set to true, GemFire will not try to lookup for an existing JTA
   * transaction manager bound to JNDI context or try to bind itself as a JTA transaction manager.
   * Also region operations will <b>not</b> participate in an ongoing JTA transaction.
   */
  @MutableForTesting
  private static boolean IGNORE_JTA =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "ignoreJTA");

  @Immutable
  private static final DataSourceFactory dataSourceFactory = new DataSourceFactory();

  /**
   * Bind the transaction resources. Bind UserTransaction and TransactionManager.
   * <p>
   * If there is pre-existing JNDI tree in the system and TransactionManager / UserTransaction is
   * already bound, GemFire will make use of these resources, but if TransactionManager /
   * UserTransaction is not available, the GemFire TransactionManager / UserTransaction will be
   * bound to the JNDI tree.
   * </p>
   *
   */
  public static void mapTransactions(DistributedSystem distSystem) {
    try {
      TransactionUtils.setLogWriter(distSystem.getLogWriter());
      cleanup();
      if (IGNORE_JTA) {
        return;
      }
      ctx = new InitialContext();
      doTransactionLookup();
    } catch (NamingException ne) {
      LogWriter writer = TransactionUtils.getLogWriter();
      if (ne instanceof NoInitialContextException) {
        String exception =
            "JNDIInvoker::mapTransactions:: No application server context found, Starting GemFire JNDI Context Context ";
        if (writer.finerEnabled())
          writer.finer(exception);
        try {
          initializeGemFireContext();
          transactionManager = TransactionManagerImpl.getTransactionManager();
          ctx.rebind("java:/TransactionManager", transactionManager);
          if (writer.fineEnabled())
            writer.fine(
                "JNDIInvoker::mapTransactions::Bound TransactionManager to Context GemFire JNDI Tree");
          UserTransactionImpl utx = new UserTransactionImpl();
          ctx.rebind("java:/UserTransaction", utx);
          if (writer.fineEnabled())
            writer.fine(
                "JNDIInvoker::mapTransactions::Bound Transaction to Context GemFire JNDI Tree");
        } catch (NamingException ne1) {
          if (writer.infoEnabled())
            writer.info(
                "JNDIInvoker::mapTransactions::NamingException while binding TransactionManager/UserTransaction to GemFire JNDI Tree");
        } catch (SystemException se1) {
          if (writer.infoEnabled())
            writer.info(
                "JNDIInvoker::mapTransactions::SystemException while binding UserTransaction to GemFire JNDI Tree");
        }
      } else if (ne instanceof NameNotFoundException) {
        String exception =
            "JNDIInvoker::mapTransactions:: No TransactionManager associated to Application server context, trying to bind GemFire TransactionManager";
        if (writer.finerEnabled())
          writer.finer(exception);
        try {
          transactionManager = TransactionManagerImpl.getTransactionManager();
          ctx.rebind("java:/TransactionManager", transactionManager);
          if (writer.fineEnabled())
            writer.fine(
                "JNDIInvoker::mapTransactions::Bound TransactionManager to Application Server Context");
          UserTransactionImpl utx = new UserTransactionImpl();
          ctx.rebind("java:/UserTransaction", utx);
          if (writer.fineEnabled())
            writer.fine(
                "JNDIInvoker::mapTransactions::Bound UserTransaction to Application Server Context");
        } catch (NamingException ne1) {
          if (writer.infoEnabled())
            writer.info(
                "JNDIInvoker::mapTransactions::NamingException while binding TransactionManager/UserTransaction to Application Server JNDI Tree");
        } catch (SystemException se1) {
          if (writer.infoEnabled())
            writer.info(
                "JNDIInvoker::mapTransactions::SystemException while binding TransactionManager/UserTransaction to Application Server JNDI Tree");
        }
      }
    }
  }

  /*
   * Cleans all the DataSource ans its associated threads gracefully, before start of the GemFire
   * system. Also kills all of the threads associated with the TransactionManager before making it
   * null.
   */
  private static void cleanup() {
    if (transactionManager instanceof TransactionManagerImpl) {
      TransactionManagerImpl.refresh();
      // unbind and set TransactionManager to null, so as to
      // initialize TXManager correctly if the cache is being restarted
      transactionManager = null;
      try {
        if (ctx != null) {
          ctx.unbind("java:/TransactionManager");
        }
      } catch (NamingException e) {
        // ok to ignore, rebind will be tried later
      }
    }
    dataSourceMap.values().stream().forEach(JNDIInvoker::closeDataSource);
    dataSourceMap.clear();
    IGNORE_JTA = Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "ignoreJTA");
  }

  private static void closeDataSource(Object dataSource) {
    if (dataSource instanceof AutoCloseable) {
      try {
        ((AutoCloseable) dataSource).close();
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Exception closing DataSource", e);
        }
      }
    }
  }

  /*
   * Helps in locating TransactionManager in presence of application server scenario. Stores the
   * value of TransactionManager for reference in GemFire system.
   */
  private static void doTransactionLookup() throws NamingException {
    Object jndiObject = null;
    LogWriter writer = TransactionUtils.getLogWriter();
    for (int i = 0; i < knownJNDIManagers.length; i++) {
      try {
        jndiObject = ctx.lookup(knownJNDIManagers[i][0]);
      } catch (NamingException e) {
        String exception = "JNDIInvoker::doTransactionLookup::Couldn't lookup ["
            + knownJNDIManagers[i][0] + " (" + knownJNDIManagers[i][1] + ")]";
        if (writer.finerEnabled())
          writer.finer(exception);
      }
      if (jndiObject instanceof TransactionManager) {
        transactionManager = (TransactionManager) jndiObject;
        String exception = "JNDIInvoker::doTransactionLookup::Found TransactionManager for "
            + knownJNDIManagers[i][1];
        if (writer.fineEnabled())
          writer.fine(exception);
        return;
      } else {
        String exception = "JNDIInvoker::doTransactionLookup::Found TransactionManager of class "
            + (jndiObject == null ? "null" : jndiObject.getClass())
            + " but is not of type javax.transaction.TransactionManager";
        if (writer.fineEnabled())
          writer.fine(exception);
      }
    }
    Class clazz;
    try {
      if (writer.finerEnabled())
        writer.finer(
            "JNDIInvoker::doTransactionLookup::Trying WebSphere 5.1: " + WS_FACTORY_CLASS_5_1);
      clazz = ClassPathLoader.getLatest().forName(WS_FACTORY_CLASS_5_1);
      if (writer.fineEnabled())
        writer
            .fine("JNDIInvoker::doTransactionLookup::Found WebSphere 5.1: " + WS_FACTORY_CLASS_5_1);
    } catch (ClassNotFoundException ex) {
      try {
        if (writer.finerEnabled())
          writer.finer(
              "JNDIInvoker::doTransactionLookup::Trying WebSphere 5.0: " + WS_FACTORY_CLASS_5_0);
        clazz = ClassPathLoader.getLatest().forName(WS_FACTORY_CLASS_5_0);
        if (writer.fineEnabled())
          writer.fine(
              "JNDIInvoker::doTransactionLookup::Found WebSphere 5.0: " + WS_FACTORY_CLASS_5_0);
      } catch (ClassNotFoundException ex2) {
        try {
          clazz = ClassPathLoader.getLatest().forName(WS_FACTORY_CLASS_4);
          String exception =
              "JNDIInvoker::doTransactionLookup::Found WebSphere 4: " + WS_FACTORY_CLASS_4;
          if (writer.fineEnabled())
            writer.fine(exception, ex);
        } catch (ClassNotFoundException ex3) {
          if (writer.finerEnabled())
            writer.finer(
                "JNDIInvoker::doTransactionLookup::Couldn't find any WebSphere TransactionManager factory class, neither for WebSphere version 5.1 nor 5.0 nor 4");
          throw new NoInitialContextException();
        }
      }
    }
    try {
      Method method = clazz.getMethod("getTransactionManager", (Class[]) null);
      transactionManager = (TransactionManager) method.invoke(null, (Object[]) null);
    } catch (Exception ex) {
      writer.warning(
          String.format(
              "JNDIInvoker::doTransactionLookup::Found WebSphere TransactionManager factory class [%s], but could not invoke its static 'getTransactionManager' method",
              clazz.getName()),
          ex);
      throw new NameNotFoundException(
          String.format(
              "JNDIInvoker::doTransactionLookup::Found WebSphere TransactionManager factory class [%s], but could not invoke its static 'getTransactionManager' method",
              new Object[] {clazz.getName()}));
    }
  }

  /**
   * Initialises the GemFire context. This is called when no external JNDI Context is found.
   *
   */
  private static void initializeGemFireContext() throws NamingException {
    Hashtable table = new Hashtable();
    table.put(Context.INITIAL_CONTEXT_FACTORY,
        "org.apache.geode.internal.jndi.InitialContextFactoryImpl");
    ctx = new InitialContext(table);
  }

  /**
   * Binds a single Datasource to the existing JNDI tree. The JNDI tree may be The Datasource
   * properties are contained in the map. The Datasource implementation class is populated based on
   * properties in the map.
   *
   * @param map contains Datasource configuration properties.
   */
  public static void mapDatasource(Map map, List<ConfigProperty> props)
      throws NamingException, DataSourceCreateException, ClassNotFoundException, SQLException,
      InstantiationException, IllegalAccessException {
    mapDatasource(map, props, dataSourceFactory, ctx);
  }

  static void mapDatasource(Map map, List<ConfigProperty> props,
      DataSourceFactory dataSourceFactory, Context context)
      throws NamingException, DataSourceCreateException, ClassNotFoundException, SQLException,
      InstantiationException, IllegalAccessException {
    String driverClassName = null;
    if (map != null) {
      driverClassName = (String) map.get("jdbc-driver-class");
      DriverJarUtil util = new DriverJarUtil();
      if (driverClassName != null) {
        util.registerDriver(driverClassName);
      }
    }

    String value = (String) map.get("type");
    String jndiName = "";
    jndiName = (String) map.get("jndi-name");
    if (value.equals("PooledDataSource")) {
      validateAndBindDataSource(context, jndiName,
          dataSourceFactory.getPooledDataSource(map, props), props);
    } else if (value.equals("XAPooledDataSource")) {
      validateAndBindDataSource(context, jndiName,
          dataSourceFactory.getTranxDataSource(map, props), props);
    } else if (value.equals("SimpleDataSource")) {
      validateAndBindDataSource(context, jndiName, dataSourceFactory.getSimpleDataSource(map),
          props);
    } else if (value.equals("ManagedDataSource")) {
      ClientConnectionFactoryWrapper wrapper = dataSourceFactory.getManagedDataSource(map, props);
      ctx.rebind("java:/" + jndiName, wrapper.getClientConnFactory());
      dataSourceMap.put(jndiName, wrapper);
      if (logger.isDebugEnabled()) {
        logger.debug("Bound java:/" + jndiName + " to Context");
      }
    } else {
      String exception = "JNDIInvoker::mapDataSource::No correct type of DataSource";
      if (logger.isDebugEnabled()) {
        logger.debug(exception);
      }
      throw new DataSourceCreateException(exception);
    }
  }

  private static void validateAndBindDataSource(Context context, String jndiName,
      DataSource dataSource, List<ConfigProperty> props)
      throws NamingException, DataSourceCreateException {
    try (Connection connection = getConnection(dataSource, props)) {
    } catch (SQLException sqlEx) {
      closeDataSource(dataSource);
      throw new DataSourceCreateException(
          "Failed to connect to \"" + jndiName + "\". See log for details", sqlEx);
    }
    context.rebind("java:/" + jndiName, dataSource);
    dataSourceMap.put(jndiName, dataSource);
    if (logger.isDebugEnabled()) {
      logger.debug("Bound java:/" + jndiName + " to Context");
    }
  }

  private static Connection getConnection(DataSource dataSource, List<ConfigProperty> props)
      throws SQLException {
    return dataSource.getConnection();
  }

  public static void unMapDatasource(String jndiName) throws NamingException {
    ctx.unbind("java:/" + jndiName);
    Object removedDataSource = dataSourceMap.remove(jndiName);
    closeDataSource(removedDataSource);
  }

  public static DataSource getDataSource(String name) {
    Object result = dataSourceMap.get(name);
    if (result instanceof DataSource) {
      return (DataSource) result;
    } else {
      return null;
    }
  }

  public static boolean isValidDataSource(String name) {
    Object dataSource = dataSourceMap.get(name);

    if (dataSource == null || (dataSource instanceof DataSource
        && !(dataSource instanceof GemFireTransactionDataSource))) {
      return true;
    }

    return false;
  }

  /**
   * @return Context the existing JNDI Context. If there is no pre-esisting JNDI Context, the
   *         GemFire JNDI Context is returned.
   */
  public static Context getJNDIContext() {
    return ctx;
  }

  /**
   * returns the GemFire TransactionManager.
   *
   */
  public static TransactionManager getTransactionManager() {
    return transactionManager;
  }

  public static int getNoOfAvailableDataSources() {
    return dataSourceMap.size();
  }

  public static Map<String, String> getBindingNamesRecursively(Context ctx) throws Exception {
    Map<String, String> result = new HashMap<>();
    NamingEnumeration<Binding> enumeration = ctx.listBindings("");

    while (enumeration.hasMore()) {
      Binding binding = enumeration.next();
      String name = binding.getName();
      String separator = name.endsWith(":") ? "" : "/";
      Object o = binding.getObject();
      if (o instanceof Context) {
        Map<String, String> innerBindings = getBindingNamesRecursively((Context) o);
        innerBindings.forEach((k, v) -> result.put(name + separator + k, v));
      } else {
        result.put(name, binding.getClassName());
      }
    }

    return result;
  }
}
