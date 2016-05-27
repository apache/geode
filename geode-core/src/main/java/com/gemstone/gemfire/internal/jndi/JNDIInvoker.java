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
package com.gemstone.gemfire.internal.jndi;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.datasource.AbstractDataSource;
import com.gemstone.gemfire.internal.datasource.ClientConnectionFactoryWrapper;
import com.gemstone.gemfire.internal.datasource.DataSourceCreateException;
import com.gemstone.gemfire.internal.datasource.DataSourceFactory;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jta.TransactionManagerImpl;
import com.gemstone.gemfire.internal.jta.TransactionUtils;
import com.gemstone.gemfire.internal.jta.UserTransactionImpl;

import javax.naming.*;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Utility class for binding DataSources and Transactional resources to JNDI
 * Tree. If there is a pre-existing JNDI tree in the system, the GemFire
 * JNDI tree is not not generated.
 * </p><p>
 * Datasources are bound to jndi tree after getting initialised. The
 * initialisation parameters are read from cache.xml.
 * </p><p>
 * If there is a pre-existing TransactionManager/UserTransaction (possible
 * in presence of application server scenerio), the GemFire TransactionManager
 * and UserTransaction will not be initialised. In that case, application server
 * TransactionManager/UserTransaction will handle the transactional activity. But
 * even in this case the datasource element will be bound to the available JNDI
 * tree. The transactional datasource (XADataSource) will make use of available
 * TransactionManager.
 * </p>
 * 
 */
public class JNDIInvoker  {

//  private static boolean DEBUG = false;
  /**
   * JNDI Context, this may refer to GemFire JNDI Context or external Context,
   * in case the external JNDI tree exists.
   */
  private static Context ctx;
  /**
   * transactionManager TransactionManager, this refers to GemFire
   * TransactionManager only.
   */
  private static TransactionManager transactionManager;
  // most of the following came from the javadocs at:
  // http://static.springsource.org/spring/docs/2.5.x/api/org/springframework/transaction/jta/JtaTransactionManager.html
  private static String[][] knownJNDIManagers = {
      {"java:/TransactionManager", "JBoss"},
      {"java:comp/TransactionManager","Cosminexus"}, // and many others
      {"java:appserver/TransactionManager","GlassFish"},
      {"java:pm/TransactionManager","SunONE"},
      {"java:comp/UserTransaction","Orion, JTOM, BEA WebLogic"},
      // not sure about the following but leaving it for backwards compat
      {"javax.transaction.TransactionManager", "BEA WebLogic"}
   };
  /** ************************************************* */
  /**
   * WebSphere 5.1 TransactionManagerFactory
   */
  private static final String WS_FACTORY_CLASS_5_1 = "com.ibm.ws.Transaction.TransactionManagerFactory";
  /**
   * WebSphere 5.0 TransactionManagerFactory
   */
  private static final String WS_FACTORY_CLASS_5_0 = "com.ibm.ejs.jts.jta.TransactionManagerFactory";
  /**
   * WebSphere 4.0 TransactionManagerFactory
   */
  private static final String WS_FACTORY_CLASS_4 = "com.ibm.ejs.jts.jta.JTSXA";
  /**
   * List of DataSource bound to the context, used for cleaning gracefully
   * closing datasource and associated threads.
   */
  private static List dataSourceList = new ArrayList();

  /**
   * If this system property is set to true, GemFire will not try to lookup for an
   * existing JTA transaction manager bound to JNDI context or try to bind itself
   * as a JTA transaction manager. Also region operations will <b>not</b> participate in
   * an ongoing JTA transaction. 
   */
  private static Boolean IGNORE_JTA = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ignoreJTA");

  /**
   * Bind the transaction resources. Bind UserTransaction and
   * TransactionManager. 
   * <p>
   * If there is pre-existing JNDI tree in the system
   * and TransactionManager / UserTransaction is already bound, GemFire will
   * make use of these resources, but if TransactionManager / UserTransaction is
   * not available, the GemFire TransactionManager / UserTransaction will be
   * bound to the JNDI tree.
   * </p>
   *  
   */
  public static void mapTransactions(DistributedSystem distSystem) {
    try {
      TransactionUtils.setLogWriter(distSystem.getLogWriter().convertToLogWriterI18n());
      cleanup();
      if (IGNORE_JTA) {
        return;
      }
      ctx = new InitialContext();
      doTransactionLookup();
    }
    catch (NamingException ne) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (ne instanceof NoInitialContextException) {
        String exception = "JNDIInvoker::mapTransactions:: No application server context found, Starting GemFire JNDI Context Context ";
        if (writer.finerEnabled()) writer.finer(exception);
        try {
          initializeGemFireContext();
          transactionManager = TransactionManagerImpl.getTransactionManager();
          ctx.rebind("java:/TransactionManager", transactionManager);
          if (writer.fineEnabled())
              writer
                  .fine("JNDIInvoker::mapTransactions::Bound TransactionManager to Context GemFire JNDI Tree");
          UserTransactionImpl utx = new UserTransactionImpl();
          ctx.rebind("java:/UserTransaction", utx);
          if (writer.fineEnabled())
              writer
                  .fine("JNDIInvoker::mapTransactions::Bound Transaction to Context GemFire JNDI Tree");
        }
        catch (NamingException ne1) {
          if (writer.infoEnabled())
              writer
                  .info(LocalizedStrings.JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSNAMINGEXCEPTION_WHILE_BINDING_TRANSACTIONMANAGERUSERTRANSACTION_TO_GEMFIRE_JNDI_TREE);
        }
        catch (SystemException se1) {
          if (writer.infoEnabled())
              writer
                  .info(LocalizedStrings.JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSSYSTEMEXCEPTION_WHILE_BINDING_USERTRANSACTION_TO_GEMFIRE_JNDI_TREE);
        }
      }
      else if (ne instanceof NameNotFoundException) {
        String exception = "JNDIInvoker::mapTransactions:: No TransactionManager associated to Application server context, trying to bind GemFire TransactionManager";
        if (writer.finerEnabled()) writer.finer(exception);
        try {
          transactionManager = TransactionManagerImpl.getTransactionManager();
          ctx.rebind("java:/TransactionManager", transactionManager);
          if (writer.fineEnabled())
              writer
                  .fine("JNDIInvoker::mapTransactions::Bound TransactionManager to Application Server Context");
          UserTransactionImpl utx = new UserTransactionImpl();
          ctx.rebind("java:/UserTransaction", utx);
          if (writer.fineEnabled())
              writer
                  .fine("JNDIInvoker::mapTransactions::Bound UserTransaction to Application Server Context");
        }
        catch (NamingException ne1) {
          if (writer.infoEnabled())
              writer
                  .info(LocalizedStrings.JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSNAMINGEXCEPTION_WHILE_BINDING_TRANSACTIONMANAGERUSERTRANSACTION_TO_APPLICATION_SERVER_JNDI_TREE);
        }
        catch (SystemException se1) {
          if (writer.infoEnabled())
              writer
                  .info(LocalizedStrings.JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSSYSTEMEXCEPTION_WHILE_BINDING_TRANSACTIONMANAGERUSERTRANSACTION_TO_APPLICATION_SERVER_JNDI_TREE);
        }
      }
    }
  }

  /*
   * Cleans all the DataSource ans its associated threads gracefully, before
   * start of the GemFire system. Also kills all of the threads associated with
   * the TransactionManager before making it null.
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
    int len = dataSourceList.size();
    for (int i = 0; i < len; i++) {
      if (dataSourceList.get(i) instanceof AbstractDataSource)
        ((AbstractDataSource) dataSourceList.get(i)).clearUp();
      else if (dataSourceList.get(i) instanceof ClientConnectionFactoryWrapper) {
        ((ClientConnectionFactoryWrapper) dataSourceList.get(i)).clearUp();
      }
    }
    dataSourceList.clear();
    IGNORE_JTA = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ignoreJTA");
  }

  /*
   * Helps in locating TransactionManager in presence of application server
   * scenario. Stores the value of TransactionManager for reference in GemFire
   * system.
   */
  private static void doTransactionLookup() throws NamingException {
    Object jndiObject = null;
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    for (int i = 0; i < knownJNDIManagers.length; i++) {
      try {
        jndiObject = ctx.lookup(knownJNDIManagers[i][0]);
      }
      catch (NamingException e) {
        String exception = "JNDIInvoker::doTransactionLookup::Couldn't lookup ["
            + knownJNDIManagers[i][0] + " (" + knownJNDIManagers[i][1] + ")]";
        if (writer.finerEnabled()) writer.finer(exception);
      }
      if (jndiObject instanceof TransactionManager) {
        transactionManager = (TransactionManager) jndiObject;
        String exception = "JNDIInvoker::doTransactionLookup::Found TransactionManager for "
            + knownJNDIManagers[i][1];
        if (writer.fineEnabled()) writer.fine(exception);
        return;
      } else {
        String exception = "JNDIInvoker::doTransactionLookup::Found TransactionManager of class "
          + (jndiObject == null?"null":jndiObject.getClass()) + " but is not of type javax.transaction.TransactionManager" ;
        if (writer.fineEnabled()) writer.fine(exception);
      }
    }
    Class clazz;
    try {
      if (writer.finerEnabled())
          writer
              .finer("JNDIInvoker::doTransactionLookup::Trying WebSphere 5.1: "
                  + WS_FACTORY_CLASS_5_1);
      clazz = ClassPathLoader.getLatest().forName(WS_FACTORY_CLASS_5_1);
      if (writer.fineEnabled())
          writer.fine("JNDIInvoker::doTransactionLookup::Found WebSphere 5.1: "
              + WS_FACTORY_CLASS_5_1);
    }
    catch (ClassNotFoundException ex) {
      try {
        if (writer.finerEnabled())
            writer
                .finer("JNDIInvoker::doTransactionLookup::Trying WebSphere 5.0: "
                    + WS_FACTORY_CLASS_5_0);
        clazz = ClassPathLoader.getLatest().forName(WS_FACTORY_CLASS_5_0);
        if (writer.fineEnabled())
            writer
                .fine("JNDIInvoker::doTransactionLookup::Found WebSphere 5.0: "
                    + WS_FACTORY_CLASS_5_0);
      }
      catch (ClassNotFoundException ex2) {
        try {
          clazz = ClassPathLoader.getLatest().forName(WS_FACTORY_CLASS_4);
          String exception = "JNDIInvoker::doTransactionLookup::Found WebSphere 4: "
              + WS_FACTORY_CLASS_4;
          if (writer.fineEnabled()) writer.fine(exception, ex);
        }
        catch (ClassNotFoundException ex3) {
          if (writer.finerEnabled())
              writer
                  .finer("JNDIInvoker::doTransactionLookup::Couldn't find any WebSphere TransactionManager factory class, neither for WebSphere version 5.1 nor 5.0 nor 4");
          throw new NoInitialContextException();
        }
      }
    }
    try {
      Method method = clazz.getMethod("getTransactionManager", (Class[])null);
      transactionManager = (TransactionManager) method.invoke(null, (Object[])null);
    }
    catch (Exception ex) {
      writer.warning(LocalizedStrings.JNDIInvoker_JNDIINVOKER_DOTRANSACTIONLOOKUP_FOUND_WEBSPHERE_TRANSACTIONMANAGER_FACTORY_CLASS_0_BUT_COULDNT_INVOKE_ITS_STATIC_GETTRANSACTIONMANAGER_METHOD, clazz.getName(), ex);
      throw new NameNotFoundException(LocalizedStrings.JNDIInvoker_JNDIINVOKER_DOTRANSACTIONLOOKUP_FOUND_WEBSPHERE_TRANSACTIONMANAGER_FACTORY_CLASS_0_BUT_COULDNT_INVOKE_ITS_STATIC_GETTRANSACTIONMANAGER_METHOD.toLocalizedString(new Object[] {clazz.getName()}));
    }
  }

  /**
   * Initialises the GemFire context. This is called when no external JNDI
   * Context is found. 
   * 
   * @throws NamingException
   */
  private static void initializeGemFireContext() throws NamingException {
    Hashtable table = new Hashtable();
    table.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.gemstone.gemfire.internal.jndi.InitialContextFactoryImpl");
    ctx = new InitialContext(table);
  }

  /**
   * Binds a single Datasource to the existing JNDI tree. The JNDI tree may be
   * The Datasource properties are contained in the map. The Datasource
   * implementation class is populated based on properties in the map.
   * 
   * @param map contains Datasource configuration properties.
   */
  public static void mapDatasource(Map map, List props) {
    String value = (String) map.get("type");
    String jndiName = "";
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    Object ds = null;
    try {
      jndiName = (String) map.get("jndi-name");
      if (value.equals("PooledDataSource")) {
        ds = DataSourceFactory.getPooledDataSource(map, props);
        ctx.rebind("java:/" + jndiName, ds);
        dataSourceList.add(ds);
        if (writer.fineEnabled())
            writer.fine("Bound java:/" + jndiName + " to Context");
      }
      else if (value.equals("XAPooledDataSource")) {
        ds = DataSourceFactory.getTranxDataSource(map, props);
        ctx.rebind("java:/" + jndiName, ds);
        dataSourceList.add(ds);
        if (writer.fineEnabled())
            writer.fine("Bound java:/" + jndiName + " to Context");
      }
      else if (value.equals("SimpleDataSource")) {
        ds = DataSourceFactory.getSimpleDataSource(map, props);
        ctx.rebind("java:/" + jndiName, ds);
        if (writer.fineEnabled())
            writer.fine("Bound java:/" + jndiName + " to Context");
      }
      else if (value.equals("ManagedDataSource")) {
        ClientConnectionFactoryWrapper ds1 = DataSourceFactory
            .getManagedDataSource(map, props);
        ctx.rebind("java:/" + jndiName, ds1.getClientConnFactory());
        dataSourceList.add(ds1);
        if (writer.fineEnabled())
            writer.fine("Bound java:/" + jndiName + " to Context");
      }
      else {
        String exception = "JNDIInvoker::mapDataSource::No correct type of DataSource";
        if (writer.fineEnabled()) writer.fine(exception);
        throw new DataSourceCreateException(exception);
      }
      ds = null;
    }
    catch (NamingException ne) {
      if (writer.infoEnabled()) writer.info(
          LocalizedStrings.JNDIInvoker_JNDIINVOKER_MAPDATASOURCE_0_WHILE_BINDING_1_TO_JNDI_CONTEXT,
          new Object[] {"NamingException", jndiName});
    }
    catch (DataSourceCreateException dsce) {
      if (writer.infoEnabled()) writer.info(
          LocalizedStrings.JNDIInvoker_JNDIINVOKER_MAPDATASOURCE_0_WHILE_BINDING_1_TO_JNDI_CONTEXT,
          new Object[] {"DataSourceCreateException", jndiName});
    }
  }

  /**
   * @return Context the existing JNDI Context. If there is no pre-esisting JNDI
   *         Context, the GemFire JNDI Context is returned.
   */
  public static Context getJNDIContext() {
    return ctx;
  }

  /**
   * returns the GemFire TransactionManager.
   * 
   * @return TransactionManager
   */
  public static TransactionManager getTransactionManager() {
    return transactionManager;
  }
  //try to find websphere lookups since we came here
  /*
   * private static void print(String str) { if (DEBUG) {
   * System.err.println(str); } }
   */
}
