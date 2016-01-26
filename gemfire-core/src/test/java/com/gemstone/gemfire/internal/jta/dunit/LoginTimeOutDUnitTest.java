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
package com.gemstone.gemfire.internal.jta.dunit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

public class LoginTimeOutDUnitTest extends DistributedTestCase {
  private static final Logger logger = LogService.getLogger();

  protected static volatile boolean runTest1Ready = false;
  protected static volatile boolean runTest2Done = false;
  
  private static Cache cache;
  private static String tblName;

  public LoginTimeOutDUnitTest(String name) {
    super(name);
  }

  private static String readFile(String filename) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String nextLine = "";
    StringBuffer sb = new StringBuffer();
    while ((nextLine = br.readLine()) != null) {
      sb.append(nextLine);
      //
      // note:
      //   BufferedReader strips the EOL character.
      //
      //    sb.append(lineSep);
    }
    logger.debug("***********\n " + sb);
    return sb.toString();
  }

  private static String modifyFile(String str) throws IOException {
    String search = "<jndi-binding type=\"XAPooledDataSource\"";
    String last_search = "</jndi-binding>";
    String newDB = "newDB_" + OSProcess.getId();
    String jndi_str = "<jndi-binding type=\"XAPooledDataSource\" jndi-name=\"XAPooledDataSource\"          jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" init-pool-size=\"1\" max-pool-size=\"1\" idle-timeout-seconds=\"600\" blocking-timeout-seconds=\"60\" login-timeout-seconds=\"1\" conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\" password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" connection-url=\"jdbc:derby:"+newDB+";create=true\" >";
    String config_prop = "<config-property>"
        + "<config-property-name>description</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>hi</config-property-value>"
        + "</config-property>"
        + "<config-property>"
        + "<config-property-name>user</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>mitul</config-property-value>"
        + "</config-property>"
        + "<config-property>"
        + "<config-property-name>password</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a</config-property-value>        "
        + "</config-property>" + "<config-property>"
        + "<config-property-name>databaseName</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>"+newDB+"</config-property-value>"
        + "</config-property>\n";
    String new_str = jndi_str + config_prop;
    /*
     * String new_str = " <jndi-binding type=\"XAPooledDataSource\"
     * jndi-name=\"XAPooledDataSource\"
     * jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\"
     * init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"600\"
     * blocking-timeout-seconds=\"60\" login-timeout-seconds=\"20\"
     * conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\"
     * xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\"
     * user-name=\"mitul\"
     * password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\"
     * connection-url=\"jdbc:derby:"+newDB+";create=true\" > <property
     * key=\"description\" value=\"hi\"/> <property key=\"databaseName\"
     * value=\""+newDB+"\"/> <property key=\"user\" value=\"mitul\"/> <property
     * key=\"password\"
     * value=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\"/>";
     */
    int n1 = str.indexOf(search);
    logger.debug("Start Index = " + n1);
    int n2 = str.indexOf(last_search, n1);
    StringBuffer sbuff = new StringBuffer(str);
    logger.debug("END Index = " + n2);
    String modified_str = sbuff.replace(n1, n2, new_str).toString();
    return modified_str;
  }

  public static String init(String className) throws Exception {
    logger.debug("PATH11 ");
    Properties props = new Properties();
    int pid = OSProcess.getId();
    String path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    logger.debug("PATH " + path);
    /** * Return file as string and then modify the string accordingly ** */
    String file_as_str = readFile(TestUtil.getResourcePath(CacheUtils.class, "cachejta.xml"));
    file_as_str = file_as_str.replaceAll("newDB", "newDB_" + pid);
    String modified_file_str = modifyFile(file_as_str);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();
    props.setProperty("cache-xml-file", path);
    props.setProperty("mcast-port", "0");
    String tableName = "";
    cache = new CacheFactory(props).create();
    if (className != null && !className.equals("")) {
      String time = new Long(System.currentTimeMillis()).toString();
      tableName = className + time;
      createTable(tableName);
    }
    tblName = tableName;
    return tableName;
  }

  public static void createTable(String tableName) throws Exception {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    String sql = "create table " + tableName + " (id integer NOT NULL, name varchar(50), CONSTRAINT "+tableName+"_key PRIMARY KEY(id))";
    logger.debug(sql);
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    sm.execute(sql);
    sm.close();
    sm = conn.createStatement();
    for (int i = 1; i <= 10; i++) {
      sql = "insert into " + tableName + " values (" + i + ",'name" + i + "')";
      sm.addBatch(sql);
      logger.debug(sql);
    }
    sm.executeBatch();
    conn.close();
  }

  public static void destroyTable() throws Exception {
    try {
      String tableName = tblName;
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      logger.debug(" trying to drop table: " + tableName);
      String sql = "drop table " + tableName;
      Statement sm = conn.createStatement();
      sm.execute(sql);
      conn.close();
    }
    catch (NamingException ne) {
      logger.debug("destroy table naming exception: " + ne);
      throw ne;
    }
    catch (SQLException se) {
      logger.debug("destroy table sql exception: " + se);
      throw se;
    }
    finally {
      closeCache();
    }
  }

  private static void closeCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    }
    finally {
      InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
      if (ids != null) {
         ids.disconnect();
      }
      cache = null;
      tblName = null;
    }
  }

  public void disabledsetUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    Object o[] = new Object[1];
    o[0] = getUniqueName();
    vm0.invoke(LoginTimeOutDUnitTest.class, "init", o);
  }

  public void disabledtearDown2() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    try {
      vm0.invoke(LoginTimeOutDUnitTest.class, "destroyTable");
    } catch (Exception e) {
      if ( (e instanceof RMIException) || (e instanceof SQLException)) {
        // sometimes we have lock timeout problems destroying the table in
        // this test
        vm0.invoke(DistributedTestCase.class, "disconnectFromDS");
      }
    }
  }
  
  public void testBug52206() {
    // reenable setUp and testLoginTimeout to work on bug 52206
  }
  
  // this test and the setUp and teardown2 methods are disabled due to frequent
  // failures in CI runs.  See bug #52206
  public void disabledtestLoginTimeOut() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    AsyncInvocation test1 = vm0.invokeAsync(LoginTimeOutDUnitTest.class, "runTest1");
    AsyncInvocation test2 = vm0.invokeAsync(LoginTimeOutDUnitTest.class, "runTest2");
    DistributedTestCase.join(test2, 120 * 1000, getLogWriter());
    if(test2.exceptionOccurred()){
      fail("asyncObj failed", test2.getException());
    }
    DistributedTestCase.join(test1, 30000, getLogWriter());
  }

  public static void runTest1() throws Exception {
    final int MAX_CONNECTIONS = 1;
    DataSource ds = null;
    try {
      try {
        Context ctx = cache.getJNDIContext();
        ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      }
      catch (NamingException e) {
        logger.debug("Naming Exception caught in lookup: " + e);
        fail("failed in naming lookup: " + e);
      }
      catch (Exception e) {
        logger.debug("Exception caught during naming lookup: {}", e);
        fail("failed in naming lookup: " + e);
      }
      try {
        for (int count = 0; count < MAX_CONNECTIONS; count++) {
          ds.getConnection();
        }
      }
      catch (Exception e) {
        logger.debug("Exception caught in runTest1: {}", e);
        fail("Exception caught in runTest1: " + e);
      }
    } finally {
      runTest1Ready = true;
    }
    logger.debug("runTest1 got all of the goodies and is now sleeping");
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return runTest2Done;  
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
  }

  public static void runTest2() throws Exception {
    try {
      logger.debug("runTest2 sleeping");
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return runTest1Ready;  
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
      
      DataSource ds = null;
      try {
        Context ctx = cache.getJNDIContext();
        ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      }
      catch (NamingException e) {
        logger.debug("Exception caught during naming lookup: " + e);
        fail("failed in naming lookup: " + e);
      }
      catch (Exception e) {
        logger.debug("Exception caught during naming lookup: " + e);
        fail("failed in because of unhandled excpetion: " + e);
      }
      try {
        logger.debug("runTest2 about to acquire connection ");
        ds.getConnection();
        fail("expected a Login time-out exceeded");
      }
      catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("Login time-out exceeded") == -1) {
          logger.debug("Exception caught in runTest2: {}", sqle);
          fail("failed because of unhandled exception : " + sqle) ;
        }
      }
      catch (Exception e) {
        logger.debug("Exception caught in runTest2: {}", e);
        fail("failed because of unhandled exception : " + e);
      }
    } finally {
      // allow runtest1 to exit
      runTest2Done = true;
    }
  }
}
