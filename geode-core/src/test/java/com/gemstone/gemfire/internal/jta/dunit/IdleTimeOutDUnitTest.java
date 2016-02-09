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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

public class IdleTimeOutDUnitTest extends DistributedTestCase {

  static DistributedSystem ds;
  static Cache cache;
  private static String tblName;

  private static String readFile(String filename) throws IOException {
//    String lineSep = System.getProperty("\n");
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
    LogWriterUtils.getLogWriter().info("***********\n " + sb);
    return sb.toString();
  }

  public IdleTimeOutDUnitTest(String name) {
    super(name);
  }

  private static String modifyFile(String str) throws IOException {
    String search = "<jndi-binding type=\"XAPooledDataSource\"";
    String last_search = "</jndi-binding>";
    String newDB = "newDB_" + OSProcess.getId();
    String jndi_str = "<jndi-binding type=\"XAPooledDataSource\" jndi-name=\"XAPooledDataSource\" jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"300\" blocking-timeout-seconds=\"10\" login-timeout-seconds=\"25\" conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\" password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" connection-url=\"jdbc:derby:"+newDB+";create=true\" >";
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
     * init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"300\"
     * blocking-timeout-seconds=\"10\" login-timeout-seconds=\"25\"
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
    LogWriterUtils.getLogWriter().info("Start Index = " + n1);
    int n2 = str.indexOf(last_search, n1);
    StringBuffer sbuff = new StringBuffer(str);
    LogWriterUtils.getLogWriter().info("END Index = " + n2);
    String modified_str = sbuff.replace(n1, n2, new_str).toString();
    return modified_str;
  }

  public static String init(String className) throws Exception {
    LogWriterUtils.getLogWriter().fine("PATH11 ");
    Properties props = new Properties();
    String path = System.getProperty("CACHEXMLFILE");
    LogWriterUtils.getLogWriter().fine("PATH2 " + path);
    int pid = OSProcess.getId();
    path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    LogWriterUtils.getLogWriter().fine("PATH " + path);
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
    String tableName = "";
    //	        props.setProperty("mcast-port", "10339");
    try {
      //	  	      ds = DistributedSystem.connect(props);
      ds = (new IdleTimeOutDUnitTest("temp")).getSystem(props);
      cache = CacheFactory.create(ds);
      if (className != null && !className.equals("")) {
        String time = new Long(System.currentTimeMillis()).toString();
        tableName = className + time;
        createTable(tableName);
      }
    }
    catch (Exception e) {
      e.printStackTrace(System.err);
      throw new Exception("" + e);
    }
    tblName = tableName;
    return tableName;
  }

  public static void createTable(String tableName) throws NamingException,
      SQLException {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    //String sql = "create table " + tableName + " (id number primary key, name
    // varchar2(50))";
    //String sql = "create table " + tableName + " (id integer primary key,
    // name varchar(50))";
    String sql = "create table "
        + tableName
        + " (id integer NOT NULL, name varchar(50), CONSTRAINT "+tableName+"_key PRIMARY KEY(id))";
    LogWriterUtils.getLogWriter().info(sql);
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    sm.execute(sql);
    sm.close();
    sm = conn.createStatement();
    for (int i = 1; i <= 10; i++) {
      sql = "insert into " + tableName + " values (" + i + ",'name" + i + "')";
      sm.addBatch(sql);
      LogWriterUtils.getLogWriter().info(sql);
    }
    sm.executeBatch();
    conn.close();
  }

  public static void destroyTable() throws NamingException, SQLException {
    try {
      String tableName = tblName;
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      LogWriterUtils.getLogWriter().info(" trying to drop table: " + tableName);
      String sql = "drop table " + tableName;
      Statement sm = conn.createStatement();
      sm.execute(sql);
      conn.close();
    }
    catch (NamingException ne) {
      LogWriterUtils.getLogWriter().info("destroy table naming exception: " + ne);
      throw ne;
    }
    catch (SQLException se) {
      LogWriterUtils.getLogWriter().info("destroy table sql exception: " + se);
      throw se;
    }
    finally {
      closeCache();
    }
  }

  public static Cache getCache() {
    return cache;
  }

  public static void startCache() {
    try {
      if (cache.isClosed()) {
        cache = CacheFactory.create(ds);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    try {
      ds.disconnect();
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Error in disconnecting from Distributed System");
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    Object o[] = new Object[1];
    o[0] = "IdleTimeOutDUnitTest";
    vm0.invoke(IdleTimeOutDUnitTest.class, "init", o);
  }

  @Override
  protected final void preTearDown() throws Exception {
    VM vm0 = Host.getHost(0).getVM(0);
    vm0.invoke(IdleTimeOutDUnitTest.class, "closeCache");
  }

  public void testIdleTimeOut() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(IdleTimeOutDUnitTest.class, "runTest1");
    AsyncInvocation asyncObj = vm0.invokeAsync(IdleTimeOutDUnitTest.class,
        "runTest2");
    ThreadUtils.join(asyncObj, 30 * 1000);
    if(asyncObj.exceptionOccurred()){
      Assert.fail("asyncObj failed", asyncObj.getException());
    }				   
  }

  public static void runTest1() throws Exception {
    final int MAX_CONNECTIONS = 30;
    DataSource ds = null;
    try {
      Context ctx = cache.getJNDIContext();
      ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
    }
    catch (NamingException e) {
      LogWriterUtils.getLogWriter().info("Naming Exception caught in lookup: " + e);
      fail("failed in naming lookup: " + e);
      return;
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception caught during naming lookup: " + e);
      fail("failed in naming lookup: " + e);
      return;
    }
    try {
      for (int count = 0; count < MAX_CONNECTIONS; count++) {
        ds.getConnection();
        LogWriterUtils.getLogWriter().info("runTest1 :acquired connection #" + count);
      }
    }
    catch (SQLException e) {
      LogWriterUtils.getLogWriter().info("Success SQLException caught in runTest1: " + e);
      fail("runTest1 SQL Exception caught: " + e);
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception caught in runTest1: " + e);
      fail("Exception caught in runTest1: " + e);
      e.printStackTrace();
    }
  }

  public static void runTest2() throws Exception {
    final int MAX_CONNECTIONS = 30;
//     getLogWriter().info("runTest2 sleeping for 20 sec");
//     Thread.sleep(20 * 1000);
    DataSource ds = null;
    try {
      Context ctx = cache.getJNDIContext();
      ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
    }
    catch (NamingException e) {
      LogWriterUtils.getLogWriter().info("Exception caught during naming lookup: " + e);
      fail("failed in naming lookup: " + e);
      return;
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception caught during naming lookup: " + e);
      fail("failed in because of unhandled excpetion: " + e);
      return;
    }
    try {
      for (int count = 0; count < MAX_CONNECTIONS; count++) {
        Connection con = ds.getConnection();
        assertNotNull("Connection object is null", con);
        LogWriterUtils.getLogWriter().info("runTest2 :acquired connection #" + count);
      }
    }
    catch (SQLException sqle) {
      LogWriterUtils.getLogWriter().info("SQLException caught in runTest2: " + sqle);
      fail("failed because of SQL exception : " + sqle);
      sqle.printStackTrace();
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception caught in runTest2: " + e);
      fail("failed because of unhandled exception : " + e);
      e.printStackTrace();
    }
  }
}
