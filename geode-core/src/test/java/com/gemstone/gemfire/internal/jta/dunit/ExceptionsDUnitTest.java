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
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

public class ExceptionsDUnitTest extends DistributedTestCase {

  static DistributedSystem ds;
  static Cache cache;
//  private static String tblName;

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
    LogWriterUtils.getLogWriter().fine("***********\n " + sb);
    return sb.toString();
  }

  public ExceptionsDUnitTest(String name) {
    super(name);
  }

  private static String modifyFile(String str) throws IOException {
    String search = "<jndi-binding type=\"XAPooledDataSource\"";
    String last_search = "</jndi-binding>";
    String newDB = "newDB_" + OSProcess.getId();
    String jndi_str = "<jndi-binding type=\"XAPooledDataSource\" jndi-name=\"XAPooledDataSource\"          jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" init-pool-size=\"5\" max-pool-size=\"5\" idle-timeout-seconds=\"600\" blocking-timeout-seconds=\"6\" login-timeout-seconds=\"2\" conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\" password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" connection-url=\"jdbc:derby:"+newDB+";create=true\" >";
    String config_prop = "<config-property>"
        + "<config-property-name>description</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>hi</config-property-value>"
        + "</config-property>"
        + "<config-property>"
        + "<config-property-name>user</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>jeeves</config-property-value>"
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
     * init-pool-size=\"5\" max-pool-size=\"5\" idle-timeout-seconds=\"600\"
     * blocking-timeout-seconds=\"6\" login-timeout-seconds=\"2\"
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
    LogWriterUtils.getLogWriter().fine("Start Index = " + n1);
    int n2 = str.indexOf(last_search, n1);
    StringBuffer sbuff = new StringBuffer(str);
    LogWriterUtils.getLogWriter().fine("END Index = " + n2);
    String modified_str = sbuff.replace(n1, n2, new_str).toString();
    return modified_str;
  }

  public static void init() throws Exception {
    Properties props = new Properties();
    int pid = OSProcess.getId();
    String path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
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
//    String tableName = "";
    //		  props.setProperty("mcast-port", "10339");
    try {
      //			   ds = DistributedSystem.connect(props);
      ds = (new ExceptionsDUnitTest("temp")).getSystem(props);
      cache = CacheFactory.create(ds);
    }
    catch (Exception e) {
      e.printStackTrace(System.err);
      throw new Exception("" + e);
    }
  }

  public static Cache getCache() {
    return cache;
  }

  public static void startCache() {
    try {
      if (cache == null || cache.isClosed()) {
        cache = CacheFactory.create(ds);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      if (cache != null && !cache.isClosed()) {
        cache.close();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    try {
      if (ds != null) ds.disconnect();
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().fine("Error in disconnecting from Distributed System");
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(ExceptionsDUnitTest.class, "init");
  }

  @Override
  protected final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(ExceptionsDUnitTest.class, "closeCache");
  }

  public static void testBlockingTimeOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(ExceptionsDUnitTest.class, "runTest1");
  }

  public static void testLoginTimeOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(ExceptionsDUnitTest.class, "runTest2");
  }

  public static void testTransactionTimeOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(ExceptionsDUnitTest.class, "runTest3");
  }

  public static void runTest1() throws Exception {
    boolean exceptionOccured = false;
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds1 = null;
      DataSource ds2 = null;
      ds1 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      ds1 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      ds1.getConnection();
      Thread.sleep(8000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured)
          fail("Exception did not occur on commit although was supposed"
              + "occur");
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().fine("Exception caught in runTest1 due to : " + e);
      fail("failed in runTest1 due to " + e);
    }
  }

  public static void runTest2() throws Exception {
    boolean exceptionOccured1 = false;
    boolean exceptionOccured2 = false;
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds1 = null;
      DataSource ds2 = null;
      ds1 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      ds1.getConnection();
      ds1.getConnection();
      ds1.getConnection();
      ds1.getConnection();
      ds1.getConnection();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      try {
        ds1.getConnection();
        Thread.sleep(8000);
      }
      catch (SQLException e) {
        exceptionOccured1 = true;
      }
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured2 = true;
      }
      if (!exceptionOccured1)
          fail("Exception (Login-Time-Out)did not occur although was supposed"
              + "to occur");
      if (exceptionOccured2)
          fail("Exception did occur on commit, although was not supposed"
              + "to occur");
    }
    catch (Exception e) {
      fail("failed in runTest2 due to " + e);
    }
  }

  public static void runTest3() throws Exception {
    boolean exceptionOccured = false;
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds1 = null;
      DataSource ds2 = null;
      ds1 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(2);
      ds1.getConnection();
      Thread.sleep(4000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured)
          fail("Exception (Transaction-Time-Out)did not occur although was supposed"
              + "to occur");
    }
    catch (Exception e) {
      fail("failed in runTest3 due to " + e);
    }
  }
}
