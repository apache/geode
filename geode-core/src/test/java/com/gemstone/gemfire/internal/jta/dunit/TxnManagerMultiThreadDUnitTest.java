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

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.internal.jta.JTAUtils;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * This test case is to test the following test scenarios: 1) Behaviour of
 * Transaction Manager in multy threaded thransactions. We will have around five
 * threads doing different activities. The response to those activities by
 * transaction manager is tested.
 * 
 * 
 * @author Prafulla Chaudhari
 *  
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TxnManagerMultiThreadDUnitTest extends DistributedTestCase {

  /////constructor/////
  public TxnManagerMultiThreadDUnitTest(String name) {
    super(name);
  }

  public static DistributedSystem ds;
  public static Cache cache;

  /////Methods for initializations/////
  private static String readFile(String filename) throws IOException {
//    String lineSep = System.getProperty("\n");
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String nextLine = "";
    StringBuffer sb = new StringBuffer();
    while ((nextLine = br.readLine()) != null) {
      sb.append(nextLine);
    }
    //getLogWriter().fine("***********\n "+ sb);
    return sb.toString();
  }//end of readFile

  private static String modifyFile(String str) throws IOException {
    String search = "<jndi-binding type=\"XAPooledDataSource\"";
    String last_search = "</jndi-binding>";
    String newDB = "newDB1_" + OSProcess.getId();
    String jndi_str = "<jndi-binding type=\"XAPooledDataSource\" jndi-name=\"XAPooledDataSource\" jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"300\" blocking-timeout-seconds=\"15\" login-timeout-seconds=\"30\" conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\" password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" connection-url=\"jdbc:derby:"+newDB+";create=true\" >";
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
     * blocking-timeout-seconds=\"15\" login-timeout-seconds=\"30\"
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
    int n2 = str.indexOf(last_search, n1);
    StringBuffer sbuff = new StringBuffer(str);
    String modified_str = sbuff.replace(n1, n2, new_str).toString();
    return modified_str;
  }//end of modifyFile

  private static String modifyFile1(String str) throws IOException {
    String search = "<jndi-binding type=\"SimpleDataSource\"";
    String last_search = "</jndi-binding>";
    String newDB = "newDB1_" + OSProcess.getId();
    String jndi_str = "<jndi-binding type=\"SimpleDataSource\" jndi-name=\"SimpleDataSource\" jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"300\" blocking-timeout-seconds=\"15\" login-timeout-seconds=\"30\" conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\" password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" connection-url=\"jdbc:derby:"+newDB+";create=true\" >";
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
     * String new_str = " <jndi-binding type=\"SimpleDataSource\"
     * jndi-name=\"SimpleDataSource\"
     * jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\"
     * init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"300\"
     * blocking-timeout-seconds=\"15\" login-timeout-seconds=\"30\"
     * conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\"
     * xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\"
     * user-name=\"mitul\"
     * password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\"
     * connection-url=\"jdbc:derby:"+newDB+";create=true\" > <property
     * key=\"description\" value=\"hi\"/> <property key=\"databaseName\"
     * value=\""+newDB1+"\"/> <property key=\"user\" value=\"mitul\"/> <property
     * key=\"password\"
     * value=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\"/>";
     */
    int n1 = str.indexOf(search);
    int n2 = str.indexOf(last_search, n1);
    StringBuffer sbuff = new StringBuffer(str);
    String modified_str = sbuff.replace(n1, n2, new_str).toString();
    return modified_str;
  }//end of modifyFile

  public static String init(String className) throws Exception {
    DistributedSystem ds;
    Properties props = new Properties();
    String jtest = System.getProperty("JTESTS");
    int pid = OSProcess.getId();
    String path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    LogWriterUtils.getLogWriter().fine("PATH " + path);
    /** * Return file as string and then modify the string accordingly ** */
    String file_as_str = readFile(TestUtil.getResourcePath(CacheUtils.class, "cachejta.xml"));
    file_as_str = file_as_str.replaceAll("newDB", "newDB_" + pid);
    String modified_file_str = modifyFile(file_as_str);
    String modified_file_str1 = modifyFile1(modified_file_str);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str1);
    wr.flush();
    wr.close();
    props.setProperty("cache-xml-file", path);
    String tableName = "";
    //    props.setProperty("mcast-port", "10339");
    try {
      //        ds = DistributedSystem.connect(props);
      ds = (new TxnManagerMultiThreadDUnitTest("temp")).getSystem(props);
      CacheUtils.ds = ds;
      cache = CacheFactory.create(ds);
      if (className != null && !className.equals("")) {
        String time = new Long(System.currentTimeMillis()).toString();
        tableName = className + time;
        createTable(tableName);
      }
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("", e);
      throw new Exception("" + e);
    }
    return tableName;
  }//end of init method

  public static void createTable(String tblName) throws NamingException,
      SQLException {
    String tableName = tblName;
    cache = TxnManagerMultiThreadDUnitTest.getCache();
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    String sql = "create table "
        + tableName
        + " (id integer NOT NULL, name varchar(50), CONSTRAINT "+tableName+"_key PRIMARY KEY(id))";
    LogWriterUtils.getLogWriter().fine(sql);
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    sm.execute(sql);
    sm.close();
    sm = conn.createStatement();
    for (int i = 1; i <= 10; i++) {
      sql = "insert into " + tableName + " values (" + i + ",'name" + i + "')";
      sm.addBatch(sql);
      LogWriterUtils.getLogWriter().fine(sql);
    }
    sm.executeBatch();
    conn.close();
  }//end of createTable

  public static void destroyTable(String tblName) throws NamingException,
      SQLException {
    //the sleep below is given to give the time to threads to start and perform
    // before destroyTable is called.
    try {
      Thread.sleep(10 * 1000);
    }
    catch (InterruptedException ie) {
      fail("interrupted");
    }
    try {
      String tableName = tblName;
      LogWriterUtils.getLogWriter().fine("Destroying table: " + tableName);
      cache = TxnManagerMultiThreadDUnitTest.getCache();
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      LogWriterUtils.getLogWriter().fine(" trying to drop table: " + tableName);
      String sql = "drop table " + tableName;
      Statement sm = conn.createStatement();
      sm.execute(sql);
      conn.close();
      LogWriterUtils.getLogWriter().fine("destroyTable is Successful!");
    }
    catch (NamingException ne) {
      LogWriterUtils.getLogWriter().fine("destroy table naming exception: " + ne);
      throw ne;
    }
    catch (SQLException se) {
      LogWriterUtils.getLogWriter().fine("destroy table sql exception: " + se);
      throw se;
    }
    finally {
      LogWriterUtils.getLogWriter().fine("Closing cache...");
      closeCache();
    }
  }//end of destroyTable

  public static Cache getCache() {
    return cache;
  }//end of getCache

  public static void startCache() {
    try {
      if (cache.isClosed()) {
        cache = CacheFactory.create(ds);
      }
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().warning("exception while creating cache", e);
    }
  }//end of startCache

  public static void closeCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
        LogWriterUtils.getLogWriter().fine("Cache closed");
      }
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().warning("exception while closing cache", e);
    }
    try {
      CacheUtils.ds.disconnect();
      LogWriterUtils.getLogWriter().fine("Disconnected from Distribuited System");
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().fine("Error in disconnecting from Distributed System");
    }
  }//end of closeCache

  /////setUp and tearDown methods/////
  public void setUp() throws java.lang.Exception {
    super.setUp();
    VM vm0 = Host.getHost(0).getVM(0);
    Object o[] = new Object[1];
    o[0] = "TxnManagerMultiThreadDUnitTest";
    Object tableName = vm0.invoke(TxnManagerMultiThreadDUnitTest.class, "init",
        o);
    //setting the table name using the CacheUtils method setTableName
    //This is to access the table name in test methods
    CacheUtils.setTableName(tableName.toString());
    //set the tableName in CacheUtils of remote VM
    o[0] = tableName;
    vm0.invoke(CacheUtils.class, "setTableName", o);
    //delete the rows which are inseted in CacheUtils.init by calling delRows
    // method
    vm0.invoke(TxnManagerMultiThreadDUnitTest.class, "delRows");
  }

  public static void delRows() {
    Region currRegion = null;
    Cache cache;
    //get the cache
    //this is used to get the context for passing to the constructor of
    // JTAUtils
    cache = TxnManagerMultiThreadDUnitTest.getCache();
    currRegion = cache.getRegion("/root");
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    //to delete all rows inserted in creatTable () of this class
    //deleteRows method of JTAUtils class is used.
    try {
      //get the table name from CacheUtils
      String tblName_delRows = CacheUtils.getTableName();
      /*int rowsDeleted = */jtaObj.deleteRows(tblName_delRows);
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().warning("Error: while deleting rows from database using JTAUtils", e);
    }
  }//end of delRows

  @Override
  protected final void preTearDown() throws Exception {
    VM vm0 = Host.getHost(0).getVM(0);
    //get tableName to pass to destroyTable
    String tableName = CacheUtils.getTableName();
    Object o[] = new Object[1];
    o[0] = tableName;
    //call the destroyTable method of the same class
    //that takes care of destroying table, closing cache, disconnecting from
    // DistributedSystem
    vm0.invoke(TxnManagerMultiThreadDUnitTest.class, "destroyTable", o);
  }//end of tearDown

  /*
   * calldestroyTable method is written to destroyTable, close cache and
   * disconnect from DistributedSystem. This method calls the destroyTable
   * method of this class.
   */
  /*
   * 
   * public static void calldestroyTable (String tableName){
   * 
   * //the sleep below is given to give the time to threads to start and perform
   * before destroyTable is called. try { Thread.sleep(20*1000); } catch
   * (InterruptedException ie) { ie.printStackTrace(); } try { String tblName =
   * tableName; getLogWriter().fine ("Destroying table: " + tblName);
   * TxnManagerMultiThreadDUnitTest.destroyTable(tblName); getLogWriter().fine
   * ("Closing cache..."); getLogWriter().fine("destroyTable is Successful!"); }
   * catch (Exception e) { fail (" failed during tear down of this test..." +
   * e); } finally { closeCache(); ds.disconnect(); } }//end of destroyTable
   *  
   */
  public static void getNumberOfRows() {
    //the sleep below is given to give the time to threads to start and perform
    // before getNumberOfRows is called.
    try {
      Thread.sleep(7 * 1000);
    }
    catch (InterruptedException ie) {
      fail("interrupted");
    }
    Region currRegion = null;
    Cache cache;
    //get the cache
    //this is used to get the context for passing to the constructor of
    // JTAUtils
    cache = TxnManagerMultiThreadDUnitTest.getCache();
    //get the table name from CacheUtils
    String tblName = CacheUtils.getTableName();
    currRegion = cache.getRegion("/root");
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    //get how many rows actually got committed
    try {
      int rows = jtaObj.getRows(tblName);
      LogWriterUtils.getLogWriter().fine("Number of rows committed current test method  are: "
          + rows);
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().warning("Error: while getting rows from database using JTAUtils", e);
    }
  }//end of getNumberOfRows

  /////test methods/////
  /*
   * The following method testAllCommit test the scenario in which all four
   * threads are committing the transaction
   */
  public static void test1AllCommit() throws Throwable {
    VM vm0 = Host.getHost(0).getVM(0);
    AsyncInvocation asyncObj1 = vm0.invokeAsync(
        TxnManagerMultiThreadDUnitTest.class, "callCommitThreads");
    ThreadUtils.join(asyncObj1, 30 * 1000);
    if(asyncObj1.exceptionOccurred()){
      Assert.fail("asyncObj1 failed", asyncObj1.getException());
    }
    vm0.invoke(TxnManagerMultiThreadDUnitTest.class, "getNumberOfRows");
  }//end of testAllCommit

  /*
   * This method gives call to CommitThread
   *  
   */
  public static void callCommitThreads() {
    LogWriterUtils.getLogWriter().fine("This is callCommitThreads method");
    try {
      new CommitThread("ct1", LogWriterUtils.getLogWriter());
      new CommitThread("ct2", LogWriterUtils.getLogWriter());
      new CommitThread("ct3", LogWriterUtils.getLogWriter());
      new CommitThread("ct4", LogWriterUtils.getLogWriter());
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().warning("Failed in Commit Threads", e);
      fail("Failed in Commit Threads" + e);
    }
  }//end of callCommitTheads

  /*
   * The following method test3Commit2Rollback test the scenario in which 3
   * threads are committing and 2 threads are rolling back the transaction
   */
  public static void _test3Commit2Rollback() throws Throwable {
    VM vm0 = Host.getHost(0).getVM(0);
    AsyncInvocation asyncObj1 = vm0.invokeAsync(
        TxnManagerMultiThreadDUnitTest.class, "callCommitandRollbackThreads");
    ThreadUtils.join(asyncObj1, 30 * 1000);
    if(asyncObj1.exceptionOccurred()){
      Assert.fail("asyncObj1 failed", asyncObj1.getException());
    }
    vm0.invoke(TxnManagerMultiThreadDUnitTest.class, "getNumberOfRows");
  }//end of test3Commit2Rollback

  public static void callCommitandRollbackThreads() {
    LogWriterUtils.getLogWriter().fine("This is callCommitandRollbackThreads method");
    try {
      new CommitThread("ct1", LogWriterUtils.getLogWriter());
      new CommitThread("ct2", LogWriterUtils.getLogWriter());
      new CommitThread("ct3", LogWriterUtils.getLogWriter());
      new RollbackThread("rt1", LogWriterUtils.getLogWriter());
      new RollbackThread("rt2", LogWriterUtils.getLogWriter());
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Failed in Commit and Rollback threads", e);
      fail("Failed in Commit and Rollback Threads" + e);
    }
  }//end of callCommitandRollbackThreads
  /*
   * The following method test3Commit2BlockingTimeOut over test the scenario in
   * which 3 threads are committing and 2 threads are passing blocking time out
   * and can not commit the transaction
   */
  /*
   * public static void test3Commit2BlockingTimeOut(){ VM vm0 =
   * Host.getHost(0).getVM(0); AsyncInvocation asyncObj1 =
   * vm0.invokeAsync(TxnManagerMultiThreadDUnitTest.class,
   * "callCommitandBlockingTimeOutThreads"); try { asyncObj1.join (); } catch
   * (InterruptedException e) { fail ("Current thread experienced Interrupted
   * Exception !"); }
   * 
   * //vm0.invoke(TxnManagerMultiThreadDUnitTest.class, "getNumberOfRows");
   * 
   * }//end of test3Commit2Rollback
   *  
   */
  /*
   * public static void callCommitandBlockingTimeOutThreads(){
   * 
   * getLogWriter().fine("This is callCommitandBlockingTimeOutThreads method");
   * try{ CommitThread ct1 = new CommitThread ("ct1"); CommitThread ct2 = new
   * CommitThread ("ct2"); CommitThread ct3 = new CommitThread ("ct3");
   * BlockingTimeOutThread bto1 = new BlockingTimeOutThread ("bto1");
   * BlockingTimeOutThread bto2 = new BlockingTimeOutThread ("bto2"); } catch
   * (Exception e){ fail("Failed in Commit and Blocking Time Out Threads" + e);
   * e.printStackTrace(); }
   * 
   * //make thread to sleep for more than blocking time out just to avoid call
   * to destroyTable method //This will help to have db in place and try commit
   * after blocking time out is passed by thread try { Thread.sleep(25*1000); }
   * catch (InterruptedException ie) { ie.printStackTrace(); }
   * 
   * }//end of callCommitandRollbackThreads
   *  
   */

   public void testFoo() {}
}
