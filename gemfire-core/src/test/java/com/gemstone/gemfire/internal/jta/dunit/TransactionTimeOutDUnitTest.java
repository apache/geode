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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.datasource.GemFireTransactionDataSource;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.internal.jta.UserTransactionImpl;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

/**
*@author Mitul D Bid
*This test tests TransactionTimeOut functionality
*/
public class TransactionTimeOutDUnitTest extends DistributedTestCase {

  static DistributedSystem ds;
  static Cache cache = null;
  private static String tblName;

  public TransactionTimeOutDUnitTest(String name) {
    super(name);
  }

  public static void init() throws Exception {
    Properties props = new Properties();
    int pid = OSProcess.getId();
    String path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    String file_as_str = readFile(TestUtil.getResourcePath(CacheUtils.class, "cachejta.xml"));
    String modified_file_str= file_as_str.replaceAll("newDB", "newDB_" + pid);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();

    props.setProperty("cache-xml-file", path);
//    props.setProperty("mcast-port", "10321");
    try {
//      ds = DistributedSystem.connect(props);
        ds = (new TransactionTimeOutDUnitTest("temp")).getSystem(props);
      if (cache == null || cache.isClosed()) cache = CacheFactory.create(ds);
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
      if (cache.isClosed()) {
        cache = CacheFactory.create(ds);
      }
    }
    catch (Exception e) {
      fail("Exception in cache creation due to "+e);
	  e.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      if (cache != null && !cache.isClosed()) {
        cache.close();
		}
	  if (ds != null && ds.isConnected())
		   ds.disconnect();
    }
    catch (Exception e) {
	fail("Exception in closing cache and disconnecting ds due to "+e);
      e.printStackTrace();
    }
  }

  public  void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "init");
  }

  @Override
  protected final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "closeCache");
  }

  public static void testTimeOut() throws Throwable{
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    AsyncInvocation async1 = vm0.invokeAsync(TransactionTimeOutDUnitTest.class, "runTest1");
    AsyncInvocation async2 =vm0.invokeAsync(TransactionTimeOutDUnitTest.class, "runTest2");
    
    ThreadUtils.join(async1, 30 * 1000);
    ThreadUtils.join(async2, 30 * 1000);
    if(async1.exceptionOccurred()){
      Assert.fail("async1 failed", async1.getException());
    }
    if(async2.exceptionOccurred()){
      Assert.fail("async2 failed", async2.getException());
    }
  }

  public static void test1() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest3");
  }

  public static void test2() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest4");
  }

  public static void test3() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest5");
  }

  public static void test4() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest6");
  }

  public static void test5() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest7");
  }

  public static void test6() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest8");
  }

  public static void test7() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest9");
  }

  public static void test8() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TransactionTimeOutDUnitTest.class, "runTest10");
  }

  public static void runTest1() throws Exception {
    boolean exceptionOccured = false;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured)
          fail("Exception did not occur although was supposed to occur");
      return;
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().fine("Exception caught " + e);
      fail("failed in naming lookup: " + e);
      return;
    }
  }

  public static void runTest2() throws Exception {
    boolean exceptionOccured = false;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured)
          fail("Exception did not occur although was supposed to occur");
      return;
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().fine("Exception caught " + e);
      fail("failed in naming lookup: " + e);
      return;
    }
  }

  public static void runTest3() {
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      utx.begin();
      utx.commit();
    }
    catch (Exception e) {
      fail("Exception in TestSetTransactionTimeOut due to " + e);
    }
  }

  public static void runTest4() {
    boolean exceptionOccured = true;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx = new UserTransactionImpl();
      utx.setTransactionTimeout(2);
      utx.begin();
      Thread.sleep(4000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    }
    catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public static void runTest5() {
    boolean exceptionOccured = true;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx = new UserTransactionImpl();
      utx.setTransactionTimeout(10);
      utx.begin();
      utx.setTransactionTimeout(8);
      utx.setTransactionTimeout(6);
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    }
    catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public static void runTest6() {
    boolean exceptionOccured = true;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.setTransactionTimeout(4);
      utx.begin();
      Thread.sleep(6000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    }
    catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public static void runTest7() {
//    boolean exceptionOccured = true;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(6);
      Thread.sleep(2000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        fail("Transaction failed to commit although TimeOut was not exceeded due to "
            + e);
      }
    }
    catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public static void runTest8() {
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable1 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      Thread.sleep(5000);
      utx.setTransactionTimeout(20);
      utx.setTransactionTimeout(10);
      sql = "insert into newTable1  values (1)";
      sm.execute(sql);
      utx.commit();
      sql = "select * from newTable1 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable1";
      sm.execute(sql);
      sm.close();
      conn.close();
    }
    catch (Exception e) {
      fail("Exception occured in test Commit due to " + e);
      e.printStackTrace();
    }
  }

  public static void runTest9() {
    try {
      boolean exceptionOccured = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable2 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      sql = "insert into newTable2  values (1)";
      sm.execute(sql);
      sql = "select * from newTable2 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable2";
      sm.execute(sql);
      sm.close();
      conn.close();
      utx.setTransactionTimeout(1);
      Thread.sleep(3000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("exception did not occur on commit although transaction timed out");
      }
    }
    catch (Exception e) {
      fail("Exception occured in test Commit due to " + e);
      e.printStackTrace();
    }
  }

  public static void runTest10() {
    try {
      boolean exceptionOccured = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable3 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      sql = "insert into newTable3  values (1)";
      sm.execute(sql);
      sql = "select * from newTable3 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable3";
      sm.execute(sql);
      sm.close();
      conn.close();
      utx.setTransactionTimeout(1);
      Thread.sleep(3000);
      try {
        utx.rollback();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("exception did not occur on rollback although transaction timed out");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Exception occured in test Commit due to " + e);
    }
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
    LogWriterUtils.getLogWriter().fine("***********\n " + sb);
    return sb.toString();
  }
}
