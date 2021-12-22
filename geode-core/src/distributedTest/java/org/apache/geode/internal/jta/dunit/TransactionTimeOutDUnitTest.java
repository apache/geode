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
package org.apache.geode.internal.jta.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.UserTransaction;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.datasource.GemFireTransactionDataSource;
import org.apache.geode.internal.jta.CacheUtils;
import org.apache.geode.internal.jta.UserTransactionImpl;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * This test tests TransactionTimeOut functionality
 */

public class TransactionTimeOutDUnitTest extends JUnit4DistributedTestCase {

  private static DistributedSystem ds;
  private static Cache cache = null;

  private void init() throws Exception {
    Properties props = new Properties();
    int pid = OSProcess.getId();
    String path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    String file_as_str = readFile(
        createTempFileFromResource(CacheUtils.class, "cachejta.xml")
            .getAbsolutePath());
    String modified_file_str = file_as_str.replaceAll("newDB", "newDB_" + pid);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();

    props.setProperty(CACHE_XML_FILE, path);

    ds = (new TransactionTimeOutDUnitTest()).getSystem(props);
    if (cache == null || cache.isClosed()) {
      cache = CacheFactory.create(ds);
    }
  }

  private void closeCache() {
    try {
      if (cache != null && !cache.isClosed()) {
        cache.close();
      }
      if (ds != null && ds.isConnected()) {
        ds.disconnect();
      }
    } catch (Exception e) {
      fail("Exception in closing cache and disconnecting ds due to ", e);
    }
  }

  @Override
  public final void postSetUp() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> init());
  }

  @Override
  public final void preTearDown() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> closeCache());
  }

  @Test
  public void testTimeOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    AsyncInvocation async1 = vm0.invokeAsync(() -> userTransactionCanBeTimedOut());
    AsyncInvocation async2 = vm0.invokeAsync(() -> userTransactionCanBeTimedOut());

    ThreadUtils.join(async1, 30 * 1000);
    ThreadUtils.join(async2, 30 * 1000);
    if (async1.exceptionOccurred()) {
      Assert.fail("async1 failed", async1.getException());
    }
    if (async2.exceptionOccurred()) {
      Assert.fail("async2 failed", async2.getException());
    }
  }

  @Test
  public void startNewTransactionAfterExistingOneTimedOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> canStartANewUserTransactionAfterExistingOneTimedOut());
  }

  @Test
  public void testUserTransactionIsTimedOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> verifyUserTransactionIsTimedOut());
  }

  @Test
  public void testTransactionTimeoutCanBeSetMultipleTimes() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> transactionTimeoutCanBeSetMultipleTimes());
  }

  @Test
  public void testTransactionCanBeCommittedBeforeTimedOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> transactionCanBeCommittedBeforeTimedOut());
  }

  @Test
  public void testTransactionUpdatingExternalDataSourceIsCommittedBeforeTimedOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> transactionUpdatingExternalDataSourceIsCommittedBeforeTimedOut());
  }

  @Test
  public void testTimedOutTransactionIsNotCommitted() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> timedOutTransactionIsNotCommitted());
  }

  @Test
  public void testTransactionUpdatingExternalDataSourceIsTimedOut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> transactionUpdatingExternalDataSourceIsTimedOut());
  }

  private void userTransactionCanBeTimedOut() {
    boolean exceptionOccurred = false;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      utx.begin();
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      utx.setTransactionTimeout(2);
      waitUntilTransactionTimeout(utx);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccurred = true;
      }
      if (!exceptionOccurred) {
        fail("Exception did not occur although was supposed to occur");
      }
    } catch (Exception e) {
      fail("failed in naming lookup: ", e);
    }
  }

  private static void waitUntilTransactionTimeout(UserTransaction utx) {
    GeodeAwaitility.await().pollInSameThread()
        .until(() -> utx.getStatus() == Status.STATUS_NO_TRANSACTION);
  }

  private void canStartANewUserTransactionAfterExistingOneTimedOut() {
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      utx.begin();
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      utx.setTransactionTimeout(2);
      waitUntilTransactionTimeout(utx);
      utx.begin();
      utx.commit();
    } catch (Exception e) {
      fail("Exception in TestSetTransactionTimeOut due to ", e);
    }
  }

  private void verifyUserTransactionIsTimedOut() {
    boolean exceptionOccurred = true;
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      utx.setTransactionTimeout(2);
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      waitUntilTransactionTimeout(utx);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccurred = false;
      }
      if (exceptionOccurred) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to ", e);
    }
  }

  private void transactionTimeoutCanBeSetMultipleTimes() {
    boolean exceptionOccurred = true;
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.setTransactionTimeout(10);
      utx.begin();
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      utx.setTransactionTimeout(8);
      utx.setTransactionTimeout(6);
      utx.setTransactionTimeout(2);
      waitUntilTransactionTimeout(utx);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccurred = false;
      }
      if (exceptionOccurred) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to ", e);
    }
  }

  private void transactionCanBeCommittedBeforeTimedOut() {
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      utx.begin();
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      utx.setTransactionTimeout(30);
      Thread.sleep(2000);
      try {
        utx.commit();
      } catch (Exception e) {
        fail("Transaction failed to commit although TimeOut was not exceeded due to ", e);
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to ", e);
    }
  }

  private void transactionUpdatingExternalDataSourceIsCommittedBeforeTimedOut() {
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds =
          (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable1 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      Thread.sleep(2000);
      utx.setTransactionTimeout(20);
      sql = "insert into newTable1  values (1)";
      sm.execute(sql);
      utx.commit();
      sql = "select * from newTable1 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) {
        fail("Transaction not committed");
      }
      sql = "drop table newTable1";
      sm.execute(sql);
      sm.close();
      conn.close();
    } catch (Exception e) {
      fail("Exception occurred in test Commit due to ", e);
    }
  }

  private void timedOutTransactionIsNotCommitted() {
    try {
      boolean exceptionOccurred = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds =
          (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
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
      if (!rs.next()) {
        fail("Database not updated");
      }
      sm.close();
      conn.close();
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      utx.setTransactionTimeout(1);
      waitUntilTransactionTimeout(utx);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccurred = true;
      }
      if (!exceptionOccurred) {
        fail("exception did not occur on commit although transaction timed out");
      }
      conn = ds.getConnection();
      sm = conn.createStatement();
      sql = "select * from newTable2 where id = 1";
      boolean gotExpectedTableNotExistException = false;
      try {
        sm.executeQuery(sql);
      } catch (SQLSyntaxErrorException expected) {
        gotExpectedTableNotExistException = true;
      }
      if (!gotExpectedTableNotExistException) {
        fail("Transaction should be timed out and not committed");
      }
      sm.close();
      conn.close();
    } catch (Exception e) {
      fail("Exception occurred in test Commit due to ", e);
    }
  }

  private void transactionUpdatingExternalDataSourceIsTimedOut() {
    try {
      boolean exceptionOccurred = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds =
          (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
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
      if (!rs.next()) {
        fail("Transaction not committed");
      }
      sql = "drop table newTable3";
      sm.execute(sql);
      sm.close();
      conn.close();
      assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
      utx.setTransactionTimeout(1);
      waitUntilTransactionTimeout(utx);
      try {
        utx.rollback();
      } catch (Exception e) {
        exceptionOccurred = true;
      }
      if (!exceptionOccurred) {
        fail("exception did not occur on rollback although transaction timed out");
      }
    } catch (Exception e) {
      fail("Exception occurred in test Commit due to ", e);
    }
  }

  private static String readFile(String filename) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String nextLine = "";
    StringBuilder sb = new StringBuilder();
    while ((nextLine = br.readLine()) != null) {
      sb.append(nextLine);
      //
      // note:
      // BufferedReader strips the EOL character.
      //
      // sb.append(lineSep);
    }
    LogWriterUtils.getLogWriter().fine("***********\n " + sb);
    return sb.toString();
  }
}
