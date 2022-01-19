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
package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.datasource.GemFireTransactionDataSource;

/**
 * TODO: this test has no assertions or validations of any sort
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransactionTimeOutJUnitTest {

  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");

    int pid = new Random().nextInt();

    File tmpFile = File.createTempFile("dunit-cachejta_", ".xml");
    tmpFile.deleteOnExit();

    String path = tmpFile.getAbsolutePath();
    String file_as_str =
        readFile(createTempFileFromResource(TransactionTimeOutJUnitTest.class, "/jta/cachejta.xml")
            .getAbsolutePath());
    String modified_file_str = file_as_str.replaceAll("newDB", "newDB_" + pid);

    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();

    props.setProperty(CACHE_XML_FILE, path);

    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    cache.getLogger().fine("SWAP:running test:" + testName.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    ds1.disconnect();
    // TODO: null out the statics!
  }

  @Test
  public void test1One() throws Exception {
    UserTransaction utx = new UserTransactionImpl();
    utx.begin();
    Thread.sleep(2000);
    utx.setTransactionTimeout(2);
    utx.setTransactionTimeout(200);
    utx.setTransactionTimeout(3);
    Thread.sleep(5000);
    // utx.commit();
  }

  @Test
  public void test2SetTransactionTimeOut() throws Exception {
    UserTransaction utx = new UserTransactionImpl();
    utx.begin();
    utx.setTransactionTimeout(2);
    System.out.println("Going to sleep");
    Thread.sleep(6000);
    utx.begin();
    utx.commit();
  }

  @Test
  public void test3ExceptionOnCommitAfterTimeOut() throws Exception {
    UserTransaction utx;
    utx = new UserTransactionImpl();
    utx.setTransactionTimeout(2);
    utx.begin();
    Thread.sleep(4000);
    try {
      utx.commit();
      fail("TimeOut did not rollback the transaction");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void test4MultipleSetTimeOuts() throws Exception {
    UserTransaction utx;
    utx = new UserTransactionImpl();
    utx.setTransactionTimeout(10);
    utx.begin();
    utx.setTransactionTimeout(8);
    utx.setTransactionTimeout(6);
    utx.setTransactionTimeout(2);
    Thread.sleep(6000);
    try {
      utx.commit();
      fail("TimeOut did not rollback the transaction");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void test5TimeOutBeforeBegin() throws Exception {
    UserTransaction utx = new UserTransactionImpl();
    utx.setTransactionTimeout(4);
    utx.begin();
    Thread.sleep(6000);
    try {
      utx.commit();
      fail("TimeOut did not rollback the transaction");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void test6CommitBeforeTimeOut() throws Exception {
    UserTransaction utx = new UserTransactionImpl();
    utx.begin();
    utx.setTransactionTimeout(6);
    Thread.sleep(2000);
    utx.commit();
  }

  @Test
  public void test7Commit() throws Exception {
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
    Thread.sleep(5000);
    utx.setTransactionTimeout(20);
    utx.setTransactionTimeout(10);
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
  }

  @Test
  public void test8CommitAfterTimeOut() throws Exception {
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
      fail("Transaction not committed");
    }
    sql = "drop table newTable2";
    sm.execute(sql);
    sm.close();
    conn.close();
    utx.setTransactionTimeout(1);
    Thread.sleep(3000);
    try {
      utx.commit();
      fail("exception did not occur on commit although transaction timed out");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void test9RollbackAfterTimeOut() throws Exception {
    Context ctx = cache.getJNDIContext();
    DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
    Connection conn2 = ds2.getConnection();
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
    conn2.close();
    utx.setTransactionTimeout(1);
    Thread.sleep(3000);
    try {
      utx.rollback();
      fail("exception did not occur on rollback although transaction timed out");
    } catch (Exception ignored) {
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
    return sb.toString();
  }

}
