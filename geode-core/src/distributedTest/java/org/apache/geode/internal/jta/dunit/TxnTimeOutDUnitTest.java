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
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
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
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.transaction.NotSupportedException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.jta.CacheUtils;
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
 * This test sees if the TransactionTimeOut works properly
 */

public class TxnTimeOutDUnitTest extends JUnit4DistributedTestCase {

  static DistributedSystem ds;
  static Cache cache = null;

  public static void init() throws Exception {
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
    props.setProperty(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    ds = (new TxnTimeOutDUnitTest()).getSystem(props);
    if (cache == null || cache.isClosed()) {
      cache = CacheFactory.create(ds);
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
    } catch (Exception e) {
      fail("Exception in starting cache due to ", e);
    }
  }

  public static void closeCache() {
    try {
      if (cache != null && !cache.isClosed()) {
        cache.close();
      }
      if (ds != null && ds.isConnected()) {
        ds.disconnect();
      }

    } catch (Exception e) {
      fail("Exception in closing cache or ds due to " + e);
      e.printStackTrace();
    }
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> TxnTimeOutDUnitTest.init());
  }

  @Override
  public final void preTearDown() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(() -> TxnTimeOutDUnitTest.closeCache());
  }

  @Test
  public void testMultiThreaded() throws Exception {
    try {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);

      Object[] o = new Object[1];
      o[0] = new Integer(2);
      AsyncInvocation asyncObj1 = vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest3", o);

      Object[] o1 = new Object[1];
      o1[0] = new Integer(2);
      AsyncInvocation asyncObj2 = vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest3", o1);

      Object[] o2 = new Object[1];
      o2[0] = new Integer(3);
      AsyncInvocation asyncObj3 = vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest3", o2);

      Object[] o3 = new Object[1];
      o3[0] = new Integer(3);
      AsyncInvocation asyncObj4 = vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest3", o3);

      Object[] o4 = new Object[1];
      o4[0] = new Integer(1);
      AsyncInvocation asyncObj5 = vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest3", o4);

      ThreadUtils.join(asyncObj1, 5 * 60 * 1000);
      if (asyncObj1.exceptionOccurred()) {
        Assert.fail("asyncObj1 failed", asyncObj1.getException());
      }

      ThreadUtils.join(asyncObj2, 5 * 60 * 1000);
      if (asyncObj2.exceptionOccurred()) {
        Assert.fail("asyncObj2 failed", asyncObj2.getException());
      }

      ThreadUtils.join(asyncObj3, 5 * 60 * 1000);
      if (asyncObj3.exceptionOccurred()) {
        Assert.fail("asyncObj3 failed", asyncObj3.getException());
      }

      ThreadUtils.join(asyncObj4, 5 * 60 * 1000);
      if (asyncObj4.exceptionOccurred()) {
        Assert.fail("asyncObj4 failed", asyncObj4.getException());
      }

      ThreadUtils.join(asyncObj5, 5 * 60 * 1000);
      if (asyncObj5.exceptionOccurred()) {
        Assert.fail("asyncObj5 failed", asyncObj5.getException());
      }


    } catch (Exception e) {
      fail("exception occurred in testMultiThreaded due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testLoginTimeOut() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    AsyncInvocation asyncObj1 = vm0.invokeAsync(() -> TxnTimeOutDUnitTest.runTest2());
    AsyncInvocation asyncObj2 = vm0.invokeAsync(() -> TxnTimeOutDUnitTest.runTest1());

    ThreadUtils.join(asyncObj1, 5 * 60 * 1000);
    if (asyncObj1.exceptionOccurred()) {
      Assert.fail("asyncObj1 failed", asyncObj1.getException());
    }

    ThreadUtils.join(asyncObj2, 5 * 60 * 1000);
    if (asyncObj2.exceptionOccurred()) {
      Assert.fail("asyncObj2 failed", asyncObj2.getException());
    }

  }

  public static void runTest1() throws Exception {
    boolean exceptionOccurred = false;
    try {
      getSystemStatic().getLogWriter().fine("<ExpectedException action=add> +"
          + "DistributedSystemDisconnectedException" + "</ExpectedException>");
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
      LogWriterUtils.getLogWriter().fine("Exception caught " + e);
      fail("failed in naming lookup: " + e);
    } finally {
      getSystemStatic().getLogWriter().fine("<ExpectedException action=remove> +"
          + "DistributedSystemDisconnectedException" + "</ExpectedException>");
    }
  }

  private static void waitUntilTransactionTimeout(UserTransaction utx) {
    GeodeAwaitility.await().pollInSameThread()
        .until(() -> utx.getStatus() == Status.STATUS_NO_TRANSACTION);
  }

  public static void runTest2() throws Exception {
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
      LogWriterUtils.getLogWriter().fine("Exception caught " + e);
      fail("failed in naming lookup: " + e);
    }
  }

  public static void runTest3(Object o)
      throws SystemException, NotSupportedException, NamingException, InterruptedException {
    boolean exceptionOccurred = false;
    int sleeptime = ((Integer) o).intValue();
    Context ctx = cache.getJNDIContext();
    UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
    utx.begin();
    assertThat(utx.getStatus() == Status.STATUS_ACTIVE);
    utx.setTransactionTimeout(sleeptime);
    waitUntilTransactionTimeout(utx);
    try {
      utx.commit();
    } catch (Exception e) {
      exceptionOccurred = true;
    }
    if (!exceptionOccurred) {
      fail("exception did not occur although was supposed to occur");
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
      // BufferedReader strips the EOL character.
      //
      // sb.append(lineSep);
    }
    LogWriterUtils.getLogWriter().fine("***********\n " + sb);
    return sb.toString();
  }
}
