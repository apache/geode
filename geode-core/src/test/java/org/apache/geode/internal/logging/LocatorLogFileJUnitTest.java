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
package org.apache.geode.internal.logging;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Creates Locator and tests logging behavior at a high level.
 */
@Category(IntegrationTest.class)
public class LocatorLogFileJUnitTest {

  protected static final int TIMEOUT_MILLISECONDS = 180 * 1000; // 2 minutes
  protected static final int INTERVAL_MILLISECONDS = 100; // 100 milliseconds

  private Locator locator;
  private FileInputStream fis;

  @Rule
  public TestName name = new TestName();

  @After
  public void tearDown() throws Exception {
    if (this.locator != null) {
      this.locator.stop();
      this.locator = null;
    }
    if (fis != null) {
      fis.close();
    }
  }

  @Test
  public void testLocatorCreatesLogFile() throws Exception {
    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = "localhost[" + port + "]";

    final Properties properties = new Properties();
    properties.put(LOG_LEVEL, "config");
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, locators);
    properties.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.put(DISABLE_AUTO_RECONNECT, "true");
    properties.put(MEMBER_TIMEOUT, "2000");
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");

    final File logFile = new File(name.getMethodName() + "-locator-" + port + ".log");
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());

    this.locator = Locator.startLocatorAndDS(port, logFile, properties);

    InternalDistributedSystem ds = (InternalDistributedSystem) this.locator.getDistributedSystem();
    assertNotNull(ds);
    DistributionConfig config = ds.getConfig();
    assertNotNull(config);
    assertEquals(
        "Expected " + LogWriterImpl.levelToString(InternalLogWriter.CONFIG_LEVEL) + " but was "
            + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.CONFIG_LEVEL, config.getLogLevel());

    // CONFIG has been replaced with INFO -- all CONFIG statements are now logged at INFO as well
    InternalLogWriter logWriter = (InternalLogWriter) ds.getLogWriter();
    assertNotNull(logWriter);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals(
        "Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was "
            + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.INFO_LEVEL, logWriter.getLogWriterLevel());

    assertNotNull(this.locator);
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }

      @Override
      public String description() {
        return "waiting for log file to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());
    // assert not empty
    this.fis = new FileInputStream(logFile);
    assertTrue(fis.available() > 0);
    this.locator.stop();
    this.locator = null;
    this.fis.close();
    this.fis = null;
  }
}
