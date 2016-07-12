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
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessStreamReader.InputListener;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.internal.util.StopWatch;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

import java.io.*;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;

/**
 * @since GemFire 8.0
 */
public abstract class AbstractLauncherIntegrationTestCase {
  protected static final Logger logger = LogService.getLogger();
  
  protected static final int WAIT_FOR_PROCESS_TO_DIE_TIMEOUT = 5 * 60 * 1000; // 5 minutes
  protected static final int TIMEOUT_MILLISECONDS = WAIT_FOR_PROCESS_TO_DIE_TIMEOUT;
  protected static final int WAIT_FOR_FILE_CREATION_TIMEOUT = 10*1000; // 10s
  protected static final int WAIT_FOR_FILE_DELETION_TIMEOUT = 10*1000; // 10s
  protected static final int WAIT_FOR_MBEAN_TIMEOUT = 10*1000; // 10s
  protected static final int INTERVAL_MILLISECONDS = 100;
  
  private static final String EXPECTED_EXCEPTION_ADD = "<ExpectedException action=add>{}</ExpectedException>";
  private static final String EXPECTED_EXCEPTION_REMOVE = "<ExpectedException action=remove>{}</ExpectedException>";
  private static final String EXPECTED_EXCEPTION_MBEAN_NOT_REGISTERED = "MBean Not Registered In GemFire Domain";
  
  protected volatile ServerSocket socket;

  protected volatile File pidFile;
  protected volatile File stopRequestFile;
  protected volatile File statusRequestFile;
  protected volatile File statusFile;
  
  @Rule
  public TestName testName= new TestName();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Before
  public final void setUpAbstractLauncherIntegrationTestCase() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, Integer.toString(0));
    logger.info(EXPECTED_EXCEPTION_ADD, EXPECTED_EXCEPTION_MBEAN_NOT_REGISTERED);
  }

  @After
  public final void tearDownAbstractLauncherIntegrationTestCase() throws Exception {
    logger.info(EXPECTED_EXCEPTION_REMOVE, EXPECTED_EXCEPTION_MBEAN_NOT_REGISTERED);
    if (this.socket != null) {
      this.socket.close();
      this.socket = null;
    }
    delete(this.pidFile); this.pidFile = null;
    delete(this.stopRequestFile); this.stopRequestFile = null;
    delete(this.statusRequestFile); this.statusRequestFile = null;
    delete(this.statusFile); this.statusFile = null;
  }
  
  protected void delete(final File file) throws Exception {
    assertEventuallyTrue("deleting " + file, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (file == null) {
          return true;
        }
        try {
          FileUtil.delete(file);
        } catch (IOException e) {
        }
        return !file.exists();
      }
    }, WAIT_FOR_FILE_DELETION_TIMEOUT, INTERVAL_MILLISECONDS);
  }
  
  protected void waitForPidToStop(final int pid, boolean throwOnTimeout) throws Exception {
    assertEventuallyFalse("Process never died", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return ProcessUtils.isProcessAlive(pid);
      }
    }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL_MILLISECONDS);
  }
  
  protected void waitForPidToStop(final int pid) throws Exception {
    waitForPidToStop(pid, true);
  }
  
  protected void waitForFileToDelete(final File file, boolean throwOnTimeout) throws Exception {
    if (file == null) {
      return;
    }
    assertEventuallyTrue("waiting for file " + file + " to delete", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !file.exists();
      }
    }, WAIT_FOR_FILE_DELETION_TIMEOUT, INTERVAL_MILLISECONDS);
  }
  
  protected void waitForFileToDelete(final File file) throws Exception {
    waitForFileToDelete(file, true);
  }
  
  protected static int getPid() throws PidUnavailableException {
    return ProcessUtils.identifyPid();
  }

  protected InputListener createLoggingListener(final String name, final String header) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        logger.info(new StringBuilder("[").append(header).append("]").append(line).toString());
      }
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected InputListener createCollectionListener(final String name, final String header, final List<String> lines) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        lines.add(line);
      }
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected InputListener createExpectedListener(final String name, final String header, final String expected, final AtomicBoolean atomic) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        if (line.contains(expected)) {
          atomic.set(true);
        }
      }
      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected void writeGemfireProperties(final Properties gemfireProperties, final File gemfirePropertiesFile) throws IOException {
    if (!gemfirePropertiesFile.exists()) {
      gemfireProperties.store(new FileWriter(gemfirePropertiesFile), "Configuration settings for the GemFire Server");
    }
  }

  protected int readPid(final File pidFile) throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(pidFile));
      return Integer.parseInt(StringUtils.trim(reader.readLine()));
    }
    finally {
      IOUtils.close(reader);
    }
  }

  protected void writePid(final File pidFile, final int pid) throws IOException {
    FileWriter writer = new FileWriter(pidFile);
    writer.write(String.valueOf(pid));
    writer.write("\n");
    writer.flush();
    writer.close();
  }

  protected void waitForFileToExist(final File file, boolean throwOnTimeout) throws Exception {
    assertEventuallyTrue("waiting for file " + file + " to exist", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return file.exists();
      }
    }, WAIT_FOR_FILE_CREATION_TIMEOUT, INTERVAL_MILLISECONDS);
  }
  
  protected void waitForFileToExist(final File file) throws Exception {
    waitForFileToExist(file, true);
  }
  
  protected String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }
  
  protected static void assertEventuallyTrue(final String message, final Callable<Boolean> callable, final int timeout, final int interval) throws Exception {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done = (callable.call())) {
      Thread.sleep(interval);
    }
    assertTrue(message, done);
  }
  
  protected static void assertEventuallyFalse(final String message, final Callable<Boolean> callable, final int timeout, final int interval) throws Exception {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done = (!callable.call())) {
      Thread.sleep(interval);
    }
    assertTrue(message, done);
  }
  
  protected static void disconnectFromDS() {
    InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }
}
