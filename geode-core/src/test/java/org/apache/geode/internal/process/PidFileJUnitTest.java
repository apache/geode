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
package org.apache.geode.internal.process;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ExpectedTimeoutRule;

/**
 * Unit tests the PidFile class.
 * 
 * @since GemFire 8.2
 */
@Category(IntegrationTest.class)
public class PidFileJUnitTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  
  @Rule
  public ExpectedTimeoutRule timeout = ExpectedTimeoutRule.none();
  
  protected Mockery mockContext;
  private ExecutorService futures;

  @Before
  public void before() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
    this.futures = Executors.newFixedThreadPool(2);
  }

  @After
  public void after() {
    mockContext.assertIsSatisfied();
    assertTrue(this.futures.shutdownNow().isEmpty());
  }
  
  @Test
  public void readsIntFromFile() throws Exception {
    final File file = testFolder.newFile("my.pid");
    final String value = "42";
    writeToFile(file, value);
    
    final int readValue = new PidFile(file).readPid();
    assertEquals(Integer.parseInt(value), readValue);
  }
  
  @Test
  public void readingEmptyFileThrowsIllegalArgumentException() throws Exception {
    final File file = testFolder.newFile("my.pid");
    
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid pid 'null' found");
    
    new PidFile(file).readPid();
  }
  
  @Test
  public void readingFileWithNonIntegerThrowsIllegalArgumentException() throws Exception {
    final File file = testFolder.newFile("my.pid");
    final String value = "fortytwo";
    writeToFile(file, value);
    
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid pid '" + value + "' found");
    
    new PidFile(file).readPid();
  }
  
  @Test
  public void readingFileWithNegativeIntegerThrowsIllegalArgumentException() throws Exception {
    final File file = testFolder.newFile("my.pid");
    final String value = "-42";
    writeToFile(file, value);
    
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid pid '" + value + "' found");
    
    new PidFile(file).readPid();
  }
  
  @Test
  public void readingNullFileThrowsNullPointerException() throws Exception {
    final File file = null;
    
    thrown.expect(NullPointerException.class);
    
    new PidFile(file).readPid();
  }
  
  @Test
  public void timesOutReadingFromEmptyFile() throws Exception {
    final File file = testFolder.newFile("my.pid");
    
    timeout.expect(TimeoutException.class);
    timeout.expectMessage("Invalid pid 'null' found");
    timeout.expectMinimumDuration(1000);
    timeout.expectMaximumDuration(10000);
    timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
    
    new PidFile(file).readPid(1500, TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void readsIntBeforeTimeout() throws Exception {
    final int AWAIT_LATCH_TIMEOUT_MILLIS = 10*1000;
    final int OPEN_LATCH_DELAY_MILLIS = 2*1000;
    final int FUTURE_GET_TIMEOUT_MILLIS = 2*1000;
    final int READ_PID_TIMEOUT_MILLIS = 2*OPEN_LATCH_DELAY_MILLIS;

    final File file = testFolder.newFile("my.pid");
    final FileWriter writer = new FileWriter(file);

    final CountDownLatch writePidLatch = new CountDownLatch(1);
    final String value = "42";
    
    // start Future to write the pid later but before timeout
    Future<Boolean> futureWritePid = this.futures.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        writePidLatch.await(AWAIT_LATCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        writeToFile(file, value);
        return true;
      }
    });
    
    // start Future to sleep and release the delay
    Future<Boolean> futureOpenLatch = this.futures.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Thread.sleep(OPEN_LATCH_DELAY_MILLIS);
        writePidLatch.countDown();
        return true;
      }
    });
    
    StopWatch stopWatch = new StopWatch(true);
    final int readValue = new PidFile(file).readPid(READ_PID_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    assertEquals(Integer.parseInt(value), readValue);
    
    long duration = stopWatch.elapsedTimeMillis();
    assertTrue(duration > OPEN_LATCH_DELAY_MILLIS);
    assertTrue(duration < READ_PID_TIMEOUT_MILLIS);
    
    assertEquals(0, writePidLatch.getCount());
    assertTrue(futureOpenLatch.get(FUTURE_GET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    assertTrue(futureWritePid.get(FUTURE_GET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void findsCorrectFile() throws Exception {
    final File directory = testFolder.getRoot();

    final String fileNames[] = new String[] { "other.txt", "my.txt", "a.log", "b.log" };
    for (String fileName : fileNames) {
      testFolder.newFile(fileName);
    }

    final int pidValue = 42;
    final File file = testFolder.newFile("my.pid");
    writeToFile(file, String.valueOf(pidValue));
    
    final File other = testFolder.newFile("other.pid");
    writeToFile(other, "43");
    
    final File[] files = directory.listFiles();
    assertEquals(fileNames.length + 2, files.length);
    
    PidFile pidFile = new PidFile(directory, file.getName());
    assertEquals(file, pidFile.getFile());
    
    int value = pidFile.readPid();
    assertEquals(pidValue, value);
  }
  
  @Test
  public void missingFileInEmptyDirectoryThrowsFileNotFoundException() throws Exception {
    final File directory = testFolder.getRoot();
    
    final String pidFileName = "my.pid";
    
    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage("Unable to find PID file '" + pidFileName + "' in directory " + directory);
    
    new PidFile(directory, pidFileName);
  }
  
  @Test
  public void missingFileThrowsFileNotFoundException() throws Exception {
    final String pidFileName = "my.pid";

    final File directory = testFolder.getRoot();
    final File file = new File(directory, pidFileName);
    
    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage("Unable to find PID file '" + file + "'");
    
    new PidFile(file);
  }
  
  @Test
  public void missingFileInFullDirectoryThrowsFileNotFoundException() throws Exception {
    final File directory = testFolder.getRoot();

    final String fileNames[] = new String[] { "other.txt", "my.txt", "a.log", "b.log" };
    for (String fileName : fileNames) {
      testFolder.newFile(fileName);
    }

    final File other = testFolder.newFile("other.pid");
    writeToFile(other, "43");
    
    final File[] files = directory.listFiles();
    assertEquals(fileNames.length + 1, files.length);

    final String pidFileName = "my.pid";
    
    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage("Unable to find PID file '" + pidFileName + "' in directory " + directory);
    
    new PidFile(directory, pidFileName);
  }
  
  private void writeToFile(final File file, String value) throws IOException {
    final FileWriter writer = new FileWriter(file);
    writer.write(value);
    writer.flush();
    writer.close();
  }
}
