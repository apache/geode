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

import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;

import org.apache.geode.LogWriter;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for (new multi-threaded) {@link MergeLogFiles} utility.
 */
@Category(LoggingTest.class)
public class MergeLogFilesIntegrationTest {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

  private final Object lock = new Object();

  /** The next integer to be written to the log */
  private int next;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  /**
   * A bunch of threads write a strictly increasing integer to log "files" stored in byte arrays.
   * The files are merged and the order is verified.
   */
  @Test
  public void testMultipleThreads() throws Exception {
    // Spawn a bunch of threads that write to a log
    WorkerGroup group = new WorkerGroup("Workers");
    Collection<Worker> workers = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Worker worker = new Worker("Worker " + i, group);
      workers.add(worker);
      worker.start();
    }

    for (Worker worker : workers) {
      ThreadUtils.join(worker, TIMEOUT_MILLIS);
    }

    // Merge the log files together
    Map<String, InputStream> logs = new HashMap<>();
    for (Worker worker : workers) {
      logs.put(worker.getName(), worker.getInputStream());
    }
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, true);
    MergeLogFiles.mergeLogFiles(logs, pw);

    // Verify that the entries are sorted
    BufferedReader br = new BufferedReader(new StringReader(sw.toString()));
    Pattern pattern = Pattern.compile("^Worker \\d+: .* VALUE: (\\d+)");
    int lastValue = -1;
    while (br.ready()) {
      String line = br.readLine();
      if (line == null) {
        break;
      }

      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        int value = Integer.parseInt(matcher.group(1));
        assertThat(value).isGreaterThan(lastValue);
        lastValue = value;
      }
    }

    assertThat(lastValue).isEqualTo(999);
  }

  /**
   * A {@code ThreadGroup} for workers
   */
  private class WorkerGroup extends ThreadGroup {

    /**
     * Creates a new {@code WorkerGroup} with the given name
     */
    private WorkerGroup(String name) {
      super(name);
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      errorCollector.addError(e);
    }
  }

  /**
   * Writes a strictly increasing number to a log "file" stored in a byte array. Waits a random
   * amount of time between writing entries.
   */
  private class Worker extends Thread {

    /** The input stream for reading from the log file */
    private InputStream in;

    /** A random number generator */
    private final Random random;

    /**
     * Creates a new {@code Worker} with the given name
     */
    private Worker(String name, ThreadGroup group) {
      super(group, name);
      random = new Random();
    }

    @Override
    public void run() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      LogWriter logWriter = new LocalLogWriter(ALL.intLevel(), new PrintStream(baos, true));
      for (int i = 0; i < 100; i++) {
        synchronized (lock) {
          int value = next++;

          // Have to log with the lock to guarantee ordering
          logWriter.info("VALUE: " + value);

          try {
            // Make sure that no two entries are at the same
            // millisecond. Since Windows doesn't have millisecond
            // granularity, we have to wait at least ten. Sigh.
            // DJM - even at 10, some entries have the same timestamp
            // on some PCs.
            Thread.sleep(15);

          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        try {
          Thread.sleep(random.nextInt(250));

        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      in = new ByteArrayInputStream(baos.toByteArray());
    }

    /**
     * Returns an {@code InputStream} for reading from the log that this worker wrote.
     */
    private InputStream getInputStream() {
      return in;
    }
  }
}
