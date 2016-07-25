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
package com.gemstone.gemfire.internal.logging;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Date;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests the functionality of the {@link SortLogFile} program.
 *
 * @since GemFire 3.0
 */
@Category(UnitTest.class)
public class SortLogFileJUnitTest {

  /**
   * Generates a "log file" whose entry timestamps are in a random
   * order.  Then it sorts the log file and asserts that the entries
   * are sorted order.
   */
  @Test
  public void testRandomLog() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos), true);
    LogWriter logger = new RandomLogWriter(pw);

    for (int i = 0; i < 100; i++) {
      logger.info(String.valueOf(i));
    }

    pw.flush();
    pw.close();
    
    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    StringWriter sw = new StringWriter();
    pw = new PrintWriter(sw, true);
    SortLogFile.sortLogFile(bais, pw);

    String sorted = sw.toString();

    BufferedReader br = new BufferedReader(new StringReader(sorted));
    LogFileParser parser = new LogFileParser(null, br);
    String prevTimestamp = null;
    while (parser.hasMoreEntries()) {
      LogFileParser.LogEntry entry = parser.getNextEntry();
      String timestamp = entry.getTimestamp();
      if (prevTimestamp != null) {
        assertTrue("Prev: " + prevTimestamp + ", current: " + timestamp,
                   prevTimestamp.compareTo(timestamp) <= 0);
      }
      prevTimestamp = entry.getTimestamp();
    }
  }

  /**
   * A <code>LogWriter</code> that generates random time stamps.
   */
  private static class RandomLogWriter extends LocalLogWriter {

    /** Used to generate a random date */
    private Random random = new Random();

    /**
     * Creates a new <code>RandomLogWriter</code> that logs to the
     * given <code>PrintWriter</code>.
     */
    public RandomLogWriter(PrintWriter pw) {
      super(ALL_LEVEL, pw);
    }

    /**
     * Ignores <code>date</code> and returns the timestamp for a
     * random date.
     */
    @Override
    protected String formatDate(Date date) {
      long time = date.getTime() + (random.nextInt(100000) * 1000);
      date = new Date(time);
      return super.formatDate(date);
    }
  }
}
