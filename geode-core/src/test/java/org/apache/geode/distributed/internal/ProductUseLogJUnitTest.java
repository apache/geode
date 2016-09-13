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
package com.gemstone.gemfire.distributed.internal;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class) // Fails on Windows -- see GEODE-373
public class ProductUseLogJUnitTest {

  private long oldMax;
  private File logFile;
  private ProductUseLog log;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    oldMax = ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE;
    logFile = temporaryFolder.newFile(getClass().getSimpleName() + "_" + testName.getMethodName() + ".log");
  }

  @After
  public void tearDown() throws Exception {
    ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE = oldMax;
    if (log != null) {
      log.close();
    }
  }

  @Test
  public void testBasics() throws Exception {
    assertTrue(logFile.delete());

    log = new ProductUseLog(logFile);

    assertTrue(logFile.exists());

    log.log("test message");
    log.close();
    log.log("shouldn't be logged");
    log.reopen();
    log.log("test message");
    log.close();

    BufferedReader reader = new BufferedReader(new  FileReader(logFile));

    String line = reader.readLine();
    assertTrue(line.length() == 0);
    line = reader.readLine();
    assertTrue("expected first line to contain 'test message'", line.contains("test message"));

    line = reader.readLine();
    assertTrue(line.length() == 0);
    line = reader.readLine();
    assertTrue("expected second line to contain 'test message'", line.contains("test message"));

    line = reader.readLine();
    assertTrue("expected only two non-empty lines in the file", line == null);
  }

  @Test
  public void testSizeLimit() throws Exception {
    ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE = 2000L;

    ProductUseLog log = new ProductUseLog(logFile);

    String logEntry = "log entry";
    for (long i=0; i<ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE; i++) {
      log.log(logEntry);
      assertTrue("expected " + logFile.getPath() + " to remain under "+
          ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE + " bytes in length",
          logFile.length() < ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE);
    }
  }
}
