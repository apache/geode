/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class ProductUseLogJUnitTest extends TestCase {

  public void testBasics() throws Exception {
    File logFile = new File("ProductUseLogTest_testBasics.log");
    if (logFile.exists()) {
      logFile.delete();
    }
    ProductUseLog log = new ProductUseLog(logFile);
    assertTrue(logFile.exists());
    log.log("test message");
    log.close();
    log.log("shouldn't be logged");
    log.reopen();
    log.log("test message");
    log.close();
    BufferedReader reader = new BufferedReader(new  FileReader(logFile));
    try {
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
    } finally {
      reader.close();
    }
  }

  public void testSizeLimit() throws Exception {
    long oldMax = ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE;
    ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE = 2000L;
    try {
      File logFile = new File("ProductUseLogTest_testSizeLimit.log");
      assertTrue(logFile.createNewFile());
      ProductUseLog log = new ProductUseLog(logFile);
      try {
        String logEntry = "log entry";
        for (long i=0; i<ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE; i++) {
          log.log(logEntry);
          assertTrue("expected " + logFile.getPath() + " to remain under "+ 
              ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE + " bytes in length",
              logFile.length() < ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE);
        }
      } finally {
        log.close();
      }
    } finally {
      ProductUseLog.MAX_PRODUCT_USE_FILE_SIZE = oldMax;
    }
  }
}
