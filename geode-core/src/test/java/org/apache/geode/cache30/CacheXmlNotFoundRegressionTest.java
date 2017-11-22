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
package org.apache.geode.cache30;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CacheXmlNotFoundRegressionTest {

  /**
   * unit test for <a href="https://issues.apache.org/jira/browse/GEODE-3038">GEODE-3038</a> Tests
   * that an error about missing cache-xml file is indeed printed in the text log file. The test
   * {@link CacheXml66DUnitTest#testNonExistentFile()} is supposed to test the same, but is not
   * enough. It only checks for an CacheXmlException exception to be thrown. Also in that test a log
   * is printed into STDOUT, and we do see our error there, but that is not the case when we work
   * with the real text log, specified via "log-file" param.
   */
  @Test
  public void testCacheXmlNotFoundInRealLog() throws Exception {

    String CACHE_SERVER_LOG = "cacheXmlNotFoundUnitTest.log";
    Properties props = new Properties();
    props.put(ConfigurationProperties.MCAST_PORT, "0");
    props.put(ConfigurationProperties.LOG_FILE, CACHE_SERVER_LOG);
    props.put(ConfigurationProperties.CACHE_XML_FILE, "non-existing-cache-xml");

    CacheFactory factory = new CacheFactory(props);

    String errorMessage = "";

    try {
      factory.create();
      fail("Should have thrown a CacheXmlException");
    } catch (CacheXmlException e) {
      errorMessage = e.toString();
    }

    // looking for an error in the text log file
    Scanner scanner = new Scanner(new File(CACHE_SERVER_LOG));

    boolean found = false;
    while (scanner.hasNextLine() && !found) {
      found = scanner.nextLine().contains(errorMessage);
    }
    scanner.close();
    assertTrue("there should be a line about cache-xml-not-found in a log file", found);

    // deleting a log file
    File logFile = new File(CACHE_SERVER_LOG);
    logFile.delete();
  }
}
