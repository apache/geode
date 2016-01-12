/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

public class DataBrowserResultLoader {
  private static DataBrowserResultLoader dbResultLoader = new DataBrowserResultLoader();

  public static DataBrowserResultLoader getInstance() {
    return dbResultLoader;
  }

  public String load(String queryString) throws IOException {

    URL url = null;
    InputStream inputStream = null;
    BufferedReader streamReader = null;
    String inputStr = null;
    StringBuilder sampleQueryResultResponseStrBuilder = null;

    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

      if (queryString.equals(PulseTest.QUERY_TYPE_ONE)) {
        url = classLoader.getResource("testQueryResultClusterSmall.txt");
      } else if (queryString.equals(PulseTest.QUERY_TYPE_TWO)) {
        url = classLoader.getResource("testQueryResultSmall.txt");
      } else if (queryString.equals(PulseTest.QUERY_TYPE_THREE)) {
        url = classLoader.getResource("testQueryResult.txt");
      } else if (queryString.equals(PulseTest.QUERY_TYPE_FOUR)) {
        url = classLoader.getResource("testQueryResultWithStructSmall.txt");
      } else if (queryString.equals(PulseTest.QUERY_TYPE_FIVE)) {
        url = classLoader.getResource("testQueryResultClusterWithStruct.txt");
      } else if (queryString.equals(PulseTest.QUERY_TYPE_SIX)) {
        url = classLoader.getResource("testQueryResultHashMapSmall.txt");
      } else if (queryString.equals(PulseTest.QUERY_TYPE_SEVENE)) {
        url = classLoader.getResource("testQueryResult1000.txt");
      } else {
        url = classLoader.getResource("testQueryResult.txt");
      }

      File sampleQueryResultFile = new File(url.getPath());
      inputStream = new FileInputStream(sampleQueryResultFile);
      streamReader = new BufferedReader(new InputStreamReader(inputStream,
          "UTF-8"));
      sampleQueryResultResponseStrBuilder = new StringBuilder();

      while ((inputStr = streamReader.readLine()) != null) {
        sampleQueryResultResponseStrBuilder.append(inputStr);
      }

      // close stream reader
      streamReader.close();

    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return sampleQueryResultResponseStrBuilder.toString();
  }
}
