/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class DataBrowserResultLoader {
  /* Constants for executing Data Browser queries */
  public static final String QUERY_TYPE_ONE = "query1";
  public static final String QUERY_TYPE_TWO = "query2";
  public static final String QUERY_TYPE_THREE = "query3";
  public static final String QUERY_TYPE_FOUR = "query4";
  public static final String QUERY_TYPE_FIVE = "query5";
  public static final String QUERY_TYPE_SIX = "query6";
  public static final String QUERY_TYPE_SEVEN = "query7";
  public static final String QUERY_TYPE_EIGHT = "query8";

  private static final DataBrowserResultLoader dbResultLoader = new DataBrowserResultLoader();

  public static DataBrowserResultLoader getInstance() {
    return dbResultLoader;
  }

  public String load(String queryString) throws IOException {

    String fileName;
    String fileContent = "";

    try {

      switch (queryString) {
        case QUERY_TYPE_ONE:
          fileName = "testQueryResultClusterSmall.txt";
          break;
        case QUERY_TYPE_TWO:
          fileName = "testQueryResultSmall.txt";
          break;
        case QUERY_TYPE_THREE:
          fileName = "testQueryResult.txt";
          break;
        case QUERY_TYPE_FOUR:
          fileName = "testQueryResultWithStructSmall.txt";
          break;
        case QUERY_TYPE_FIVE:
          fileName = "testQueryResultClusterWithStruct.txt";
          break;
        case QUERY_TYPE_SIX:
          fileName = "testQueryResultHashMapSmall.txt";
          break;
        case QUERY_TYPE_SEVEN:
          fileName = "testQueryResult1000.txt";
          break;
        case QUERY_TYPE_EIGHT:
          fileName = "testQueryResultClusterSmallJSInject.txt";
          break;
        default:
          fileName = "testQueryResult.txt";
          break;
      }

      InputStream inputStream = getClass().getResourceAsStream("/" + fileName);
      assert inputStream != null;
      BufferedReader streamReader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      fileContent = streamReader.lines().collect(Collectors.joining(System.lineSeparator()));

      // close stream reader
      streamReader.close();

    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return fileContent;
  }
}
