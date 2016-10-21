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

package org.apache.geode.tools.pulse.internal.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;
import org.apache.geode.tools.pulse.internal.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.ResourceBundle;
import java.util.Scanner;

/**
 * Class DataBrowser This class contains Data browser functionalities for managing queries and
 * histories.
 * 
 * @since GemFire version 7.5.Beta 2013-03-25
 */
public class DataBrowser {

  private final PulseLogWriter LOGGER = PulseLogWriter.getLogger();
  private final ResourceBundle resourceBundle = Repository.get().getResourceBundle();

  private SimpleDateFormat simpleDateFormat =
      new SimpleDateFormat(PulseConstants.PULSE_QUERY_HISTORY_DATE_PATTERN);

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * addQueryInHistory method adds user's query into query history file
   * 
   * @param userId Logged in User's Id
   * @param queryText Query text to execute
   */
  public boolean addQueryInHistory(String queryText, String userId) {
    boolean operationStatus = false;
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(queryText)
        && StringUtils.isNotNullNotEmptyNotWhiteSpace(userId)) {

      // Fetch all queries from query log file
      ObjectNode queries = fetchAllQueriesFromFile();

      // Get user's query history list
      ObjectNode userQueries = (ObjectNode) queries.get(userId);
      if (userQueries == null) {
        userQueries = mapper.createObjectNode();
      }

      // Add query in user's query history list
      userQueries.put(Long.toString(System.currentTimeMillis()), queryText);
      queries.put(userId, userQueries);

      // Store queries in file back
      operationStatus = storeQueriesInFile(queries);
    }

    return operationStatus;
  }

  /**
   * deleteQueryById method deletes query from query history file
   * 
   * @param userId Logged in user's Unique Id
   * @param queryId Unique Id of Query to be deleted
   * @return boolean
   */
  public boolean deleteQueryById(String userId, String queryId) {

    boolean operationStatus = false;
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(queryId)
        && StringUtils.isNotNullNotEmptyNotWhiteSpace(userId)) {

      // Fetch all queries from query log file
      ObjectNode queries = fetchAllQueriesFromFile();

      // Get user's query history list
      ObjectNode userQueries = (ObjectNode) queries.get(userId);

      if (userQueries != null) {
        // Remove user's query
        userQueries.remove(queryId);
        queries.put(userId, userQueries);

        // Store queries in file back
        operationStatus = storeQueriesInFile(queries);
      }
    }

    return operationStatus;
  }

  /**
   * getQueryHistoryByUserId method reads and lists out the queries from history file
   * 
   * @param userId Logged in User's Id
   */
  public ArrayNode getQueryHistoryByUserId(String userId) {

    ArrayNode queryList = mapper.createArrayNode();

    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(userId)) {

      // Fetch all queries from query log file
      ObjectNode queries = fetchAllQueriesFromFile();

      // Get user's query history list
      ObjectNode userQueries = (ObjectNode) queries.get(userId);

      if (userQueries != null) {
        Iterator<String> it = userQueries.fieldNames();
        while (it.hasNext()) {
          String key = it.next();
          ObjectNode queryItem = mapper.createObjectNode();
          queryItem.put("queryId", key);
          queryItem.put("queryText", userQueries.get(key).toString());
          queryItem.put("queryDateTime", simpleDateFormat.format(Long.valueOf(key)));
          queryList.add(queryItem);
        }
      }
    }

    return queryList;
  }

  /**
   * generateQueryKey method fetches queries from query history file
   * 
   * @return Properties A collection queries in form of key and values
   */
  private ObjectNode fetchAllQueriesFromFile() {
    InputStream inputStream = null;
    JsonNode queriesJSON = mapper.createObjectNode();

    try {
      inputStream =
          new FileInputStream(Repository.get().getPulseConfig().getQueryHistoryFileName());
      String inputStreamString = new Scanner(inputStream, "UTF-8").useDelimiter("\\A").next();
      queriesJSON = mapper.readTree(inputStreamString);
    } catch (FileNotFoundException e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine(resourceBundle.getString("LOG_MSG_DATA_BROWSER_QUERY_HISTORY_FILE_NOT_FOUND")
            + " : " + e.getMessage());
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info(e.getMessage());
      }
    } finally {
      // Close input stream
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          if (LOGGER.infoEnabled()) {
            LOGGER.info(e.getMessage());
          }
        }
      }
    }

    return (ObjectNode) queriesJSON;
  }

  /**
   * generateQueryKey method stores queries in query history file.
   * 
   * @return Boolean true is operation is successful, false otherwise
   */
  private boolean storeQueriesInFile(ObjectNode queries) {
    boolean operationStatus = false;
    FileOutputStream fileOut = null;

    File file = new File(Repository.get().getPulseConfig().getQueryHistoryFileName());
    try {
      fileOut = new FileOutputStream(file);

      // if file does not exists, then create it
      if (!file.exists()) {
        file.createNewFile();
      }

      // get the content in bytes
      byte[] contentInBytes = queries.toString().getBytes();

      fileOut.write(contentInBytes);
      fileOut.flush();

      operationStatus = true;
    } catch (FileNotFoundException e) {

      if (LOGGER.fineEnabled()) {
        LOGGER.fine(resourceBundle.getString("LOG_MSG_DATA_BROWSER_QUERY_HISTORY_FILE_NOT_FOUND")
            + " : " + e.getMessage());
      }
    } catch (IOException e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info(e.getMessage());
      }
    } finally {
      if (fileOut != null) {
        try {
          fileOut.close();
        } catch (IOException e) {
          if (LOGGER.infoEnabled()) {
            LOGGER.info(e.getMessage());
          }
        }
      }
    }
    return operationStatus;
  }

}
