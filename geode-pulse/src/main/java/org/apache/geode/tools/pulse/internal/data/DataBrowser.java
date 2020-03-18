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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class DataBrowser This class contains Data browser functionalities for managing queries and
 * histories.
 *
 * @since GemFire version 7.5.Beta 2013-03-25
 */
public class DataBrowser {

  private static final Logger logger = LogManager.getLogger();
  private final ResourceBundle resourceBundle;
  private final Repository repository;

  private SimpleDateFormat simpleDateFormat =
      new SimpleDateFormat(PulseConstants.PULSE_QUERY_HISTORY_DATE_PATTERN);

  private final ObjectMapper mapper = new ObjectMapper();

  public DataBrowser(ResourceBundle resourceBundle, Repository repository) {
    this.resourceBundle = resourceBundle;
    this.repository = repository;
  }

  /**
   * addQueryInHistory method adds user's query into query history file
   *
   * @param userId Logged in User's Id
   * @param queryText Query text to execute
   */
  public boolean addQueryInHistory(String queryText, String userId) {
    boolean operationStatus = false;
    if (StringUtils.isNotBlank(queryText) && StringUtils.isNotBlank(userId)) {

      // Fetch all queries from query log file
      ObjectNode queries = fetchAllQueriesFromFile();

      // Get user's query history list
      ObjectNode userQueries = (ObjectNode) queries.get(userId);
      if (userQueries == null) {
        userQueries = mapper.createObjectNode();
      }

      // Add query in user's query history list
      userQueries.put(Long.toString(System.currentTimeMillis()), queryText);
      queries.set(userId, userQueries);

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
   */
  public boolean deleteQueryById(String userId, String queryId) {

    boolean operationStatus = false;
    if (StringUtils.isNotBlank(queryId) && StringUtils.isNotBlank(userId)) {

      // Fetch all queries from query log file
      ObjectNode queries = fetchAllQueriesFromFile();

      // Get user's query history list
      ObjectNode userQueries = (ObjectNode) queries.get(userId);

      if (userQueries != null) {
        // Remove user's query
        userQueries.remove(queryId);
        queries.set(userId, userQueries);

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

    if (StringUtils.isNotBlank(userId)) {

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
          new FileInputStream(repository.getPulseConfig().getQueryHistoryFileName());
      String inputStreamString = new Scanner(inputStream, "UTF-8").useDelimiter("\\A").next();
      queriesJSON = mapper.readTree(inputStreamString);
    } catch (FileNotFoundException e) {
      logger.debug(resourceBundle.getString("LOG_MSG_DATA_BROWSER_QUERY_HISTORY_FILE_NOT_FOUND"),
          e);
    } catch (Exception e) {
      logger.info(e);
    } finally {
      // Close input stream
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          logger.info(e);
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

    File file = new File(repository.getPulseConfig().getQueryHistoryFileName());
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

      logger.debug(resourceBundle.getString("LOG_MSG_DATA_BROWSER_QUERY_HISTORY_FILE_NOT_FOUND"),
          e.getMessage());
    } catch (IOException e) {
      logger.info(e);
    } finally {
      if (fileOut != null) {
        try {
          fileOut.close();
        } catch (IOException e) {
          logger.info(e);
        }
      }
    }
    return operationStatus;
  }

}
