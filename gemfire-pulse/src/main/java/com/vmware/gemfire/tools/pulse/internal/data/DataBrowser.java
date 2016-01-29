/*
 *
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
 *
 */

package com.vmware.gemfire.tools.pulse.internal.data;

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

import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class DataBrowser This class contains Data browser functionalities for
 * managing queries and histories.
 * 
 * @author Sachin K
 * @since version 7.5.Beta 2013-03-25
 */
public class DataBrowser {

  private final PulseLogWriter LOGGER = PulseLogWriter.getLogger();
  private final ResourceBundle resourceBundle = Repository.get()
      .getResourceBundle();

  private final String queryHistoryFile = PulseConstants.PULSE_QUERY_HISTORY_FILE_LOCATION
      + System.getProperty("file.separator")
      + PulseConstants.PULSE_QUERY_HISTORY_FILE_NAME;

  private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
      PulseConstants.PULSE_QUERY_HISTORY_DATE_PATTERN);

  /**
   * addQueryInHistory method adds user's query into query history file
   * 
   * @param userId
   *          Logged in User's Id
   * @param queryText
   *          Query text to execute
   */
  public boolean addQueryInHistory(String queryText, String userId) {

    boolean operationStatus = false;
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(queryText)
        && StringUtils.isNotNullNotEmptyNotWhiteSpace(userId)) {

      // Fetch all queries from query log file
      JSONObject queries = fetchAllQueriesFromFile();

      // Get user's query history list
      JSONObject userQueries = null;
      try {
        userQueries = queries.getJSONObject(userId);
      } catch (JSONException e) {
        userQueries = new JSONObject();
      }

      // Add query in user's query history list
      try {
        userQueries.put(Long.toString(System.currentTimeMillis()), queryText);
        queries.put(userId, userQueries);
      } catch (JSONException e) {
        if (LOGGER.fineEnabled()) {
          LOGGER.fine("JSONException Occured while adding user's query : " + e.getMessage());
        }
      }

      // Store queries in file back
      operationStatus = storeQueriesInFile(queries);

    }

    return operationStatus;
  }

  /**
   * deleteQueryById method deletes query from query history file
   * 
   * @param userId
   *          Logged in user's Unique Id
   * @param queryId
   *          Unique Id of Query to be deleted
   * @return boolean
   */
  public boolean deleteQueryById(String userId, String queryId) {

    boolean operationStatus = false;
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(queryId)
        && StringUtils.isNotNullNotEmptyNotWhiteSpace(userId)) {

      // Fetch all queries from query log file
      JSONObject queries = fetchAllQueriesFromFile();

      // Get user's query history list
      JSONObject userQueries = null;
      try {
        userQueries = queries.getJSONObject(userId);
      } catch (JSONException e) {
        userQueries = new JSONObject();
      }

      // Remove user's query
      try {
        userQueries.remove(queryId);
        queries.put(userId, userQueries);
      } catch (JSONException e) {
        if (LOGGER.fineEnabled()) {
          LOGGER.fine("JSONException Occured while deleting user's query : " + e.getMessage());
        }
      }

      // Store queries in file back
      operationStatus = storeQueriesInFile(queries);

    }
    
    return operationStatus;
  }

  /**
   * getQueryHistoryByUserId method reads and lists out the queries from history
   * file
   * 
   * @param userId
   *          Logged in User's Id
   */
  public JSONArray getQueryHistoryByUserId(String userId) {

    JSONArray queryList = new JSONArray();

    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(userId)) {

      // Fetch all queries from query log file
      JSONObject queries = fetchAllQueriesFromFile();
      
      // Get user's query history list
      JSONObject userQueries = null;
      try {
        userQueries = queries.getJSONObject(userId);
      } catch (JSONException e) {
        userQueries = new JSONObject();
      }

      try {
        Iterator<?> it = userQueries.keys();
        while (it.hasNext()) {
          String key = (String) it.next();
          JSONObject queryItem = new JSONObject();
          queryItem.put("queryId", key);
          queryItem.put("queryText", userQueries.get(key).toString());
          queryItem.put("queryDateTime",
              simpleDateFormat.format(Long.valueOf(key)));
          queryList.put(queryItem);
        }
      } catch (JSONException e) {
        if (LOGGER.fineEnabled()) {
          LOGGER.fine("JSONException Occured: " + e.getMessage());
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
  private JSONObject fetchAllQueriesFromFile() {
    InputStream inputStream = null;
    JSONObject queriesJSON = new JSONObject();

    try {
      inputStream = new FileInputStream(queryHistoryFile);
      String inputStreamString = new Scanner(inputStream, "UTF-8")
          .useDelimiter("\\A").next();
      queriesJSON = new JSONObject(inputStreamString);
    } catch (FileNotFoundException e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine(resourceBundle
            .getString("LOG_MSG_DATA_BROWSER_QUERY_HISTORY_FILE_NOT_FOUND")
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

    return queriesJSON;
  }

  /**
   * generateQueryKey method stores queries in query history file.
   * 
   * @return Boolean true is operation is successful, false otherwise
   */
  private boolean storeQueriesInFile(JSONObject queries) {
    boolean operationStatus = false;
    FileOutputStream fileOut = null;

    File file = new File(queryHistoryFile);
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
        LOGGER.fine(resourceBundle
            .getString("LOG_MSG_DATA_BROWSER_QUERY_HISTORY_FILE_NOT_FOUND")
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
