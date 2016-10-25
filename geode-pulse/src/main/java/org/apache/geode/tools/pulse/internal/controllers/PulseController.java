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

package org.apache.geode.tools.pulse.internal.controllers;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.PulseVersion;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;
import org.apache.geode.tools.pulse.internal.service.PulseService;
import org.apache.geode.tools.pulse.internal.service.PulseServiceFactory;
import org.apache.geode.tools.pulse.internal.service.SystemAlertsService;
import org.apache.geode.tools.pulse.internal.util.StringUtils;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * Class PulseController
 * 
 * This class contains the implementations for all http Ajax requests needs to
 * be served in Pulse.
 * 
 * @since GemFire version 7.5
 */
@Controller
public class PulseController {

  private static final PulseLogWriter LOGGER = PulseLogWriter.getLogger();

  // CONSTANTS
  private final String DEFAULT_EXPORT_FILENAME = "DataBrowserQueryResult.json";
  private final String QUERYSTRING_PARAM_ACTION = "action";
  private final String QUERYSTRING_PARAM_QUERYID = "queryId";
  private final String ACTION_VIEW = "view";
  private final String ACTION_DELETE = "delete";

  private String STATUS_REPSONSE_SUCCESS = "success";
  private String STATUS_REPSONSE_FAIL = "fail";

  private String ERROR_REPSONSE_QUERYNOTFOUND = "No queries found";
  private String ERROR_REPSONSE_QUERYIDMISSING = "Query id is missing";

  private static final String EMPTY_JSON = "{}";

  // Shared object to hold pulse version details
  public static PulseVersion pulseVersion = new PulseVersion();

  //default is gemfire
  private static String pulseProductSupport = PulseConstants.PRODUCT_NAME_GEMFIRE;

  private final ObjectMapper mapper = new ObjectMapper();

  @Autowired
  PulseServiceFactory pulseServiceFactory;

  @RequestMapping(value = "/pulseUpdate", method = RequestMethod.POST)
  public void getPulseUpdate(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    String pulseData = request.getParameter("pulseData");

    ObjectNode responseMap = mapper.createObjectNode();

    JsonNode requestMap = null;

    try {
      requestMap = mapper.readTree(pulseData);
      Iterator<?> keys = requestMap.fieldNames();

      // Execute Services
      while (keys.hasNext()) {
        String serviceName = keys.next().toString();
        try {
          PulseService pulseService = pulseServiceFactory
              .getPulseServiceInstance(serviceName);
          responseMap.put(serviceName, pulseService.execute(request));
        } catch (Exception serviceException) {
          LOGGER.warning("serviceException [for service "+serviceName+"] = " + serviceException.getMessage());
          responseMap.put(serviceName, EMPTY_JSON);
        }
      }
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occurred : " + e.getMessage());
      }
    }

    // Create Response
    response.getOutputStream().write(responseMap.toString().getBytes());
  }

  @RequestMapping(value = "/authenticateUser", method = RequestMethod.GET)
  public void authenticateUser(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      responseJSON.put("isUserLoggedIn", this.isUserLoggedIn(request));
      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occurred : " + e.getMessage());
      }
    }
  }

  /**
   * Method isUserLoggedIn Check whether user is logged in or not.
   * 
   * @param request
   * @return boolean
   */
  protected boolean isUserLoggedIn(HttpServletRequest request) {
    return null != request.getUserPrincipal();
  }

  @RequestMapping(value = "/pulseVersion", method = RequestMethod.GET)
  public void pulseVersion(HttpServletRequest request,
      HttpServletResponse response) throws IOException {

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      // Reference to repository
      Repository repository = Repository.get();
      // set pulse web app url
      String pulseWebAppUrl = request.getScheme() + "://"
          + request.getServerName() + ":" + request.getServerPort()
          + request.getContextPath();

      repository.setPulseWebAppUrl(pulseWebAppUrl);

      // Response
      responseJSON.put("pulseVersion", PulseController.pulseVersion.getPulseVersion());
      responseJSON.put("buildId", PulseController.pulseVersion.getPulseBuildId());
      responseJSON.put("buildDate", PulseController.pulseVersion.getPulseBuildDate());
      responseJSON.put("sourceDate", PulseController.pulseVersion.getPulseSourceDate());
      responseJSON.put("sourceRevision", PulseController.pulseVersion.getPulseSourceRevision());
      responseJSON.put("sourceRepository", PulseController.pulseVersion.getPulseSourceRepository());

    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/clearAlerts", method = RequestMethod.GET)
  public void clearAlerts(HttpServletRequest request, HttpServletResponse response) throws IOException {
    int alertType;
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      alertType = Integer.valueOf(request.getParameter("alertType"));
    } catch (NumberFormatException e) {
      // Empty json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
      if (LOGGER.finerEnabled()) {
        LOGGER.finer(e.getMessage());
      }
      return;
    }

    try {
      boolean isClearAll = Boolean.valueOf(request.getParameter("clearAll"));
      // get cluster object
      Cluster cluster = Repository.get().getCluster();
      cluster.clearAlerts(alertType, isClearAll);
      responseJSON.put("status", "deleted");
      responseJSON.put(
          "systemAlerts", SystemAlertsService.getAlertsJson(cluster,
              cluster.getNotificationPageNumber()));
      responseJSON.put("pageNumber", cluster.getNotificationPageNumber());

      boolean isGFConnected = cluster.isConnectedFlag();
      if(isGFConnected){
        responseJSON.put("connectedFlag", isGFConnected);
      }else{
        responseJSON.put("connectedFlag", isGFConnected);
        responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());
      }
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occurred : " + e.getMessage());
      }
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/acknowledgeAlert", method = RequestMethod.GET)
  public void acknowledgeAlert(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    int alertId;
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      alertId = Integer.valueOf(request.getParameter("alertId"));
    } catch (NumberFormatException e) {
      // Empty json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
      if (LOGGER.finerEnabled()) {
        LOGGER.finer(e.getMessage());
      }
      return;
    }

    try {
      // get cluster object
      Cluster cluster = Repository.get().getCluster();

      // set alert is acknowledged
      cluster.acknowledgeAlert(alertId);
      responseJSON.put("status", "deleted");
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserRegions", method = RequestMethod.GET)
  public void dataBrowserRegions(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();
    ArrayNode regionsData = mapper.createArrayNode();

    try {
      // getting cluster's Regions
      responseJSON.put("clusterName", cluster.getServerName());
      regionsData = getRegionsJson(cluster);
      responseJSON.put("clusterRegions", regionsData);
      responseJSON.put("connectedFlag", cluster.isConnectedFlag());
      responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  /**
   * This method creates json for list of cluster regions
   * 
   * @param cluster
   * @return ArrayNode JSON array
   */
  private ArrayNode getRegionsJson(Cluster cluster) {

    Collection<Cluster.Region> clusterRegions = cluster.getClusterRegions().values();
    ArrayNode regionsListJson = mapper.createArrayNode();

    if (!clusterRegions.isEmpty()) {
      for (Cluster.Region region : clusterRegions) {
        ObjectNode regionJSON = mapper.createObjectNode();
        regionJSON.put("name", region.getName());
        regionJSON.put("fullPath", region.getFullPath());
        regionJSON.put("regionType", region.getRegionType());

        if (region.getRegionType().contains("PARTITION")) {
          regionJSON.put("isPartition", true);
        } else {
          regionJSON.put("isPartition", false);
        }

        regionJSON.put("memberCount", region.getMemberCount());
        List<String> regionsMembers = region.getMemberName();
        ArrayNode jsonRegionMembers = mapper.createArrayNode();

        for (int i = 0; i < regionsMembers.size(); i++) {
          Cluster.Member member = cluster.getMembersHMap().get(
              regionsMembers.get(i));
          ObjectNode jsonMember = mapper.createObjectNode();
          jsonMember.put("key", regionsMembers.get(i));
          jsonMember.put("id", member.getId());
          jsonMember.put("name", member.getName());

          jsonRegionMembers.add(jsonMember);
        }

        regionJSON.put("members", jsonRegionMembers);
        regionsListJson.add(regionJSON);
      }
    }
    return regionsListJson;
  }

  @RequestMapping(value = "/dataBrowserQuery", method = RequestMethod.GET)
  public void dataBrowserQuery(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    // get query string
    String query = request.getParameter("query");
    String members = request.getParameter("members");
    int limit = 0;

    try {
      limit = Integer.valueOf(request.getParameter("limit"));
    } catch (NumberFormatException e) {
      limit = 0;
      if (LOGGER.finerEnabled()) {
        LOGGER.finer(e.getMessage());
      }
    }

    ObjectNode queryResult = mapper.createObjectNode();
    try {

      if (StringUtils.isNotNullNotEmptyNotWhiteSpace(query)) {
        // get cluster object
        Cluster cluster = Repository.get().getCluster();
        String userName = request.getUserPrincipal().getName();

        // Call execute query method
        queryResult = cluster.executeQuery(query, members, limit);

        // Add query in history if query is executed successfully
        if (!queryResult.has("error")) {
          // Add html escaped query to history
          String escapedQuery = StringEscapeUtils.escapeHtml(query);
          cluster.addQueryInHistory(escapedQuery, userName);
        }
      }
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    response.getOutputStream().write(queryResult.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserQueryHistory", method = RequestMethod.GET)
  public void dataBrowserQueryHistory(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    ObjectNode responseJSON = mapper.createObjectNode();
    ArrayNode queryResult = null;
    String action = "";

    try {
      // get cluster object
      Cluster cluster = Repository.get().getCluster();
      String userName = request.getUserPrincipal().getName();

      // get query string
      action = request.getParameter(QUERYSTRING_PARAM_ACTION);
      if (!StringUtils.isNotNullNotEmptyNotWhiteSpace(action)) {
        action = ACTION_VIEW;
      }

      if (action.toLowerCase().equalsIgnoreCase(ACTION_DELETE)) {
        String queryId = request.getParameter(QUERYSTRING_PARAM_QUERYID);
        if (StringUtils.isNotNullNotEmptyNotWhiteSpace(queryId)) {

          boolean deleteStatus = cluster.deleteQueryById(userName, queryId);
          if (deleteStatus) {
            responseJSON.put("status", STATUS_REPSONSE_SUCCESS);
          } else {
            responseJSON.put("status", STATUS_REPSONSE_FAIL);
            responseJSON.put("error", ERROR_REPSONSE_QUERYNOTFOUND);
          }
        } else {
          responseJSON.put("status", STATUS_REPSONSE_FAIL);
          responseJSON.put("error", ERROR_REPSONSE_QUERYIDMISSING);
        }
      }

      // Get list of past executed queries
      queryResult = cluster.getQueryHistoryByUserId(userName);
      responseJSON.put("queryHistory", queryResult);
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserExport", method = RequestMethod.POST)
  public void dataBrowserExport(HttpServletRequest request,
      HttpServletResponse response) throws IOException {

    // get query string
    String filename = request.getParameter("filename");
    String resultContent = request.getParameter("content");

    response.setHeader("Cache-Control", "");
    response.setHeader("Content-type", "text/plain");
    if (StringUtils.isNotNullNotEmptyNotWhiteSpace(filename)) {
      response.setHeader("Content-Disposition", "attachment; filename=" + filename);
    } else {
      response.setHeader("Content-Disposition", "attachment; filename=" + DEFAULT_EXPORT_FILENAME);
    }

    if (!StringUtils.isNotNullNotEmptyNotWhiteSpace(resultContent)) {
      resultContent = "";
    }

    response.getOutputStream().write(resultContent.getBytes());
  }

  @RequestMapping(value = "/pulseProductSupport", method = RequestMethod.GET)
  public void getConfiguredPulseProduct(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
      ObjectNode responseJSON = mapper.createObjectNode();

    try {
      responseJSON.put("product", pulseProductSupport);

      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occurred : " + e.getMessage());
      }
    }
  }

  @RequestMapping(value = "/getQueryStatisticsGridModel", method = RequestMethod.GET)
  public void getQueryStatisticsGridModel(HttpServletRequest request,
      HttpServletResponse response) throws IOException {

    ObjectNode responseJSON = mapper.createObjectNode();
    // get cluster object
    Cluster cluster = Repository.get().getCluster();
    String userName = request.getUserPrincipal().getName();

    try {
      String[] arrColNames = Cluster.Statement.getGridColumnNames();
      String[] arrColAttribs = Cluster.Statement.getGridColumnAttributes();
      int[] arrColWidths = Cluster.Statement.getGridColumnWidths();

      ArrayNode colNamesList = mapper.createArrayNode();
      for (int i = 0; i < arrColNames.length; ++i) {
        colNamesList.add(arrColNames[i]);
      }

      ArrayNode colModelList = mapper.createArrayNode();
      for (int i = 0; i < arrColAttribs.length; ++i) {
        ObjectNode columnJSON = mapper.createObjectNode();
        columnJSON.put("name", arrColAttribs[i]);
        columnJSON.put("index", arrColAttribs[i]);
        columnJSON.put("width", arrColWidths[i]);
        columnJSON.put("sortable", "true");
        columnJSON.put("sorttype", ((i == 0) ? "String" : "integer"));
        colModelList.add(columnJSON);
      }

      responseJSON.put("columnNames", colNamesList);
      responseJSON.put("columnModels", colModelList);
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);

      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }
  }

  /**
   * @return the pulseProductSupport
   */
  public static String getPulseProductSupport() {
    return pulseProductSupport;
  }

  /**
   * @param pulseProductSupport
   *          the pulseProductSupport to set
   */
  public static void setPulseProductSupport(String pulseProductSupport) {
    PulseController.pulseProductSupport = pulseProductSupport;
  }
}