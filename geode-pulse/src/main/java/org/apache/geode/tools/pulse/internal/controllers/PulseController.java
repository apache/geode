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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseVersion;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.service.PulseService;
import org.apache.geode.tools.pulse.internal.service.PulseServiceFactory;
import org.apache.geode.tools.pulse.internal.service.SystemAlertsService;

/**
 * Class PulseController
 *
 * This class contains the implementations for all http Ajax requests needs to be served in Pulse.
 *
 * @since GemFire version 7.5
 */
@Controller
public class PulseController {

  private static final Logger logger = LogManager.getLogger();

  private static final String QUERYSTRING_PARAM_ACTION = "action";
  private static final String QUERYSTRING_PARAM_QUERYID = "queryId";
  private static final String ACTION_VIEW = "view";
  private static final String ACTION_DELETE = "delete";

  private static final String STATUS_REPSONSE_SUCCESS = "success";
  private static final String STATUS_REPSONSE_FAIL = "fail";

  private static final String ERROR_REPSONSE_QUERYNOTFOUND = "No queries found";
  private static final String ERROR_REPSONSE_QUERYIDMISSING = "Query id is missing";

  private static final String EMPTY_JSON = "{}";

  // Shared object to hold pulse version details
  public static PulseVersion pulseVersion = new PulseVersion();

  private final ObjectMapper mapper = new ObjectMapper();

  @Autowired
  PulseServiceFactory pulseServiceFactory;

  @RequestMapping(value = "/pulseUpdate", method = RequestMethod.POST)
  public void getPulseUpdate(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String pulseData = request.getParameter("pulseData");

    ObjectNode responseMap = mapper.createObjectNode();

    try {
      JsonNode requestMap = mapper.readTree(pulseData);
      Iterator<?> keys = requestMap.fieldNames();

      // Execute Services
      while (keys.hasNext()) {
        String serviceName = keys.next().toString();
        try {
          PulseService pulseService = pulseServiceFactory.getPulseServiceInstance(serviceName);
          responseMap.set(serviceName, pulseService.execute(request));
        } catch (Exception serviceException) {
          logger.warn("serviceException [for service {}] = {}", serviceName,
              serviceException.getMessage());
          responseMap.put(serviceName, EMPTY_JSON);
        }
      }
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }

    // Create Response
    response.getOutputStream().write(responseMap.toString().getBytes());
  }

  @RequestMapping(value = "/authenticateUser", method = RequestMethod.GET)
  public void authenticateUser(HttpServletRequest request, HttpServletResponse response) {
    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      responseJSON.put("isUserLoggedIn", isUserLoggedIn(request));
      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
  }

  /**
   * Method isUserLoggedIn Check whether user is logged in or not.
   *
   */
  protected boolean isUserLoggedIn(HttpServletRequest request) {
    return null != request.getUserPrincipal();
  }

  @RequestMapping(value = "/pulseVersion", method = RequestMethod.GET)
  public void pulseVersion(@SuppressWarnings("unused") HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      // Response
      responseJSON.put("pulseVersion", PulseController.pulseVersion.getPulseVersion());
      responseJSON.put("buildId", PulseController.pulseVersion.getPulseBuildId());
      responseJSON.put("buildDate", PulseController.pulseVersion.getPulseBuildDate());
      responseJSON.put("sourceDate", PulseController.pulseVersion.getPulseSourceDate());
      responseJSON.put("sourceRevision", PulseController.pulseVersion.getPulseSourceRevision());
      responseJSON.put("sourceRepository", PulseController.pulseVersion.getPulseSourceRepository());

    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/clearAlerts", method = RequestMethod.GET)
  public void clearAlerts(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    int alertType;
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      alertType = Integer.parseInt(request.getParameter("alertType"));
    } catch (NumberFormatException e) {
      // Empty json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
      logger.debug(e);
      return;
    }

    try {
      boolean isClearAll = Boolean.parseBoolean(request.getParameter("clearAll"));
      // get cluster object
      Cluster cluster = Repository.get().getCluster();
      cluster.clearAlerts(alertType, isClearAll);
      responseJSON.put("status", "deleted");
      responseJSON.set("systemAlerts",
          SystemAlertsService.getAlertsJson(cluster, cluster.getNotificationPageNumber()));
      responseJSON.put("pageNumber", cluster.getNotificationPageNumber());

      boolean isGFConnected = cluster.isConnectedFlag();
      if (isGFConnected) {
        responseJSON.put("connectedFlag", isGFConnected);
      } else {
        responseJSON.put("connectedFlag", isGFConnected);
        responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());
      }
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/acknowledgeAlert", method = RequestMethod.GET)
  public void acknowledgeAlert(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    int alertId;
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      alertId = Integer.parseInt(request.getParameter("alertId"));
    } catch (NumberFormatException e) {
      // Empty json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
      logger.debug(e);
      return;
    }

    try {
      // get cluster object
      Cluster cluster = Repository.get().getCluster();

      // set alert is acknowledged
      cluster.acknowledgeAlert(alertId);
      responseJSON.put("status", "deleted");
    } catch (Exception e) {
      logger.debug("Exception Occurred", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserRegions", method = RequestMethod.GET)
  public void dataBrowserRegions(@SuppressWarnings("unused") HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      // getting cluster's Regions
      responseJSON.put("clusterName", cluster.getServerName());
      ArrayNode regionsData = getRegionsJson(cluster);
      responseJSON.set("clusterRegions", regionsData);
      responseJSON.put("connectedFlag", cluster.isConnectedFlag());
      responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());
    } catch (Exception e) {
      logger.debug("Exception Occurred", e);
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  /**
   * This method creates json for list of cluster regions
   *
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

        for (String regionsMember : regionsMembers) {
          Cluster.Member member = cluster.getMembersHMap().get(regionsMember);
          ObjectNode jsonMember = mapper.createObjectNode();
          jsonMember.put("key", regionsMember);
          jsonMember.put("id", member.getId());
          jsonMember.put("name", member.getName());

          jsonRegionMembers.add(jsonMember);
        }

        regionJSON.set("members", jsonRegionMembers);
        regionsListJson.add(regionJSON);
      }
    }
    return regionsListJson;
  }

  @RequestMapping(value = "/dataBrowserQuery", method = RequestMethod.GET)
  public void dataBrowserQuery(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    // get query string
    String query = request.getParameter("query");
    String members = request.getParameter("members");
    int limit;

    try {
      limit = Integer.parseInt(request.getParameter("limit"));
    } catch (NumberFormatException e) {
      limit = 0;
      logger.debug(e);
    }

    ObjectNode queryResult = executeQuery(request, query, members, limit);

    response.getOutputStream().write(queryResult.toString().getBytes());
  }

  @RequestMapping(value = "/dataBrowserQueryHistory", method = RequestMethod.GET)
  public void dataBrowserQueryHistory(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    ObjectNode responseJSON = mapper.createObjectNode();

    try {
      // get cluster object
      Cluster cluster = Repository.get().getCluster();
      String userName = request.getUserPrincipal().getName();

      // get query string
      String action = request.getParameter(QUERYSTRING_PARAM_ACTION);
      if (StringUtils.isBlank(action)) {
        action = ACTION_VIEW;
      }

      if (action.toLowerCase().equalsIgnoreCase(ACTION_DELETE)) {
        String queryId = request.getParameter(QUERYSTRING_PARAM_QUERYID);
        if (StringUtils.isNotBlank(queryId)) {

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
      ArrayNode queryResult = cluster.getQueryHistoryByUserId(userName);
      responseJSON.set("queryHistory", queryResult);
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
    response.getOutputStream().write(responseJSON.toString().getBytes());


  }


  @RequestMapping(value = "/dataBrowserExport", method = RequestMethod.GET)
  public void dataBrowserExport(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    // get query string
    String query = request.getParameter("query");
    String members = request.getParameter("members");
    int limit;

    try {
      limit = Integer.parseInt(request.getParameter("limit"));
    } catch (NumberFormatException e) {
      limit = 0;
      logger.debug(e);
    }

    ObjectNode queryResult = executeQuery(request, query, members, limit);

    response.setContentType("application/json");
    response.setHeader("Content-Disposition", "attachment; filename=results.json");
    response.getOutputStream().write(queryResult.toString().getBytes());
  }

  @RequestMapping(value = "/getQueryStatisticsGridModel", method = RequestMethod.GET)
  public void getQueryStatisticsGridModel(HttpServletRequest request,
      HttpServletResponse response) {

    ObjectNode responseJSON = mapper.createObjectNode();
    // get cluster object
    Cluster cluster = Repository.get().getCluster();
    String userName = request.getUserPrincipal().getName();

    try {
      String[] arrColNames = Cluster.Statement.getGridColumnNames();
      String[] arrColAttribs = Cluster.Statement.getGridColumnAttributes();
      int[] arrColWidths = Cluster.Statement.getGridColumnWidths();

      ArrayNode colNamesList = mapper.createArrayNode();
      for (String arrColName : arrColNames) {
        colNamesList.add(arrColName);
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

      responseJSON.set("columnNames", colNamesList);
      responseJSON.set("columnModels", colModelList);
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);

      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
  }

  private ObjectNode executeQuery(HttpServletRequest request, String query, String members,
      int limit) {
    ObjectNode queryResult = mapper.createObjectNode();
    try {

      if (StringUtils.isNotBlank(query)) {
        // get cluster object
        Cluster cluster = Repository.get().getCluster();
        String userName = request.getUserPrincipal().getName();

        // Add html escaped query to history
        String escapedQuery = StringEscapeUtils.escapeHtml4(query);
        cluster.addQueryInHistory(escapedQuery, userName);

        // Call execute query method
        queryResult = cluster.executeQuery(query, members, limit);
      }
    } catch (Exception e) {
      logger.debug("Exception Occurred : ", e);
    }
    return queryResult;
  }
}
