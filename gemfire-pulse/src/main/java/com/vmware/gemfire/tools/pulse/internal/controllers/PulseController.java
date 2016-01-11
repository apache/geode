/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.PulseVersion;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import com.vmware.gemfire.tools.pulse.internal.service.PulseService;
import com.vmware.gemfire.tools.pulse.internal.service.PulseServiceFactory;
import com.vmware.gemfire.tools.pulse.internal.service.SystemAlertsService;
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class PulseController
 * 
 * This class contains the implementations for all http Ajax requests needs to
 * be served in Pulse.
 * 
 * @author azambre
 * @since version 7.5
 */
@Controller
public class PulseController {

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

  private final JSONObject NoDataJSON = new JSONObject();

  // Shared object to hold pulse version details
  public static PulseVersion pulseVersion = new PulseVersion();
  //default is gemfire
  private static String pulseProductSupport = PulseConstants.PRODUCT_NAME_GEMFIRE;

  @Autowired
  PulseServiceFactory pulseServiceFactory;

  @RequestMapping(value = "/pulseUpdate", method = RequestMethod.POST)
  public void getPulseUpdate(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    String pulseData = request.getParameter("pulseData");

    JSONObject responseMap = new JSONObject();

    JSONObject requestMap = null;

    try {
      requestMap = new JSONObject(pulseData);
      Iterator<?> keys = requestMap.keys();

      // Execute Services
      while (keys.hasNext()) {
        String serviceName = keys.next().toString();
        try {
          PulseService pulseService = pulseServiceFactory
              .getPulseServiceInstance(serviceName);
          responseMap.put(serviceName, pulseService.execute(request));
        } catch (Exception serviceException) {
          LOGGER.warning("serviceException [for service "+serviceName+"] = " + serviceException.getMessage());
          responseMap.put(serviceName, NoDataJSON);
        }
      }

    } catch (JSONException eJSON) {
      LOGGER.logJSONError(new JSONException(eJSON),
          new String[] { "requestMap:"
              + ((requestMap == null) ? "" : requestMap) });
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    // Create Response
    response.getOutputStream().write(responseMap.toString().getBytes());
  }

  @RequestMapping(value = "/authenticateUser", method = RequestMethod.GET)
  public void authenticateUser(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      responseJSON.put("isUserLoggedIn", this.isUserLoggedIn(request));
      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, null);
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
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

  @RequestMapping(value = "/clusterLogout", method = RequestMethod.GET)
  public void clusterLogout(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    HttpSession session = request.getSession(false);
    if (session != null) {

      // End session and redirect
      session.invalidate();
    }
    response.sendRedirect("../Login.html");
  }

  @RequestMapping(value = "/pulseVersion", method = RequestMethod.GET)
  public void pulseVersion(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      // Reference to repository
      Repository repository = Repository.get();
      // set pulse web app url
      String pulseWebAppUrl = request.getScheme() + "://"
          + request.getServerName() + ":" + request.getServerPort()
          + request.getContextPath();

      repository.setPulseWebAppUrl(pulseWebAppUrl);

      // Response
      responseJSON.put("pulseVersion",
          PulseController.pulseVersion.getPulseVersion());
      responseJSON.put("buildId",
          PulseController.pulseVersion.getPulseBuildId());
      responseJSON.put("buildDate",
          PulseController.pulseVersion.getPulseBuildDate());
      responseJSON.put("sourceDate",
          PulseController.pulseVersion.getPulseSourceDate());
      responseJSON.put("sourceRevision",
          PulseController.pulseVersion.getPulseSourceRevision());
      responseJSON.put("sourceRepository",
          PulseController.pulseVersion.getPulseSourceRepository());

    } catch (JSONException eJSON) {
      LOGGER
          .logJSONError(eJSON, new String[] { "pulseVersionData :"
              + PulseController.pulseVersion });
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/clearAlerts", method = RequestMethod.GET)
  public void clearAlerts(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    int alertType;
    JSONObject responseJSON = new JSONObject();
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

    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, null);
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }

    // Send json response
    response.getOutputStream().write(responseJSON.toString().getBytes());
  }

  @RequestMapping(value = "/acknowledgeAlert", method = RequestMethod.GET)
  public void acknowledgeAlert(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    int alertId;
    JSONObject responseJSON = new JSONObject();

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
    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, null);
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
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();
    List<JSONObject> regionsData = Collections.emptyList();

    try {
      // getting cluster's Regions
      responseJSON.put("clusterName", cluster.getServerName());
      regionsData = getRegionsJson(cluster);
      responseJSON.put("clusterRegions", regionsData);
      responseJSON.put("connectedFlag", cluster.isConnectedFlag());
      responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());

    } catch (JSONException eJSON) {
      LOGGER.logJSONError(
          eJSON,
          new String[] { "clusterName:" + cluster.getServerName(),
              "clusterRegions:" + regionsData,
              "connectedFlag:" + cluster.isConnectedFlag(),
              "connectedErrorMsg:" + cluster.getConnectionErrorMsg() });
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
   * @return JSONObject Array List
   */
  private List<JSONObject> getRegionsJson(Cluster cluster) throws JSONException {

    Collection<Cluster.Region> clusterRegions = cluster.getClusterRegions()
        .values();
    List<JSONObject> regionsListJson = new ArrayList<JSONObject>();

    if (!clusterRegions.isEmpty()) {
      for (Cluster.Region region : clusterRegions) {
        JSONObject regionJSON = new JSONObject();

        regionJSON.put("name", region.getName());
        regionJSON.put("fullPath", region.getFullPath());
        regionJSON.put("regionType", region.getRegionType());
        if(region.getRegionType().contains("PARTITION")){
          regionJSON.put("isPartition", true);
        }else{
          regionJSON.put("isPartition", false);
        }
        regionJSON.put("memberCount", region.getMemberCount());
        List<String> regionsMembers = region.getMemberName();
        JSONArray jsonRegionMembers = new JSONArray();
        for (int i = 0; i < regionsMembers.size(); i++) {
          Cluster.Member member = cluster.getMembersHMap().get(
              regionsMembers.get(i));
          JSONObject jsonMember = new JSONObject();
          jsonMember.put("key", regionsMembers.get(i));
          jsonMember.put("id", member.getId());
          jsonMember.put("name", member.getName());

          jsonRegionMembers.put(jsonMember);
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
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
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

    JSONObject queryResult = new JSONObject();
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
    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, new String[] { "queryResult:" + queryResult });
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
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    JSONObject responseJSON = new JSONObject();
    JSONArray queryResult = null;
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

    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, new String[] { "action:" + action,
          "queryResult:" + queryResult });
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
      response.setHeader("Content-Disposition", "attachment; filename="
          + filename);
    } else {
      response.setHeader("Content-Disposition", "attachment; filename="
          + DEFAULT_EXPORT_FILENAME);
    }

    if (!StringUtils.isNotNullNotEmptyNotWhiteSpace(resultContent)) {
      resultContent = "";
    }

    response.getOutputStream().write(resultContent.getBytes());
  }

  @RequestMapping(value = "/pulseProductSupport", method = RequestMethod.GET)
  public void getConfiguredPulseProduct(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    JSONObject responseJSON = new JSONObject();

    try {
      responseJSON.put("product", pulseProductSupport);

      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, null);
    } catch (Exception e) {
      if (LOGGER.fineEnabled()) {
        LOGGER.fine("Exception Occured : " + e.getMessage());
      }
    }
  }

  @RequestMapping(value = "/getQueryStatisticsGridModel", method = RequestMethod.GET)
  public void getQueryStatisticsGridModel(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    JSONObject responseJSON = new JSONObject();
    // get cluster object
    Cluster cluster = Repository.get().getCluster();
    String userName = request.getUserPrincipal().getName();

    try {
      String[] arrColNames = Cluster.Statement.getGridColumnNames();
      String[] arrColAttribs = Cluster.Statement.getGridColumnAttributes();
      int[] arrColWidths = Cluster.Statement.getGridColumnWidths();

      JSONArray colNamesList = new JSONArray();
      for (int i = 0; i < arrColNames.length; ++i) {
        colNamesList.put(arrColNames[i]);
      }

      JSONArray colModelList = new JSONArray();
      JSONObject columnJSON = null;
      for (int i = 0; i < arrColAttribs.length; ++i) {
        columnJSON = new JSONObject();
        columnJSON.put("name", arrColAttribs[i]);
        columnJSON.put("index", arrColAttribs[i]);
        columnJSON.put("width", arrColWidths[i]);
        columnJSON.put("sortable", "true");
        columnJSON.put("sorttype", ((i == 0) ? "String" : "integer"));
        colModelList.put(columnJSON);
      }

      responseJSON.put("columnNames", colNamesList);
      responseJSON.put("columnModels", colModelList);
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);

      // Send json response
      response.getOutputStream().write(responseJSON.toString().getBytes());
    } catch (JSONException eJSON) {
      LOGGER.logJSONError(eJSON, null);
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