/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Class QueryStatisticsService
 * 
 * This class returns top N queries based on pagination and filtering criteria
 * if any
 * 
 * @author Riya Bhandekar
 * @since version 7.5
 */
@Component
@Service("QueryStatistics")
@Scope("singleton")
public class QueryStatisticsService implements PulseService {

  @Override
  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      Cluster.Statement[] stmts = cluster.getStatements();

      JSONObject queryJSON;
      JSONArray queryListJson = new JSONArray();
      for (int i = 0; i < stmts.length; ++i) {
        queryJSON = new JSONObject();
        queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_QUERYDEFINITION,
            stmts[i].getQueryDefinition());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED,
            (stmts[i].getNumTimesCompiled() < 0) ? this.VALUE_NA : stmts[i]
                .getNumTimesCompiled());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTION,
            (stmts[i].getNumExecution() < 0) ? this.VALUE_NA : stmts[i]
                .getNumExecution());
        queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS,
            (stmts[i].getNumExecutionsInProgress() < 0) ? this.VALUE_NA
                : stmts[i].getNumExecutionsInProgress());
        queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
            (stmts[i].getNumTimesGlobalIndexLookup() < 0) ? this.VALUE_NA
                : stmts[i].getNumTimesGlobalIndexLookup());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED,
            (stmts[i].getNumRowsModified() < 0) ? this.VALUE_NA : stmts[i]
                .getNumRowsModified());
        queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_PARSETIME, (stmts[i]
            .getParseTime() < 0) ? this.VALUE_NA : stmts[i].getParseTime());
        queryJSON.put(PulseConstants.MBEAN_ATTRIBUTE_BINDTIME, (stmts[i]
            .getBindTime() < 0) ? this.VALUE_NA : stmts[i].getBindTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME,
            (stmts[i].getOptimizeTime() < 0) ? this.VALUE_NA : stmts[i]
                .getOptimizeTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME,
            (stmts[i].getRoutingInfoTime() < 0) ? this.VALUE_NA : stmts[i]
                .getRoutingInfoTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_GENERATETIME,
            (stmts[i].getGenerateTime() < 0) ? this.VALUE_NA : stmts[i]
                .getGenerateTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME,
            (stmts[i].getTotalCompilationTime() < 0) ? this.VALUE_NA : stmts[i]
                .getTotalCompilationTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME,
            (stmts[i].getExecutionTime() < 0) ? this.VALUE_NA : stmts[i]
                .getExecutionTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME,
            (stmts[i].getProjectionTime() < 0) ? this.VALUE_NA : stmts[i]
                .getProjectionTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME,
            (stmts[i].getTotalExecutionTime() < 0) ? this.VALUE_NA : stmts[i]
                .getTotalExecutionTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME,
            (stmts[i].getRowsModificationTime() < 0) ? this.VALUE_NA : stmts[i]
                .getRowsModificationTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN,
            (stmts[i].getqNNumRowsSeen() < 0) ? this.VALUE_NA : stmts[i]
                .getqNNumRowsSeen());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME,
            (stmts[i].getqNMsgSendTime() < 0) ? this.VALUE_NA : stmts[i]
                .getqNMsgSendTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME,
            (stmts[i].getqNMsgSerTime() < 0) ? this.VALUE_NA : stmts[i]
                .getqNMsgSerTime());
        queryJSON.put(
            PulseConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME,
            (stmts[i].getqNRespDeSerTime() < 0) ? this.VALUE_NA : stmts[i]
                .getqNRespDeSerTime());
        queryListJson.put(queryJSON);
      }
      responseJSON.put("queriesList", queryListJson);

      // return jmx status
      responseJSON.put("connectedFlag", cluster.isConnectedFlag());
      responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());

      // Send json response
      return responseJSON;

    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
