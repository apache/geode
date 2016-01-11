/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import java.text.DecimalFormat;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Class ClusterDetailsService
 * 
 * This service class has implementation for providing cluster's basic
 * statistical data.
 * 
 * @author azambare
 * @since version 7.5
 */

@Component
@Service("ClusterDetails")
@Scope("singleton")
public class ClusterDetailsService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    String userName = request.getUserPrincipal().getName();

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    Cluster.Alert[] alertsList = cluster.getAlertsList();
    int severeAlertCount = 0;
    int errorAlertCount = 0;
    int warningAlertCount = 0;
    int infoAlertCount = 0;

    for (Cluster.Alert alertObj : alertsList) {
      if (alertObj.getSeverity() == Cluster.Alert.SEVERE) {
        severeAlertCount++;
      } else if (alertObj.getSeverity() == Cluster.Alert.ERROR) {
        errorAlertCount++;
      } else if (alertObj.getSeverity() == Cluster.Alert.WARNING) {
        warningAlertCount++;
      } else {
        infoAlertCount++;
      }
    }
    try {
      // getting basic details of Cluster
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("severeAlertCount", severeAlertCount);
      responseJSON.put("errorAlertCount", errorAlertCount);
      responseJSON.put("warningAlertCount", warningAlertCount);
      responseJSON.put("infoAlertCount", infoAlertCount);

      responseJSON.put("totalMembers", cluster.getMemberCount());
      responseJSON.put("servers", cluster.getServerCount());
      responseJSON.put("clients", cluster.getClientConnectionCount());
      responseJSON.put("locators", cluster.getLocatorCount());
      responseJSON.put("totalRegions", cluster.getTotalRegionCount());
      Long heapSize = cluster.getTotalHeapSize();

      DecimalFormat df2 = new DecimalFormat(
          PulseConstants.DECIMAL_FORMAT_PATTERN);
      Double heapS = heapSize.doubleValue() / 1024;
      responseJSON.put("totalHeap", Double.valueOf(df2.format(heapS)));
      responseJSON.put("functions", cluster.getRunningFunctionCount());
      responseJSON.put("uniqueCQs", cluster.getRegisteredCQCount());
      responseJSON.put("subscriptions", cluster.getSubscriptionCount());
      responseJSON.put("txnCommitted", cluster.getTxnCommittedCount());
      responseJSON.put("txnRollback", cluster.getTxnRollbackCount());
      responseJSON.put("userName", userName);

      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
