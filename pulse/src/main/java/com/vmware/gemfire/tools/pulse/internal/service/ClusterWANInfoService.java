/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Class ClusterWANInfoService
 * 
 * This class contains implementations of getting Cluster's WAN Informations
 * (connected clusters)
 * 
 * @author Sachin K
 * @since version 7.5
 */
@Component
@Service("ClusterWANInfo")
@Scope("singleton")
public class ClusterWANInfoService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    // members list
    // ArrayList<Cluster> connectedClusters = cluster.getConnectedClusterList();
    JSONArray connectedClusterListJson = new JSONArray();

    try {

      for (Map.Entry<String, Boolean> entry : cluster.getWanInformation()
          .entrySet()) {
        JSONObject clusterJSON = new JSONObject();
        clusterJSON.put("clusterId", entry.getKey());
        clusterJSON.put("name", entry.getKey());
        clusterJSON.put("status", entry.getValue());

        connectedClusterListJson.put(clusterJSON);
      }
      // Response JSON
      responseJSON.put("connectedClusters", connectedClusterListJson);
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }

}
