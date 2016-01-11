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
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Class ClusterMemoryUsageService
 * 
 * This class contains implementations of getting Cluster's overall current
 * memory usage details and its trend over the time.
 * 
 * @author Anchal G
 * @since version 7.5
 */

@Component
@Service("ClusterMemoryUsage")
@Scope("singleton")
public class ClusterMemoryUsageService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();
    // clucter's Memory Usage trend added to json response object

    try {
      responseJSON.put("currentMemoryUsage", cluster.getUsedHeapSize());
      responseJSON.put(
          "memoryUsageTrend",
          new JSONArray(cluster
              .getStatisticTrend(Cluster.CLUSTER_STAT_MEMORY_USAGE)));
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
