/*
 * =========================================================================
 *  Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
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
 * Class ClusterDiskThroughput This class contains implementations for getting
 * cluster's current disk throughput details and its trend over time
 * 
 * @author Sachin K.
 * @since version 7.0.Beta
 */

@Component
@Service("ClusterDiskThroughput")
@Scope("singleton")
public class ClusterDiskThroughputService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();
    // clucter's Throughout Writes trend added to json response object
    // CircularFifoBuffer throughoutWritesTrend =
    // cluster.getThroughoutWritesTrend();
    Float currentThroughputWrites = cluster.getDiskWritesRate();
    Float currentThroughputReads = cluster.getDiskReadsRate();

    try {
      responseJSON.put("currentThroughputReads", currentThroughputReads);
      responseJSON.put(
          "throughputReads",
          new JSONArray(cluster
              .getStatisticTrend(Cluster.CLUSTER_STAT_THROUGHPUT_READS)));

      responseJSON.put("currentThroughputWrites", currentThroughputWrites);
      responseJSON.put(
          "throughputWrites",
          new JSONArray(cluster
              .getStatisticTrend(Cluster.CLUSTER_STAT_THROUGHPUT_WRITES)));

      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
