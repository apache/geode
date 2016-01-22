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
