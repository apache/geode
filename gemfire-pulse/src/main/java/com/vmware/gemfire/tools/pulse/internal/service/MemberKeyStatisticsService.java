/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
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
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class MemberKeyStatisticsService
 * 
 * This class contains implementations of getting Member's CPU, Memory and Read
 * Write details
 * 
 * @author Sachin K
 * @since version 7.5
 */
@Component
@Service("MemberKeyStatistics")
@Scope("singleton")
public class MemberKeyStatisticsService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      String memberName = requestDataJSON.getJSONObject("MemberKeyStatistics")
          .getString("memberName");

      Cluster.Member clusterMember = cluster.getMember(StringUtils
          .makeCompliantName(memberName));
      if (clusterMember != null) {
        // response
        responseJSON
            .put(
                "cpuUsageTrend",
                new JSONArray(
                    clusterMember
                        .getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_CPU_USAGE_SAMPLE)));
        responseJSON
            .put(
                "memoryUsageTrend",
                new JSONArray(
                    clusterMember
                        .getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_HEAP_USAGE_SAMPLE)));
        responseJSON
            .put(
                "readPerSecTrend",
                new JSONArray(
                    clusterMember
                        .getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_GETS_PER_SECOND)));
        responseJSON
            .put(
                "writePerSecTrend",
                new JSONArray(
                    clusterMember
                        .getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_PUTS_PER_SECOND)));
      }
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
