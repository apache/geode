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
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class MemberHeapUsageService
 * 
 * This class contains implementations of getting Memeber's current Heap Usage
 * and its trend over the time.
 * 
 * @author Sachin K
 * @since version 7.5
 */
@Component
@Service("MemberHeapUsage")
@Scope("singleton")
public class MemberHeapUsageService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    // members list
    try {
      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      String memberName = requestDataJSON.getJSONObject("MemberHeapUsage")
          .getString("memberName");

      Cluster.Member clusterMember = cluster.getMember(StringUtils
          .makeCompliantName(memberName));
      if (clusterMember != null) {
        // response
        responseJSON
            .put(
                "heapUsageTrend",
                new JSONArray(
                    clusterMember
                        .getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_HEAP_USAGE_SAMPLE)));
        responseJSON
            .put("currentHeapUsage", clusterMember.getCurrentHeapSize());
      }

      // Send json response
      return responseJSON;

    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
