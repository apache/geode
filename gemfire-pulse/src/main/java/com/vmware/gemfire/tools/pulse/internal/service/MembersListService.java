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
 * Class MembersListService
 * 
 * This class contains implementations of getting list of Cluster Members.
 * 
 * @author Sachin K
 * @since version 7.5
 */
@Component
@Service("MembersList")
@Scope("singleton")
public class MembersListService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    // members list
    JSONArray memberListJson = new JSONArray();
    Cluster.Member[] memberSet = cluster.getMembers();

    try {
      for (Cluster.Member member : memberSet) {
        JSONObject memberJSON = new JSONObject();
        memberJSON.put("memberId", member.getId());
        memberJSON.put("name", member.getName());
        memberJSON.put("host", member.getHost());

        memberListJson.put(memberJSON);
      }

      // Response JSON
      responseJSON.put("clusterMembers", memberListJson);
      responseJSON.put("clusterName", cluster.getServerName());
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
