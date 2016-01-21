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
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class MemberAsynchEventQueuesService
 * 
 * This class contains implementations of getting Asynchronous Event Queues
 * details of Cluster Member.
 * 
 * @author Sachin K
 * @since version 7.5
 */
@Component
@Service("MemberAsynchEventQueues")
@Scope("singleton")
public class MemberAsynchEventQueuesService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {

      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      String memberName = requestDataJSON.getJSONObject(
          "MemberAsynchEventQueues").getString("memberName");

      Cluster.Member clusterMember = cluster.getMember(StringUtils
          .makeCompliantName(memberName));

      if (clusterMember != null) {
        // response
        Cluster.AsyncEventQueue[] asyncEventQueues = clusterMember
            .getMemberAsyncEventQueueList();
        JSONArray asyncEventQueueJsonList = new JSONArray();

        if (asyncEventQueues != null && asyncEventQueues.length > 0) {

          responseJSON.put("isAsyncEventQueuesPresent", true);

          for (Cluster.AsyncEventQueue asyncEventQueue : asyncEventQueues) {
            JSONObject asyncEventQueueJSON = new JSONObject();
            asyncEventQueueJSON.put("id", asyncEventQueue.getId());
            asyncEventQueueJSON.put("primary", asyncEventQueue.getPrimary());
            asyncEventQueueJSON.put("senderType", asyncEventQueue.isParallel());
            asyncEventQueueJSON
                .put("batchSize", asyncEventQueue.getBatchSize());
            asyncEventQueueJSON.put("batchTimeInterval",
                asyncEventQueue.getBatchTimeInterval());
            asyncEventQueueJSON.put("batchConflationEnabled",
                asyncEventQueue.isBatchConflationEnabled());
            asyncEventQueueJSON.put("asyncEventListener",
                asyncEventQueue.getAsyncEventListener());
            asyncEventQueueJSON.put("queueSize",
                asyncEventQueue.getEventQueueSize());

            asyncEventQueueJsonList.put(asyncEventQueueJSON);
          }
          responseJSON.put("asyncEventQueues", asyncEventQueueJsonList);
        } else {
          responseJSON.put("isAsyncEventQueuesPresent", false);
        }

      }
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
