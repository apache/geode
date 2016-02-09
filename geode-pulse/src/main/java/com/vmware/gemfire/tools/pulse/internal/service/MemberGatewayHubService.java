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

import java.util.ArrayList;
import java.util.List;
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
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class MemberGatewayHubService
 * 
 * This class contains implementations of getting Gateway Receivers and Senders
 * details of Cluster Member.
 * 
 * @author Sachin K
 * @since version 7.5
 */
@Component
@Service("MemberGatewayHub")
@Scope("singleton")
public class MemberGatewayHubService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {

      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      String memberName = requestDataJSON.getJSONObject("MemberGatewayHub")
          .getString("memberName");

      Cluster.Member clusterMember = cluster.getMember(StringUtils
          .makeCompliantName(memberName));

      if (clusterMember != null) {
        // response
        // get gateway receiver
        Cluster.GatewayReceiver gatewayReceiver = clusterMember
            .getGatewayReceiver();

        Boolean isGateway = false;

        if (gatewayReceiver != null) {
          responseJSON.put("isGatewayReceiver", true);
          responseJSON.put("listeningPort", gatewayReceiver.getListeningPort());
          responseJSON
              .put("linkTroughput", gatewayReceiver.getLinkThroughput());
          responseJSON.put("avgBatchLatency",
              gatewayReceiver.getAvgBatchProcessingTime());
        } else {
          responseJSON.put("isGatewayReceiver", false);
        }

        // get gateway senders
        Cluster.GatewaySender[] gatewaySenders = clusterMember
            .getMemberGatewaySenders();

        if (gatewaySenders.length > 0) {
          isGateway = true;
        }
        responseJSON.put("isGatewaySender", isGateway);
        // Senders
        JSONArray gatewaySendersJsonList = new JSONArray();

        for (Cluster.GatewaySender gatewaySender : gatewaySenders) {
          JSONObject gatewaySenderJSON = new JSONObject();
          gatewaySenderJSON.put("id", gatewaySender.getId());
          gatewaySenderJSON.put("queueSize", gatewaySender.getQueueSize());
          gatewaySenderJSON.put("status", gatewaySender.getStatus());
          gatewaySenderJSON.put("primary", gatewaySender.getPrimary());
          gatewaySenderJSON.put("senderType", gatewaySender.getSenderType());
          gatewaySenderJSON.put("batchSize", gatewaySender.getBatchSize());
          gatewaySenderJSON.put("PersistenceEnabled",
              gatewaySender.getPersistenceEnabled());
          gatewaySenderJSON.put("remoteDSId",
              gatewaySender.getRemoteDSId());
          gatewaySenderJSON.put("eventsExceedingAlertThreshold",
              gatewaySender.getEventsExceedingAlertThreshold());

          gatewaySendersJsonList.put(gatewaySenderJSON);
        }
        // senders response
        responseJSON.put("gatewaySenders", gatewaySendersJsonList);

        // async event queues
        Cluster.AsyncEventQueue[] asyncEventQueues = clusterMember.getMemberAsyncEventQueueList();
        JSONArray asyncEventQueueJsonList = new JSONArray();

        for (Cluster.AsyncEventQueue asyncEventQueue : asyncEventQueues) {
          JSONObject asyncEventQueueJSON = new JSONObject();
          asyncEventQueueJSON.put("id", asyncEventQueue.getId());
          asyncEventQueueJSON.put("primary", asyncEventQueue.getPrimary());
          asyncEventQueueJSON.put("senderType", asyncEventQueue.isParallel());
          asyncEventQueueJSON.put("batchSize", asyncEventQueue.getBatchSize());
          asyncEventQueueJSON.put("batchTimeInterval", asyncEventQueue.getBatchTimeInterval());
          asyncEventQueueJSON.put("batchConflationEnabled", asyncEventQueue.isBatchConflationEnabled());
          asyncEventQueueJSON.put("asyncEventListener", asyncEventQueue.getAsyncEventListener());
          asyncEventQueueJSON.put("queueSize", asyncEventQueue.getEventQueueSize());

          asyncEventQueueJsonList.put(asyncEventQueueJSON);
        }
        responseJSON.put("asyncEventQueues", asyncEventQueueJsonList);


        Map<String,Cluster.Region> clusterRegions = cluster.getClusterRegions();

        List<Cluster.Region> clusterRegionsList = new ArrayList<Cluster.Region>();
        clusterRegionsList.addAll(clusterRegions.values());
        
        JSONArray regionsList = new JSONArray();

        for (Cluster.Region region : clusterRegionsList) {
          if (region.getWanEnabled()) {
            JSONObject regionJSON = new JSONObject();
            regionJSON.put("name", region.getName());
            regionsList.put(regionJSON);
          }
        }
        responseJSON.put("regionsInvolved", regionsList);
      }
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
