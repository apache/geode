/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.tools.pulse.internal.service;

import static org.apache.geode.tools.pulse.internal.util.NameUtil.makeCompliantName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class MemberGatewayHubService
 *
 * This class contains implementations of getting Gateway Receivers and Senders details of Cluster
 * Member.
 *
 * @since GemFire version 7.5
 */
@Component
@Service("MemberGatewayHub")
@Scope("singleton")
public class MemberGatewayHubService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    String memberName = requestDataJSON.get("MemberGatewayHub").get("memberName").textValue();

    Cluster.Member clusterMember = cluster.getMember(makeCompliantName(memberName));

    if (clusterMember != null) {
      // response
      // get gateway receiver
      Cluster.GatewayReceiver gatewayReceiver = clusterMember.getGatewayReceiver();

      boolean isGateway = false;

      if (gatewayReceiver != null) {
        responseJSON.put("isGatewayReceiver", true);
        responseJSON.put("listeningPort", gatewayReceiver.getListeningPort());
        responseJSON.put("linkTroughput", gatewayReceiver.getLinkThroughput());
        responseJSON.put("avgBatchLatency", gatewayReceiver.getAvgBatchProcessingTime());
      } else {
        responseJSON.put("isGatewayReceiver", false);
      }

      // get gateway senders
      Cluster.GatewaySender[] gatewaySenders = clusterMember.getMemberGatewaySenders();

      if (gatewaySenders.length > 0) {
        isGateway = true;
      }
      responseJSON.put("isGatewaySender", isGateway);
      // Senders
      ArrayNode gatewaySendersJsonList = mapper.createArrayNode();

      for (Cluster.GatewaySender gatewaySender : gatewaySenders) {
        ObjectNode gatewaySenderJSON = mapper.createObjectNode();
        gatewaySenderJSON.put("id", gatewaySender.getId());
        gatewaySenderJSON.put("queueSize", gatewaySender.getQueueSize());
        gatewaySenderJSON.put("status", gatewaySender.getStatus());
        gatewaySenderJSON.put("primary", gatewaySender.getPrimary());
        gatewaySenderJSON.put("senderType", gatewaySender.getSenderType());
        gatewaySenderJSON.put("batchSize", gatewaySender.getBatchSize());
        gatewaySenderJSON.put("PersistenceEnabled", gatewaySender.getPersistenceEnabled());
        gatewaySenderJSON.put("remoteDSId", gatewaySender.getRemoteDSId());
        gatewaySenderJSON.put("eventsExceedingAlertThreshold",
            gatewaySender.getEventsExceedingAlertThreshold());

        gatewaySendersJsonList.add(gatewaySenderJSON);
      }
      // senders response
      responseJSON.set("gatewaySenders", gatewaySendersJsonList);

      // async event queues
      Cluster.AsyncEventQueue[] asyncEventQueues = clusterMember.getMemberAsyncEventQueueList();
      ArrayNode asyncEventQueueJsonList = mapper.createArrayNode();

      for (Cluster.AsyncEventQueue asyncEventQueue : asyncEventQueues) {
        ObjectNode asyncEventQueueJSON = mapper.createObjectNode();
        asyncEventQueueJSON.put("id", asyncEventQueue.getId());
        asyncEventQueueJSON.put("primary", asyncEventQueue.getPrimary());
        asyncEventQueueJSON.put("senderType", asyncEventQueue.isParallel());
        asyncEventQueueJSON.put("batchSize", asyncEventQueue.getBatchSize());
        asyncEventQueueJSON.put("batchTimeInterval", asyncEventQueue.getBatchTimeInterval());
        asyncEventQueueJSON.put("batchConflationEnabled",
            asyncEventQueue.isBatchConflationEnabled());
        asyncEventQueueJSON.put("asyncEventListener", asyncEventQueue.getAsyncEventListener());
        asyncEventQueueJSON.put("queueSize", asyncEventQueue.getEventQueueSize());

        asyncEventQueueJsonList.add(asyncEventQueueJSON);
      }
      responseJSON.set("asyncEventQueues", asyncEventQueueJsonList);

      Map<String, Cluster.Region> clusterRegions = cluster.getClusterRegions();

      List<Cluster.Region> clusterRegionsList = new ArrayList<>(clusterRegions.values());

      ArrayNode regionsList = mapper.createArrayNode();

      for (Cluster.Region region : clusterRegionsList) {
        if (region.getWanEnabled()) {
          ObjectNode regionJSON = mapper.createObjectNode();
          regionJSON.put("name", region.getName());
          regionsList.add(regionJSON);
        }
      }
      responseJSON.set("regionsInvolved", regionsList);
    }

    // Send json response
    return responseJSON;
  }
}
