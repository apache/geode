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
import org.apache.geode.tools.pulse.internal.util.TimeUtils;

/**
 * Class MemberClientsService
 *
 * This class contains implementations of getting Member's Clients.
 *
 * @since GemFire version 7.5
 */
@Component
@Service("MemberClients")
@Scope("singleton")
public class MemberClientsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  // String constants used for forming a json response
  private static final String NAME = "name";
  private static final String HOST = "host";

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    String memberName = requestDataJSON.get("MemberClients").get("memberName").textValue();

    ArrayNode clientListJson = mapper.createArrayNode();

    Cluster.Member clusterMember = cluster.getMember(makeCompliantName(memberName));
    if (clusterMember != null) {
      responseJSON.put("memberId", clusterMember.getId());
      responseJSON.put(NAME, clusterMember.getName());
      responseJSON.put(HOST, clusterMember.getHost());

      // member's clients

      Cluster.Client[] memberClients = clusterMember.getMemberClients();
      for (Cluster.Client memberClient : memberClients) {
        ObjectNode regionJSON = mapper.createObjectNode();
        regionJSON.put("clientId", memberClient.getId());
        regionJSON.put(NAME, memberClient.getName());
        regionJSON.put(HOST, memberClient.getHost());
        regionJSON.put("queueSize", memberClient.getQueueSize());
        regionJSON.put("clientCQCount", memberClient.getClientCQCount());
        regionJSON.put("isConnected", memberClient.isConnected() ? "Yes" : "No");
        regionJSON.put("isSubscriptionEnabled",
            memberClient.isSubscriptionEnabled() ? "Yes" : "No");
        regionJSON.put("uptime", TimeUtils.convertTimeSecondsToHMS(memberClient.getUptime()));

        regionJSON.put("cpuUsage", String.format("%.4f", memberClient.getCpuUsage()));
        // regionJSON.put("cpuUsage", memberClient.getCpuUsage());
        regionJSON.put("threads", memberClient.getThreads());
        regionJSON.put("gets", memberClient.getGets());
        regionJSON.put("puts", memberClient.getPuts());

        clientListJson.add(regionJSON);
      }
      responseJSON.set("memberClients", clientListJson);
    }
    // Send json response
    return responseJSON;

  }
}
