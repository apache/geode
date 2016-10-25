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

package org.apache.geode.tools.pulse.internal.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.util.StringUtils;
import org.apache.geode.tools.pulse.internal.util.TimeUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

/**
 * Class MemberClientsService
 * 
 * This class contains implementations of getting Memeber's Clients.
 * 
 * @since GemFire version 7.5
 */
@Component
@Service("MemberClients")
@Scope("singleton")
public class MemberClientsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  // String constants used for forming a json response
  private final String NAME = "name";
  private final String HOST = "host";

  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    String memberName = requestDataJSON.get("MemberClients").get("memberName").textValue();

    ArrayNode clientListJson = mapper.createArrayNode();

    Cluster.Member clusterMember = cluster.getMember(StringUtils.makeCompliantName(memberName));
    if (clusterMember != null) {
      responseJSON.put("memberId", clusterMember.getId());
      responseJSON.put(this.NAME, clusterMember.getName());
      responseJSON.put(this.HOST, clusterMember.getHost());

      // member's clients

      Cluster.Client[] memberClients = clusterMember.getMemberClients();
      for (Cluster.Client memberClient : memberClients) {
        ObjectNode regionJSON = mapper.createObjectNode();
        regionJSON.put("clientId", memberClient.getId());
        regionJSON.put(this.NAME, memberClient.getName());
        regionJSON.put(this.HOST, memberClient.getHost());
        regionJSON.put("queueSize", memberClient.getQueueSize());
        regionJSON.put("clientCQCount", memberClient.getClientCQCount());
        regionJSON.put("isConnected", memberClient.isConnected() ? "Yes" : "No");
        regionJSON.put("isSubscriptionEnabled", memberClient.isSubscriptionEnabled() ? "Yes" : "No");
        regionJSON.put("uptime", TimeUtils.convertTimeSecondsToHMS(memberClient.getUptime()));

        regionJSON.put("cpuUsage", String.format("%.4f", memberClient.getCpuUsage()).toString());
        // regionJSON.put("cpuUsage", memberClient.getCpuUsage());
        regionJSON.put("threads", memberClient.getThreads());
        regionJSON.put("gets", memberClient.getGets());
        regionJSON.put("puts", memberClient.getPuts());

        clientListJson.add(regionJSON);
      }
      responseJSON.put("memberClients", clientListJson);
    }
    // Send json response
    return responseJSON;

  }
}
