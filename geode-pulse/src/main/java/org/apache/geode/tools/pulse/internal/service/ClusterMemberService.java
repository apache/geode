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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.util.TimeUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Class ClusterMemberService
 * 
 * This class contains implementations of getting Cluster Member's details
 * 
 * @since GemFire version 7.5
 */
@Component
// @Service("ClusterMember")
@Service("ClusterMembers")
@Scope("singleton")
public class ClusterMemberService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  private final String HEAP_USAGE = "heapUsage";

  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    Cluster.Member[] clusterMembersList = cluster.getMembers();

    // create members json
    ArrayNode memberListJson = mapper.createArrayNode();
    for (Cluster.Member clusterMember : clusterMembersList) {
      ObjectNode memberJSON = mapper.createObjectNode();
      // getting members detail
      memberJSON.put("gemfireVersion", clusterMember.getGemfireVersion());
      memberJSON.put("memberId", clusterMember.getId());
      memberJSON.put("name", clusterMember.getName());
      memberJSON.put("host", clusterMember.getHost());

      List<String> serverGroups = clusterMember.getServerGroups();
      if(serverGroups.size() == 0){
        serverGroups = new ArrayList<>();
        serverGroups.add(PulseConstants.DEFAULT_SERVER_GROUP);
      }

      memberJSON.put("serverGroups", mapper.valueToTree(serverGroups));

      List<String> redundancyZones = clusterMember.getRedundancyZones();
      if(redundancyZones.size() == 0){
        redundancyZones = new ArrayList<String>();
        redundancyZones.add(PulseConstants.DEFAULT_REDUNDANCY_ZONE);
      }
      memberJSON.put("redundancyZones", mapper.valueToTree(redundancyZones));

      long usedHeapSize = cluster.getUsedHeapSize();
      long currentHeap = clusterMember.getCurrentHeapSize();
      if (usedHeapSize > 0) {
        double heapUsage = ((double) currentHeap / (double) usedHeapSize) * 100;
        memberJSON.put(this.HEAP_USAGE, truncate(heapUsage, 2));
      } else {
        memberJSON.put(this.HEAP_USAGE, 0);
      }
      double currentCPUUsage = clusterMember.getCpuUsage();
      double loadAvg = clusterMember.getLoadAverage();

      memberJSON.put("cpuUsage", truncate(currentCPUUsage, 2));
      memberJSON.put("currentHeapUsage", clusterMember.getCurrentHeapSize());
      memberJSON.put("isManager", clusterMember.isManager());
      memberJSON.put("uptime", TimeUtils.convertTimeSecondsToHMS(clusterMember.getUptime()));
      memberJSON.put("loadAvg", truncate(loadAvg, 2));
      memberJSON.put("sockets", clusterMember.getTotalFileDescriptorOpen());
      memberJSON.put("threads", clusterMember.getNumThreads());

      // Number of member clients
      if (PulseController.getPulseProductSupport().equalsIgnoreCase(
          PulseConstants.PRODUCT_NAME_SQLFIRE)){
        memberJSON.put("clients", clusterMember.getNumSqlfireClients());
      }else{
        memberJSON.put("clients", clusterMember.getMemberClientsHMap().size());
      }
      memberJSON.put("queues", clusterMember.getQueueBacklog());

      memberListJson.add(memberJSON);
    }
    // cluster's Members
    responseJSON.put("members", memberListJson);
    // Send json response
    return responseJSON;
  }

  private double truncate(double value, int places) {
    return new BigDecimal(value).setScale(places, RoundingMode.HALF_UP).doubleValue();
  }
}
