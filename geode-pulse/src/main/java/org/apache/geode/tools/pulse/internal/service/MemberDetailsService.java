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

import static org.apache.geode.tools.pulse.internal.data.PulseConstants.TWO_PLACE_DECIMAL_FORMAT;
import static org.apache.geode.tools.pulse.internal.util.NameUtil.makeCompliantName;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class MemberDetailsService
 *
 * This class contains implementations of getting Memeber's Statistics.
 *
 * @since GemFire version 7.5
 */
@Component
@Service("MemberDetails")
@Scope("singleton")
public class MemberDetailsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();
  private final Repository repository;

  @Autowired
  public MemberDetailsService(Repository repository) {
    this.repository = repository;
  }

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    String userName = request.getUserPrincipal().getName();

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    String memberName = requestDataJSON.get("MemberDetails").get("memberName").textValue();

    Cluster.Member clusterMember = cluster.getMember(makeCompliantName(memberName));
    if (clusterMember != null) {
      responseJSON.put("memberId", clusterMember.getId());
      responseJSON.put("name", clusterMember.getName());
      responseJSON.put("host", clusterMember.getHost());
      responseJSON.put("clusterId", cluster.getId());
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);
      double loadAvg = clusterMember.getLoadAverage();
      responseJSON.put("loadAverage", TWO_PLACE_DECIMAL_FORMAT.format(loadAvg));
      responseJSON.put("sockets", clusterMember.getTotalFileDescriptorOpen());
      responseJSON.put("threads", clusterMember.getNumThreads());
      responseJSON.put("offHeapFreeSize", clusterMember.getOffHeapFreeSize());
      responseJSON.put("offHeapUsedSize", clusterMember.getOffHeapUsedSize());
      responseJSON.put("regionsCount", clusterMember.getMemberRegionsList().length);

      // Number of member clients
      responseJSON.put("numClients", clusterMember.getMemberClientsHMap().size());

      long diskUsageVal = clusterMember.getTotalDiskUsage();
      double diskUsage = diskUsageVal / 1024D;

      responseJSON.put("diskStorageUsed", TWO_PLACE_DECIMAL_FORMAT.format(diskUsage));

      Cluster.Alert[] alertsList = cluster.getAlertsList();

      String status = "Normal";

      for (Cluster.Alert alert : alertsList) {
        if (clusterMember.getName().equals(alert.getMemberName())) {
          if (alert.getSeverity() == Cluster.Alert.SEVERE) {
            status = "Severe";
            break;
          } else if (alert.getSeverity() == Cluster.Alert.ERROR) {
            status = "Error";
          } else if (alert.getSeverity() == Cluster.Alert.WARNING) {
            status = "Warning";
          }
        }
      }

      responseJSON.put("status", status);

    } else {
      responseJSON.put("errorOnMember", "Member [" + memberName + "] is not available");
    }

    // Send json response
    return responseJSON;
  }
}
