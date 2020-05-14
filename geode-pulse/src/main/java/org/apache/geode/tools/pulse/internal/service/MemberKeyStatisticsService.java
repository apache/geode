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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class MemberKeyStatisticsService
 *
 * This class contains implementations of getting Member's CPU, Memory and Read Write details
 *
 * @since GemFire version 7.5
 */
@Component
@Service("MemberKeyStatistics")
@Scope("singleton")
public class MemberKeyStatisticsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();
  private final Repository repository;

  @Autowired
  public MemberKeyStatisticsService(Repository repository) {
    this.repository = repository;
  }

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    String memberName = requestDataJSON.get("MemberKeyStatistics").get("memberName").textValue();

    Cluster.Member clusterMember = cluster.getMember(makeCompliantName(memberName));

    if (clusterMember != null) {
      // response
      responseJSON.set("cpuUsageTrend", mapper.valueToTree(
          clusterMember.getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_CPU_USAGE_SAMPLE)));
      responseJSON.set("memoryUsageTrend", mapper.valueToTree(
          clusterMember.getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_HEAP_USAGE_SAMPLE)));
      responseJSON.set("readPerSecTrend", mapper.valueToTree(
          clusterMember.getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_GETS_PER_SECOND)));
      responseJSON.set("writePerSecTrend", mapper.valueToTree(
          clusterMember.getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_PUTS_PER_SECOND)));
    }
    // Send json response
    return responseJSON;
  }
}
