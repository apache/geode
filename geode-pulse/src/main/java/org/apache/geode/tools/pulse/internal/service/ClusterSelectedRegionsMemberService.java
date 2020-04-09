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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Cluster.RegionOnMember;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class ClusterSelectedRegionsMemberService
 *
 * This class contains implementations of getting Cluster's selected region's member specific
 * details for all members in that region
 *
 * @since GemFire version 7.5 cedar 2014-03-01
 */

@Component
@Service("ClusterSelectedRegionsMember")
@Scope("singleton")
public class ClusterSelectedRegionsMemberService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();
  private static final Logger logger = LogManager.getLogger();
  private final Repository repository;

  @Autowired
  public ClusterSelectedRegionsMemberService(Repository repository) {
    this.repository = repository;
  }

  // Comparator based upon regions entry count
  private static Comparator<Cluster.RegionOnMember> romEntryCountComparator = (m1, m2) -> {
    long m1EntryCount = m1.getEntryCount();
    long m2EntryCount = m2.getEntryCount();
    return Long.compare(m1EntryCount, m2EntryCount);
  };

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {
    String userName = request.getUserPrincipal().getName();
    String pulseData = request.getParameter("pulseData");
    JsonNode parameterMap = mapper.readTree(pulseData);
    String selectedRegionFullPath =
        parameterMap.get("ClusterSelectedRegionsMember").get("regionFullPath").textValue();
    logger.trace("ClusterSelectedRegionsMemberService selectedRegionFullPath = {}",
        selectedRegionFullPath);

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // getting cluster's Regions
    responseJSON.put("clusterName", cluster.getServerName());
    responseJSON.put("userName", userName);
    responseJSON.set("selectedRegionsMembers",
        getSelectedRegionsMembersJson(cluster, selectedRegionFullPath));
    // Send json response
    return responseJSON;
  }

  /**
   * Create JSON for selected cluster region's all members
   */
  private ObjectNode getSelectedRegionsMembersJson(Cluster cluster, String selectedRegionFullPath) {
    Cluster.Region reg = cluster.getClusterRegion(selectedRegionFullPath);

    if (reg != null) {
      ObjectNode regionMemberJSON = mapper.createObjectNode();
      RegionOnMember[] regionOnMembers = reg.getRegionOnMembers();

      // sort on entry count
      List<RegionOnMember> romList = Arrays.asList(regionOnMembers);
      romList.sort(romEntryCountComparator);

      for (RegionOnMember rom : romList) {
        ObjectNode memberJSON = mapper.createObjectNode();
        memberJSON.put("memberName", rom.getMemberName());
        memberJSON.put("regionFullPath", rom.getRegionFullPath());
        memberJSON.put("entryCount", rom.getEntryCount());
        memberJSON.put("entrySize", rom.getEntrySize());

        memberJSON.put("accessor", ((rom.getLocalMaxMemory() == 0) ? "True" : "False"));
        logger.trace("calling getSelectedRegionsMembersJson :: rom.getLocalMaxMemory() = {}",
            rom.getLocalMaxMemory());

        memberJSON.set("memoryReadsTrend",
            mapper.valueToTree(rom.getRegionOnMemberStatisticTrend(
                RegionOnMember.REGION_ON_MEMBER_STAT_GETS_PER_SEC_TREND)));
        logger.trace("memoryReadsTrend = {}", rom.getRegionOnMemberStatisticTrend(
            RegionOnMember.REGION_ON_MEMBER_STAT_GETS_PER_SEC_TREND).length);

        memberJSON.set("memoryWritesTrend",
            mapper.valueToTree(rom.getRegionOnMemberStatisticTrend(
                RegionOnMember.REGION_ON_MEMBER_STAT_PUTS_PER_SEC_TREND)));
        logger.trace("memoryWritesTrend = {}", rom.getRegionOnMemberStatisticTrend(
            RegionOnMember.REGION_ON_MEMBER_STAT_PUTS_PER_SEC_TREND).length);
        memberJSON.set("diskReadsTrend",
            mapper.valueToTree(rom.getRegionOnMemberStatisticTrend(
                RegionOnMember.REGION_ON_MEMBER_STAT_DISK_READS_PER_SEC_TREND)));
        logger.trace("diskReadsTrend = {}", rom.getRegionOnMemberStatisticTrend(
            RegionOnMember.REGION_ON_MEMBER_STAT_DISK_READS_PER_SEC_TREND).length);

        memberJSON.set("diskWritesTrend",
            mapper.valueToTree(rom.getRegionOnMemberStatisticTrend(
                RegionOnMember.REGION_ON_MEMBER_STAT_DISK_WRITES_PER_SEC_TREND)));
        logger.trace("diskWritesTrend = {}", rom.getRegionOnMemberStatisticTrend(
            RegionOnMember.REGION_ON_MEMBER_STAT_DISK_WRITES_PER_SEC_TREND).length);

        regionMemberJSON.set(rom.getMemberName(), memberJSON);
      }

      logger.debug("calling getSelectedRegionsMembersJson :: regionJSON = {}", regionMemberJSON);
      return regionMemberJSON;
    } else {
      ObjectNode responseJSON = mapper.createObjectNode();
      responseJSON.put("errorOnRegion", "Region [" + selectedRegionFullPath + "] is not available");
      return responseJSON;
    }
  }
}
