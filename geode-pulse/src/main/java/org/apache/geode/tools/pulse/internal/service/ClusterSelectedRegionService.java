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

import static org.apache.geode.tools.pulse.internal.data.PulseConstants.FOUR_PLACE_DECIMAL_FORMAT;
import static org.apache.geode.tools.pulse.internal.data.PulseConstants.TWO_PLACE_DECIMAL_FORMAT;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.util.TimeUtils;

/**
 * Class ClusterSelectedRegionService
 *
 * This class contains implementations of getting Cluster's selected region details
 *
 * @since GemFire version 7.5 cedar 2014-03-01
 */

@Component
@Service("ClusterSelectedRegion")
@Scope("singleton")
public class ClusterSelectedRegionService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();
  private static final Logger logger = LogManager.getLogger();

  // String constants used for forming a json response
  private static final String ENTRY_SIZE = "entrySize";
  private final Repository repository;

  @Autowired
  public ClusterSelectedRegionService(Repository repository) {
    this.repository = repository;
  }

  // Comparator based upon regions entry count
  private static Comparator<Cluster.Member> memberCurrentHeapUsageComparator = (m1, m2) -> {
    long m1HeapUsage = m1.getCurrentHeapSize();
    long m2HeapUsage = m2.getCurrentHeapSize();
    return Long.compare(m1HeapUsage, m2HeapUsage);
  };

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    String userName = request.getUserPrincipal().getName();
    String pulseData = request.getParameter("pulseData");
    JsonNode parameterMap = mapper.readTree(pulseData);
    String selectedRegionFullPath =
        parameterMap.get("ClusterSelectedRegion").get("regionFullPath").textValue();

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // getting cluster's Regions
    responseJSON.put("clusterName", cluster.getServerName());
    responseJSON.put("userName", userName);
    responseJSON.set("selectedRegion", getSelectedRegionJson(cluster, selectedRegionFullPath));

    // Send json response
    return responseJSON;
  }

  /**
   * Create JSON for selected cluster region
   *
   * @return ObjectNode Array List
   */
  private ObjectNode getSelectedRegionJson(Cluster cluster, String selectedRegionFullPath) {
    Long totalHeapSize = cluster.getTotalHeapSize();
    Long totalDiskUsage = cluster.getTotalBytesOnDisk();

    Cluster.Region reg = cluster.getClusterRegion(selectedRegionFullPath);
    if (reg != null) {
      ObjectNode regionJSON = mapper.createObjectNode();

      regionJSON.put("name", reg.getName());
      regionJSON.put("path", reg.getFullPath());
      regionJSON.put("totalMemory", totalHeapSize);
      regionJSON.put("systemRegionEntryCount", reg.getSystemRegionEntryCount());
      regionJSON.put("memberCount", reg.getMemberCount());

      final String regionType = reg.getRegionType();
      regionJSON.put("type", regionType);
      regionJSON.put("getsRate", reg.getGetsRate());
      regionJSON.put("putsRate", reg.getPutsRate());
      regionJSON.put("lruEvictionRate", reg.getLruEvictionRate());

      Cluster.Member[] clusterMembersList = cluster.getMembers();

      // collect members of this region
      List<Cluster.Member> clusterMembersL = new ArrayList<>();
      for (String memberName : reg.getMemberName()) {
        for (Cluster.Member member : clusterMembersList) {
          String name = member.getName();
          name = name.replace(":", "-");
          String id = member.getId();
          id = id.replace(":", "-");

          if ((memberName.equals(id)) || (memberName.equals(name))) {
            clusterMembersL.add(member);
          }
        }
      }

      // sort members of this region
      clusterMembersL.sort(memberCurrentHeapUsageComparator);

      // return sorted member list by heap usage
      ArrayNode memberArray = mapper.createArrayNode();
      for (Cluster.Member member : clusterMembersL) {
        ObjectNode regionMember = mapper.createObjectNode();
        regionMember.put("memberId", member.getId());
        regionMember.put("name", member.getName());
        regionMember.put("host", member.getHost());

        long usedHeapSize = cluster.getUsedHeapSize();
        long currentHeap = member.getCurrentHeapSize();
        if (usedHeapSize > 0) {
          double heapUsage = ((double) currentHeap / (double) usedHeapSize) * 100;
          regionMember.put("heapUsage", TWO_PLACE_DECIMAL_FORMAT.format(heapUsage));
        } else {
          regionMember.put("heapUsage", 0);
        }
        double currentCPUUsage = member.getCpuUsage();
        double loadAvg = member.getLoadAverage();

        regionMember.put("cpuUsage", TWO_PLACE_DECIMAL_FORMAT.format(currentCPUUsage));
        regionMember.put("currentHeapUsage", member.getCurrentHeapSize());
        regionMember.put("isManager", member.isManager());
        regionMember.put("uptime", TimeUtils.convertTimeSecondsToHMS(member.getUptime()));

        regionMember.put("loadAvg", TWO_PLACE_DECIMAL_FORMAT.format(loadAvg));
        regionMember.put("sockets", member.getTotalFileDescriptorOpen());
        regionMember.put("threads", member.getNumThreads());

        regionMember.put("clients", member.getMemberClientsHMap().size());

        regionMember.put("queues", member.getQueueBacklog());
        memberArray.add(regionMember);
      }

      regionJSON.set("members", memberArray);
      regionJSON.put("entryCount", reg.getSystemRegionEntryCount());

      regionJSON.put("persistence",
          reg.getPersistentEnabled() ? PulseService.VALUE_ON : PulseService.VALUE_OFF);

      regionJSON.put("isEnableOffHeapMemory",
          reg.isEnableOffHeapMemory() ? PulseService.VALUE_ON : PulseService.VALUE_OFF);

      String regCompCodec = reg.getCompressionCodec();
      if (StringUtils.isNotBlank(regCompCodec)) {
        regionJSON.put("compressionCodec", reg.getCompressionCodec());
      } else {
        regionJSON.put("compressionCodec", PulseService.VALUE_NA);
      }


      regionJSON.put("regionPath", reg.getFullPath());


      regionJSON.set("memoryReadsTrend", mapper.valueToTree(
          reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_GETS_PER_SEC_TREND)));
      regionJSON.set("memoryWritesTrend", mapper.valueToTree(
          reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_PUTS_PER_SEC_TREND)));
      regionJSON.set("diskReadsTrend", mapper.valueToTree(
          reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_READS_PER_SEC_TREND)));
      regionJSON.set("diskWritesTrend", mapper.valueToTree(
          reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_WRITES_PER_SEC_TREND)));

      regionJSON.put("emptyNodes", reg.getEmptyNode());
      long entrySize = reg.getEntrySize();
      String entrySizeInMB = FOUR_PLACE_DECIMAL_FORMAT.format(entrySize / (1024f * 1024f));
      if (entrySize < 0) {
        regionJSON.put(ENTRY_SIZE, PulseService.VALUE_NA);
      } else {
        regionJSON.put(ENTRY_SIZE, entrySizeInMB);
      }
      regionJSON.put("dataUsage", reg.getDiskUsage());
      regionJSON.put("wanEnabled", reg.getWanEnabled());
      regionJSON.put("totalDataUsage", totalDiskUsage);
      regionJSON.put("memoryUsage", entrySizeInMB);

      logger.debug("calling getSelectedRegionJson :: regionJSON = {}", regionJSON);
      return regionJSON;
    } else {
      ObjectNode responseJSON = mapper.createObjectNode();
      responseJSON.put("errorOnRegion", "Region [" + selectedRegionFullPath + "] is not available");
      return responseJSON;
    }
  }
}
