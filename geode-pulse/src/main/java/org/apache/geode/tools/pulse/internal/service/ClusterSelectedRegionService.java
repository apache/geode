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
import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;
import org.apache.geode.tools.pulse.internal.util.StringUtils;
import org.apache.geode.tools.pulse.internal.util.TimeUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Class ClusterSelectedRegionService
 *
 * This class contains implementations of getting Cluster's selected region details
 *
 * @since GemFire version 7.5 cedar  2014-03-01
 */

@Component
@Service("ClusterSelectedRegion")
@Scope("singleton")
public class ClusterSelectedRegionService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  // String constants used for forming a json response
  private final String ENTRY_SIZE = "entrySize";

  // Comparator based upon regions entry count
  private static Comparator<Cluster.Member> memberCurrentHeapUsageComparator = (m1, m2) -> {
    long m1HeapUsage = m1.getCurrentHeapSize();
    long m2HeapUsage = m2.getCurrentHeapSize();
    if (m1HeapUsage < m2HeapUsage) {
      return -1;
    } else if (m1HeapUsage > m2HeapUsage) {
      return 1;
    } else {
      return 0;
    }
  };

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    String userName = request.getUserPrincipal().getName();
    String pulseData = request.getParameter("pulseData");
    JsonNode parameterMap = mapper.readTree(pulseData);
    String selectedRegionFullPath = parameterMap.get("ClusterSelectedRegion").get("regionFullPath").textValue();

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // getting cluster's Regions
    responseJSON.put("clusterName", cluster.getServerName());
    responseJSON.put("userName", userName);
    responseJSON.put("selectedRegion", getSelectedRegionJson(cluster, selectedRegionFullPath));

    // Send json response
    return responseJSON;
  }

  /**
   * Create JSON for selected cluster region
   *
   * @param cluster
   * @return ObjectNode Array List
   */
  private ObjectNode getSelectedRegionJson(Cluster cluster, String selectedRegionFullPath) {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    Long totalHeapSize = cluster.getTotalHeapSize();
    Long totalDiskUsage = cluster.getTotalBytesOnDisk();

    Cluster.Region reg = cluster.getClusterRegion(selectedRegionFullPath);
    if (reg != null){
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

      DecimalFormat df2 = new DecimalFormat(PulseConstants.DECIMAL_FORMAT_PATTERN);
      Cluster.Member[] clusterMembersList = cluster.getMembers();

      // collect members of this region
      List<Cluster.Member> clusterMembersL = new ArrayList<Cluster.Member>();
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
      Collections.sort(clusterMembersL, memberCurrentHeapUsageComparator);

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
            regionMember.put("heapUsage", Double.valueOf(df2.format(heapUsage)));
          } else {
            regionMember.put("heapUsage", 0);
          }
          double currentCPUUsage = member.getCpuUsage();
          double loadAvg = member.getLoadAverage();

          regionMember.put("cpuUsage", Double.valueOf(df2.format(currentCPUUsage)));
          regionMember.put("currentHeapUsage", member.getCurrentHeapSize());
          regionMember.put("isManager", member.isManager());
          regionMember.put("uptime", TimeUtils.convertTimeSecondsToHMS(member.getUptime()));

          regionMember.put("loadAvg", Double.valueOf(df2.format(loadAvg)));
          regionMember.put("sockets", member.getTotalFileDescriptorOpen());
          regionMember.put("threads", member.getNumThreads());

          if (PulseController.getPulseProductSupport().equalsIgnoreCase(
              PulseConstants.PRODUCT_NAME_SQLFIRE)){
            regionMember.put("clients", member.getNumSqlfireClients());
          }else{
            regionMember.put("clients", member.getMemberClientsHMap().size());
          }
          regionMember.put("queues", member.getQueueBacklog());
          memberArray.add(regionMember);
      }

      regionJSON.put("members", memberArray);
      regionJSON.put("entryCount", reg.getSystemRegionEntryCount());

      regionJSON.put("persistence", reg.getPersistentEnabled() ? PulseService.VALUE_ON : PulseService.VALUE_OFF);

      regionJSON.put("isEnableOffHeapMemory", reg.isEnableOffHeapMemory() ? PulseService.VALUE_ON : PulseService.VALUE_OFF);

      String regCompCodec = reg.getCompressionCodec();
      if (StringUtils.isNotNullNotEmptyNotWhiteSpace(regCompCodec)) {
        regionJSON.put("compressionCodec", reg.getCompressionCodec());
      } else {
        regionJSON.put("compressionCodec", PulseService.VALUE_NA);
      }

      if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController.getPulseProductSupport())) {
        // Convert region path to dot separated region path
        regionJSON.put("regionPath", StringUtils.getTableNameFromRegionName(reg.getFullPath()));
      } else {
        regionJSON.put("regionPath", reg.getFullPath());
      }

      regionJSON.put("memoryReadsTrend",
          mapper.<JsonNode>valueToTree(reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_GETS_PER_SEC_TREND)));
      regionJSON.put("memoryWritesTrend",
          mapper.<JsonNode>valueToTree(reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_PUTS_PER_SEC_TREND)));
      regionJSON.put("diskReadsTrend",
          mapper.<JsonNode>valueToTree(reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_READS_PER_SEC_TREND)));
      regionJSON.put("diskWritesTrend",
          mapper.<JsonNode>valueToTree(reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_WRITES_PER_SEC_TREND)));

      regionJSON.put("emptyNodes", reg.getEmptyNode());
      Long entrySize = reg.getEntrySize();
      DecimalFormat form = new DecimalFormat(
          PulseConstants.DECIMAL_FORMAT_PATTERN_2);
      String entrySizeInMB = form.format(entrySize / (1024f * 1024f));
      if (entrySize < 0) {
        regionJSON.put(this.ENTRY_SIZE, PulseService.VALUE_NA);
      } else {
        regionJSON.put(this.ENTRY_SIZE, entrySizeInMB);
      }
      regionJSON.put("dataUsage", reg.getDiskUsage());
      regionJSON.put("wanEnabled", reg.getWanEnabled());
      regionJSON.put("totalDataUsage", totalDiskUsage);
      regionJSON.put("memoryUsage", entrySizeInMB);

      LOGGER.fine("calling getSelectedRegionJson :: regionJSON = " + regionJSON);
      return regionJSON;
    } else {
      ObjectNode responseJSON = mapper.createObjectNode();
      responseJSON.put("errorOnRegion", "Region [" + selectedRegionFullPath + "] is not available");
      return responseJSON;
    }
  }
}
