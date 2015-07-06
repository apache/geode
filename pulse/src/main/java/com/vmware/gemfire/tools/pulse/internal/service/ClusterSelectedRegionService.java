/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.controllers.PulseController;
import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;
import com.vmware.gemfire.tools.pulse.internal.util.TimeUtils;

/**
 * Class ClusterSelectedRegionService
 *
 * This class contains implementations of getting Cluster's selected region details
 *
 * @author Riya Bhandekar
 * @since version 7.5 cedar  2014-03-01
 */

@Component
@Service("ClusterSelectedRegion")
@Scope("singleton")
public class ClusterSelectedRegionService implements PulseService {

  // String constants used for forming a json response
  private final String ENTRY_SIZE = "entrySize";

  // Comparator based upon regions entry count
  private static Comparator<Cluster.Member> memberCurrentHeapUsageComparator = new Comparator<Cluster.Member>() {
    @Override
    public int compare(Cluster.Member m1, Cluster.Member m2) {
      long m1HeapUsage = m1.getCurrentHeapSize();
      long m2HeapUsage = m2.getCurrentHeapSize();
      if (m1HeapUsage < m2HeapUsage) {
        return -1;
      } else if (m1HeapUsage > m2HeapUsage) {
        return 1;
      } else {
        return 0;
      }
    }
  };

  @Override
  public JSONObject execute(final HttpServletRequest request) throws Exception {

    String userName = request.getUserPrincipal().getName();
    String pulseData = request.getParameter("pulseData");
    JSONObject parameterMap = new JSONObject(pulseData);
    String selectedRegionFullPath = parameterMap.getJSONObject("ClusterSelectedRegion").getString("regionFullPath");

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      // getting cluster's Regions
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);
      responseJSON.put("selectedRegion", getSelectedRegionJson(cluster, selectedRegionFullPath));
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }

  /**
   * Create JSON for selected cluster region
   *
   * @param cluster
   * @return JSONObject Array List
   */
  private JSONObject getSelectedRegionJson(Cluster cluster, String selectedRegionFullPath) throws JSONException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    Long totalHeapSize = cluster.getTotalHeapSize();
    Long totalDiskUsage = cluster.getTotalBytesOnDisk();

    Cluster.Region reg = cluster.getClusterRegion(selectedRegionFullPath);
    if(reg != null){
      JSONObject regionJSON = new JSONObject();

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

      DecimalFormat df2 = new DecimalFormat(
          PulseConstants.DECIMAL_FORMAT_PATTERN);
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
      JSONArray memberArray = new JSONArray();
      for (Cluster.Member member : clusterMembersL) {

          JSONObject regionMember = new JSONObject();
          regionMember.put("memberId", member.getId());
          regionMember.put("name", member.getName());
          regionMember.put("host", member.getHost());

          long usedHeapSize = cluster.getUsedHeapSize();
          long currentHeap = member.getCurrentHeapSize();
          if (usedHeapSize > 0) {
            float heapUsage = ((float) currentHeap / (float) usedHeapSize) * 100;
            regionMember.put("heapUsage", Double.valueOf(df2.format(heapUsage)));
          } else {
            regionMember.put("heapUsage", 0);
          }
          Float currentCPUUsage = member.getCpuUsage();

          regionMember.put("cpuUsage", Float.valueOf(df2.format(currentCPUUsage)));
          regionMember.put("currentHeapUsage", member.getCurrentHeapSize());
          regionMember.put("isManager", member.isManager());
          regionMember.put("uptime", TimeUtils.convertTimeSecondsToHMS(member.getUptime()));

          regionMember.put("loadAvg", member.getLoadAverage());
          regionMember.put("sockets", member.getTotalFileDescriptorOpen());
          regionMember.put("threads", member.getNumThreads());

          if (PulseController.getPulseProductSupport().equalsIgnoreCase(
              PulseConstants.PRODUCT_NAME_SQLFIRE)){
            regionMember.put("clients", member.getNumSqlfireClients());
          }else{
            regionMember.put("clients", member.getMemberClientsHMap().size());
          }
          regionMember.put("queues", member.getQueueBacklog());
          memberArray.put(regionMember);
      }

      regionJSON.put("members", memberArray);
      regionJSON.put("entryCount", reg.getSystemRegionEntryCount());

      Boolean persistent = reg.getPersistentEnabled();
      if (persistent) {
        regionJSON.put("persistence", PulseService.VALUE_ON);
      } else {
        regionJSON.put("persistence", PulseService.VALUE_OFF);
      }

      Boolean isEnableOffHeapMemory = reg.isEnableOffHeapMemory();
      if (isEnableOffHeapMemory) {
        regionJSON.put("isEnableOffHeapMemory", PulseService.VALUE_ON);
      } else {
        regionJSON.put("isEnableOffHeapMemory", PulseService.VALUE_OFF);
      }

      Boolean isHDFSWriteOnly = reg.isHdfsWriteOnly();
      if (regionType.startsWith("HDFS")) {
        if (isHDFSWriteOnly) {
          regionJSON.put("isHDFSWriteOnly", PulseService.VALUE_ON);
        } else {
          regionJSON.put("isHDFSWriteOnly", PulseService.VALUE_OFF);
        }
      } else {
        regionJSON.put("isHDFSWriteOnly", PulseService.VALUE_NA);
      }

      String regCompCodec = reg.getCompressionCodec();
      if (StringUtils.isNotNullNotEmptyNotWhiteSpace(regCompCodec)) {
        regionJSON.put("compressionCodec", reg.getCompressionCodec());
      } else {
        regionJSON.put("compressionCodec", PulseService.VALUE_NA);
      }

      if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController
          .getPulseProductSupport())) {
        // Convert region path to dot separated region path
        regionJSON.put("regionPath",
            StringUtils.getTableNameFromRegionName(reg.getFullPath()));
      } else {
        regionJSON.put("regionPath", reg.getFullPath());
      }

      regionJSON
          .put(
              "memoryReadsTrend",
              new JSONArray(
                  reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_GETS_PER_SEC_TREND)));
      regionJSON
          .put(
              "memoryWritesTrend",
              new JSONArray(
                  reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_PUTS_PER_SEC_TREND)));
      regionJSON
          .put(
              "diskReadsTrend",
              new JSONArray(
                  reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_READS_PER_SEC_TREND)));
      regionJSON
          .put(
              "diskWritesTrend",
              new JSONArray(
                  reg.getRegionStatisticTrend(Cluster.Region.REGION_STAT_DISK_WRITES_PER_SEC_TREND)));
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
      JSONObject responseJSON = new JSONObject();
      responseJSON.put("errorOnRegion", "Region [" + selectedRegionFullPath
          + "] is not available");
      return responseJSON;
    }
  }
}