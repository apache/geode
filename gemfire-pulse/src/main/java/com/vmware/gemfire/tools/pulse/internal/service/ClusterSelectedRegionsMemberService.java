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

package com.vmware.gemfire.tools.pulse.internal.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.Cluster.RegionOnMember;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;

/**
 * Class ClusterSelectedRegionsMemberService
 *
 * This class contains implementations of getting Cluster's selected region's member specific details
 * for all members in that region
 *
 * @author Riya Bhandekar
 * @since version 7.5 cedar 2014-03-01
 */

@Component
@Service("ClusterSelectedRegionsMember")
@Scope("singleton")
public class ClusterSelectedRegionsMemberService implements PulseService {

  //Comparator based upon regions entry count
  private static Comparator<Cluster.RegionOnMember> romEntryCountComparator = new Comparator<Cluster.RegionOnMember>() {
   @Override
     public int compare(Cluster.RegionOnMember m1, Cluster.RegionOnMember m2) {
       long m1EntryCount = m1.getEntryCount();
       long m2EntryCount = m2.getEntryCount();
       if (m1EntryCount < m2EntryCount) {
         return -1;
       } else if (m1EntryCount > m2EntryCount) {
         return 1;
       } else {
         return 0;
       }
     }
  };

  @Override
  public JSONObject execute(final HttpServletRequest request) throws Exception {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    String userName = request.getUserPrincipal().getName();
    String pulseData = request.getParameter("pulseData");
    JSONObject parameterMap = new JSONObject(pulseData);
    String selectedRegionFullPath = parameterMap.getJSONObject("ClusterSelectedRegionsMember").getString("regionFullPath");
    LOGGER.finest("ClusterSelectedRegionsMemberService selectedRegionFullPath = " + selectedRegionFullPath);

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      // getting cluster's Regions
      responseJSON.put("clusterName", cluster.getServerName());
      responseJSON.put("userName", userName);
      responseJSON.put("selectedRegionsMembers", getSelectedRegionsMembersJson(cluster, selectedRegionFullPath));
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }

  /**
   * Create JSON for selected cluster region's all members
   */
  private JSONObject getSelectedRegionsMembersJson(Cluster cluster, String selectedRegionFullPath) throws JSONException {
    PulseLogWriter LOGGER = PulseLogWriter.getLogger();
    Cluster.Region reg = cluster.getClusterRegion(selectedRegionFullPath);
    if(reg != null){
      JSONObject regionMemberJSON = new JSONObject();
      RegionOnMember[] regionOnMembers = reg.getRegionOnMembers();

      //sort on entry count
      List<RegionOnMember> romList = Arrays.asList(regionOnMembers);
      Collections.sort(romList, romEntryCountComparator);

      for(RegionOnMember rom : romList) {

        JSONObject memberJSON = new JSONObject();
        memberJSON.put("memberName", rom.getMemberName());
        memberJSON.put("regionFullPath", rom.getRegionFullPath());
        memberJSON.put("entryCount", rom.getEntryCount());
        memberJSON.put("entrySize", rom.getEntrySize());
        memberJSON.put("accessor", ((rom.getLocalMaxMemory() == 0) ? "True" : "False"));
        LOGGER.finest("calling getSelectedRegionsMembersJson :: rom.getLocalMaxMemory() = " + rom.getLocalMaxMemory());
        memberJSON.put(
            "memoryReadsTrend", new JSONArray(rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_GETS_PER_SEC_TREND)));
        LOGGER.finest("memoryReadsTrend = " + rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_GETS_PER_SEC_TREND).length);
        memberJSON.put(
            "memoryWritesTrend", new JSONArray(rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_PUTS_PER_SEC_TREND)));
        LOGGER.finest("memoryWritesTrend = " + rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_PUTS_PER_SEC_TREND).length);
        memberJSON.put(
            "diskReadsTrend", new JSONArray(rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_DISK_READS_PER_SEC_TREND)));
        LOGGER.finest("diskReadsTrend = " + rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_DISK_READS_PER_SEC_TREND).length);
        memberJSON.put(
            "diskWritesTrend", new JSONArray(rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_DISK_WRITES_PER_SEC_TREND)));
        LOGGER.finest("diskWritesTrend = " + rom.getRegionOnMemberStatisticTrend(RegionOnMember.REGION_ON_MEMBER_STAT_DISK_WRITES_PER_SEC_TREND).length);
        regionMemberJSON.put(rom.getMemberName(), memberJSON);
      }

      LOGGER.fine("calling getSelectedRegionsMembersJson :: regionJSON = " + regionMemberJSON);
      return regionMemberJSON;
    } else {
      JSONObject responseJSON = new JSONObject();
      responseJSON.put("errorOnRegion", "Region [" + selectedRegionFullPath
          + "] is not available");
      return responseJSON;
    }
  }
}