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

import java.text.DecimalFormat;

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
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class MemberRegionsService
 * 
 * This class contains implementations of getting Memeber's Regions details.
 * 
 * @author Sachin K
 * @since version 7.5
 */

@Component
@Service("MemberRegions")
@Scope("singleton")
public class MemberRegionsService implements PulseService {

  // String constants used for forming a json response
  private final String NAME = "name";
  private final String ENTRY_SIZE = "entrySize";
  private final String DISC_STORE_NAME = "diskStoreName";
  private final String DISC_SYNCHRONOUS = "diskSynchronous";

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {

      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      String memberName = requestDataJSON.getJSONObject("MemberRegions")
          .getString("memberName");

      Cluster.Member clusterMember = cluster.getMember(StringUtils
          .makeCompliantName(memberName));

      if (clusterMember != null) {
        responseJSON.put("memberId", clusterMember.getId());
        responseJSON.put(this.NAME, clusterMember.getName());
        responseJSON.put("host", clusterMember.getHost());

        // member's regions
        Cluster.Region[] memberRegions = clusterMember.getMemberRegionsList();
        JSONArray regionsListJson = new JSONArray();
        for (Cluster.Region memberRegion : memberRegions) {
          JSONObject regionJSON = new JSONObject();
          regionJSON.put(this.NAME, memberRegion.getName());

          if (PulseConstants.PRODUCT_NAME_SQLFIRE
              .equalsIgnoreCase(PulseController.getPulseProductSupport())) {
            // Convert region path to dot separated region path
            regionJSON.put("fullPath", StringUtils
                .getTableNameFromRegionName(memberRegion.getFullPath()));
          } else {
            regionJSON.put("fullPath", memberRegion.getFullPath());
          }

          regionJSON.put("type", memberRegion.getRegionType());
          regionJSON
              .put("entryCount", memberRegion.getSystemRegionEntryCount());
          Long entrySize = memberRegion.getEntrySize();

          DecimalFormat form = new DecimalFormat(
              PulseConstants.DECIMAL_FORMAT_PATTERN_2);
          String entrySizeInMB = form.format(entrySize / (1024f * 1024f));

          if (entrySize < 0) {
            regionJSON.put(this.ENTRY_SIZE, this.VALUE_NA);
          } else {
            regionJSON.put(this.ENTRY_SIZE, entrySizeInMB);
          }
          regionJSON.put("scope", memberRegion.getScope());
          String diskStoreName = memberRegion.getDiskStoreName();
          if (StringUtils.isNotNullNotEmptyNotWhiteSpace(diskStoreName)) {
            regionJSON.put(this.DISC_STORE_NAME, diskStoreName);
            regionJSON.put(this.DISC_SYNCHRONOUS,
                memberRegion.isDiskSynchronous());
          } else {
            regionJSON.put(this.DISC_SYNCHRONOUS, this.VALUE_NA);
            regionJSON.put(this.DISC_STORE_NAME, "");
          }
          regionJSON.put("gatewayEnabled", memberRegion.getWanEnabled());

          regionsListJson.put(regionJSON);
        }
        responseJSON.put("memberRegions", regionsListJson);

        // response
        responseJSON.put("status", "Normal");

      }

      // Send json response
      return responseJSON;

    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
