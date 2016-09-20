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
import org.apache.geode.tools.pulse.internal.util.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.text.DecimalFormat;

/**
 * Class MemberRegionsService
 * 
 * This class contains implementations of getting Memeber's Regions details.
 * 
 * @since GemFire version 7.5
 */

@Component
@Service("MemberRegions")
@Scope("singleton")
public class MemberRegionsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  // String constants used for forming a json response
  private final String NAME = "name";
  private final String ENTRY_SIZE = "entrySize";
  private final String DISC_STORE_NAME = "diskStoreName";
  private final String DISC_SYNCHRONOUS = "diskSynchronous";

  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    String memberName = requestDataJSON.get("MemberRegions").get("memberName").textValue();

    Cluster.Member clusterMember = cluster.getMember(StringUtils.makeCompliantName(memberName));

    if (clusterMember != null) {
      responseJSON.put("memberId", clusterMember.getId());
      responseJSON.put(this.NAME, clusterMember.getName());
      responseJSON.put("host", clusterMember.getHost());

      // member's regions
      Cluster.Region[] memberRegions = clusterMember.getMemberRegionsList();
      ArrayNode regionsListJson = mapper.createArrayNode();
      for (Cluster.Region memberRegion : memberRegions) {
        ObjectNode regionJSON = mapper.createObjectNode();
        regionJSON.put(this.NAME, memberRegion.getName());

        if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController.getPulseProductSupport())) {
          // Convert region path to dot separated region path
          regionJSON.put("fullPath", StringUtils.getTableNameFromRegionName(memberRegion.getFullPath()));
        } else {
          regionJSON.put("fullPath", memberRegion.getFullPath());
        }

        regionJSON.put("type", memberRegion.getRegionType());
        regionJSON.put("entryCount", memberRegion.getSystemRegionEntryCount());
        Long entrySize = memberRegion.getEntrySize();

        DecimalFormat form = new DecimalFormat(PulseConstants.DECIMAL_FORMAT_PATTERN_2);
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

        regionsListJson.add(regionJSON);
      }
      responseJSON.put("memberRegions", regionsListJson);

      // response
      responseJSON.put("status", "Normal");

    }

    // Send json response
    return responseJSON;
  }
}
