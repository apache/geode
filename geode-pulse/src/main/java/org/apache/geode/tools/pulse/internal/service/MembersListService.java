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

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class MembersListService
 *
 * This class contains implementations of getting list of Cluster Members.
 *
 * @since GemFire version 7.5
 */
@Component
@Service("MembersList")
@Scope("singleton")
public class MembersListService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // members list
    ArrayNode memberListJson = mapper.createArrayNode();
    Cluster.Member[] memberSet = cluster.getMembers();

    for (Cluster.Member member : memberSet) {
      ObjectNode memberJSON = mapper.createObjectNode();
      memberJSON.put("memberId", member.getId());
      memberJSON.put("name", member.getName());
      memberJSON.put("host", member.getHost());

      memberListJson.add(memberJSON);
    }

    // Response JSON
    responseJSON.set("clusterMembers", memberListJson);
    responseJSON.put("clusterName", cluster.getServerName());

    // Send json response
    return responseJSON;
  }
}
