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
 * Class ClusterGCPausesService
 *
 * This class contains implementations of getting Cluster's GC Pauses (JVM Pauses) Details and its
 * trend over the time.
 *
 * @since GemFire version 7.5
 */

@Component
@Service("ClusterJVMPauses")
@Scope("singleton")
public class ClusterGCPausesService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();
    // cluster's GC Pauses trend added to json response object

    ArrayNode pauses = mapper.createArrayNode();
    for (Object obj : cluster.getStatisticTrend(Cluster.CLUSTER_STAT_GARBAGE_COLLECTION)) {
      if (obj instanceof Number) {
        pauses.add(((Number) obj).longValue());
      }
    }
    responseJSON.put("currentGCPauses", cluster.getGarbageCollectionCount());
    responseJSON.set("gCPausesTrend", pauses);
    // Send json response
    return responseJSON;
  }
}
