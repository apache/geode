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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * Class ClusterWANInfoService
 * 
 * This class contains implementations of getting Cluster's WAN Informations
 * (connected clusters)
 * 
 * @since GemFire version 7.5
 */
@Component
@Service("ClusterWANInfo")
@Scope("singleton")
public class ClusterWANInfoService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // members list
    ArrayNode connectedClusterListJson = mapper.createArrayNode();

    for (Map.Entry<String, Boolean> entry : cluster.getWanInformation()
        .entrySet()) {
      ObjectNode clusterJSON = mapper.createObjectNode();
      clusterJSON.put("clusterId", entry.getKey());
      clusterJSON.put("name", entry.getKey());
      clusterJSON.put("status", entry.getValue());

      connectedClusterListJson.add(clusterJSON);
    }
    // Response JSON
    responseJSON.put("connectedClusters", connectedClusterListJson);
    // Send json response
    return responseJSON;
  }

}
