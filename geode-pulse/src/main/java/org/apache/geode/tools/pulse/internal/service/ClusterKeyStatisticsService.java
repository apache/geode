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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class ClusterKeyStatisticsService
 *
 * This class contains implementations of getting Cluster's current Reads, Writes and queries
 * details and their trends over the time.
 *
 * @since GemFire version 7.5
 */

@Component
@Service("ClusterKeyStatistics")
@Scope("singleton")
public class ClusterKeyStatisticsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();
  private final Repository repository;

  @Autowired
  public ClusterKeyStatisticsService(Repository repository) {
    this.repository = repository;
  }

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    responseJSON.set("writePerSecTrend",
        mapper.valueToTree(cluster.getStatisticTrend(Cluster.CLUSTER_STAT_WRITES_PER_SECOND)));

    responseJSON.set("readPerSecTrend",
        mapper.valueToTree(cluster.getStatisticTrend(Cluster.CLUSTER_STAT_READ_PER_SECOND)));

    responseJSON.set("queriesPerSecTrend",
        mapper.valueToTree(cluster.getStatisticTrend(Cluster.CLUSTER_STAT_QUERIES_PER_SECOND)));

    // Send json response
    return responseJSON;

  }
}
