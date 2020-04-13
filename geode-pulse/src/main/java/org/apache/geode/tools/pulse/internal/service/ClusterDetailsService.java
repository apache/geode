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

import static org.apache.geode.tools.pulse.internal.data.PulseConstants.TWO_PLACE_DECIMAL_FORMAT;

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
 * Class ClusterDetailsService
 *
 * This service class has implementation for providing cluster's basic statistical data.
 *
 * @since GemFire version 7.5
 */

@Component
@Service("ClusterDetails")
@Scope("singleton")
public class ClusterDetailsService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();
  private final Repository repository;

  @Autowired
  public ClusterDetailsService(Repository repository) {
    this.repository = repository;
  }

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    String userName = request.getUserPrincipal().getName();

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    Cluster.Alert[] alertsList = cluster.getAlertsList();
    int severeAlertCount = 0;
    int errorAlertCount = 0;
    int warningAlertCount = 0;
    int infoAlertCount = 0;

    for (Cluster.Alert alertObj : alertsList) {
      if (alertObj.getSeverity() == Cluster.Alert.SEVERE) {
        severeAlertCount++;
      } else if (alertObj.getSeverity() == Cluster.Alert.ERROR) {
        errorAlertCount++;
      } else if (alertObj.getSeverity() == Cluster.Alert.WARNING) {
        warningAlertCount++;
      } else {
        infoAlertCount++;
      }
    }
    // getting basic details of Cluster
    responseJSON.put("clusterName", cluster.getServerName());
    responseJSON.put("severeAlertCount", severeAlertCount);
    responseJSON.put("errorAlertCount", errorAlertCount);
    responseJSON.put("warningAlertCount", warningAlertCount);
    responseJSON.put("infoAlertCount", infoAlertCount);

    responseJSON.put("totalMembers", cluster.getMemberCount());
    responseJSON.put("servers", cluster.getServerCount());
    responseJSON.put("clients", cluster.getClientConnectionCount());
    responseJSON.put("locators", cluster.getLocatorCount());
    responseJSON.put("totalRegions", cluster.getTotalRegionCount());
    long heapSize = cluster.getTotalHeapSize();

    Double heapS = heapSize / 1024D;
    responseJSON.put("totalHeap", TWO_PLACE_DECIMAL_FORMAT.format(heapS));
    responseJSON.put("functions", cluster.getRunningFunctionCount());
    responseJSON.put("uniqueCQs", cluster.getRegisteredCQCount());
    responseJSON.put("subscriptions", cluster.getSubscriptionCount());
    responseJSON.put("txnCommitted", cluster.getTxnCommittedCount());
    responseJSON.put("txnRollback", cluster.getTxnRollbackCount());
    responseJSON.put("userName", userName);

    return responseJSON;
  }
}
