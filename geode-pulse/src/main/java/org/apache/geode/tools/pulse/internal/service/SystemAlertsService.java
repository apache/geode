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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class SystemAlertsService
 *
 * This class contains implementations of getting system's alerts details (like errors, warnings and
 * severe errors).
 *
 * @since GemFire version 7.5
 */

@Component
@Service("SystemAlerts")
@Scope("singleton")
public class SystemAlertsService implements PulseService {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    JsonNode requestDataJSON = mapper.readTree(request.getParameter("pulseData"));
    int pageNumber = 1; // Default
    String strPageNumber = requestDataJSON.get("SystemAlerts").get("pageNumber").textValue();
    if (StringUtils.isNotBlank(strPageNumber)) {
      try {
        pageNumber = Integer.parseInt(strPageNumber);
      } catch (NumberFormatException ignored) {
      }
    }

    // cluster's Members
    responseJSON.set("systemAlerts", getAlertsJson(cluster, pageNumber));
    responseJSON.put("pageNumber", cluster.getNotificationPageNumber());
    responseJSON.put("connectedFlag", cluster.isConnectedFlag());
    responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());

    // Send json response
    return responseJSON;
  }

  /**
   * function used for getting all members details in format of JSON Object array defined under a
   * cluster
   *
   * @return JSONObject Array list
   */
  public static ObjectNode getAlertsJson(Cluster cluster, int pageNumber) {
    // getting list of all types of alerts
    Cluster.Alert[] alertsList = cluster.getAlertsList();

    // create alerts json
    ObjectNode alertsJsonObject = mapper.createObjectNode();

    if ((alertsList != null) && (alertsList.length > 0)) {
      ArrayNode errorJsonArray = mapper.createArrayNode();
      ArrayNode severeJsonArray = mapper.createArrayNode();
      ArrayNode warningJsonArray = mapper.createArrayNode();
      ArrayNode infoJsonArray = mapper.createArrayNode();

      cluster.setNotificationPageNumber(pageNumber);
      for (Cluster.Alert alert : alertsList) {
        ObjectNode objAlertJson = mapper.createObjectNode();
        objAlertJson.put("description", alert.getDescription());
        objAlertJson.put("memberName", alert.getMemberName());
        objAlertJson.put("severity", alert.getSeverity());
        objAlertJson.put("isAcknowledged", alert.isAcknowledged());
        objAlertJson.put("timestamp", alert.getTimestamp().toString());
        objAlertJson.put("iso8601Ts", alert.getIso8601Ts());
        objAlertJson.put("id", alert.getId());

        if (alert.getSeverity() == Cluster.Alert.SEVERE) {
          severeJsonArray.add(objAlertJson);
        } else if (alert.getSeverity() == Cluster.Alert.ERROR) {
          errorJsonArray.add(objAlertJson);
        } else if (alert.getSeverity() == Cluster.Alert.WARNING) {
          warningJsonArray.add(objAlertJson);
        } else {
          infoJsonArray.add(objAlertJson);
        }
      }
      alertsJsonObject.set("info", infoJsonArray);
      alertsJsonObject.set("warnings", warningJsonArray);
      alertsJsonObject.set("errors", errorJsonArray);
      alertsJsonObject.set("severe", severeJsonArray);
    }
    return alertsJsonObject;
  }
}
