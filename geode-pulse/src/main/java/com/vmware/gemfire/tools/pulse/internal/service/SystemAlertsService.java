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

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class SystemAlertsService
 * 
 * This class contains implementations of getting system's alerts details (like
 * errors, warnings and severe errors).
 * 
 * @since version 7.5
 */

@Component
@Service("SystemAlerts")
@Scope("singleton")
public class SystemAlertsService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      int pageNumber = 1; // Default
      String strPageNumber = requestDataJSON.getJSONObject("SystemAlerts")
          .getString("pageNumber");
      if (StringUtils.isNotNullNotEmptyNotWhiteSpace(strPageNumber)) {
        try {
          pageNumber = Integer.valueOf(strPageNumber);
        } catch (NumberFormatException e) {
        }
      }

      // clucter's Members
      responseJSON.put("systemAlerts", getAlertsJson(cluster, pageNumber));
      responseJSON.put("pageNumber", cluster.getNotificationPageNumber());
      responseJSON.put("connectedFlag", cluster.isConnectedFlag());
      responseJSON.put("connectedErrorMsg", cluster.getConnectionErrorMsg());

    } catch (JSONException e) {
      throw new Exception(e);
    }

    // Send json response
    return responseJSON;
  }

  /**
   * function used for getting all members details in format of JSON Object
   * array defined under a cluster
   * 
   * @param cluster
   * @return JSONObject Array list
   */
  public static JSONObject getAlertsJson(Cluster cluster, int pageNumber)
      throws JSONException {
    // getting list of all types of alerts
    Cluster.Alert[] alertsList = cluster.getAlertsList();

    // create alerts json
    JSONObject alertsJsonObject = new JSONObject();

    if ((alertsList != null) && (alertsList.length > 0)) {
      JSONArray errorJsonArray = new JSONArray();
      JSONArray severeJsonArray = new JSONArray();
      JSONArray warningJsonArray = new JSONArray();
      JSONArray infoJsonArray = new JSONArray();

      cluster.setNotificationPageNumber(pageNumber);
      for (Cluster.Alert alert : alertsList) {
        JSONObject objAlertJson = new JSONObject();
        objAlertJson.put("description", alert.getDescription());
        objAlertJson.put("memberName", alert.getMemberName());
        objAlertJson.put("severity", alert.getSeverity());
        objAlertJson.put("isAcknowledged", alert.isAcknowledged());
        objAlertJson.put("timestamp", alert.getTimestamp().toString());
        objAlertJson.put("iso8601Ts", alert.getIso8601Ts());
        objAlertJson.put("id", alert.getId());

        if (alert.getSeverity() == Cluster.Alert.SEVERE) {
          severeJsonArray.put(objAlertJson);
        } else if (alert.getSeverity() == Cluster.Alert.ERROR) {
          errorJsonArray.put(objAlertJson);
        } else if (alert.getSeverity() == Cluster.Alert.WARNING) {
          warningJsonArray.put(objAlertJson);
        } else {
          infoJsonArray.put(objAlertJson);
        }
      }
      alertsJsonObject.put("info", infoJsonArray);
      alertsJsonObject.put("warnings", warningJsonArray);
      alertsJsonObject.put("errors", errorJsonArray);
      alertsJsonObject.put("severe", severeJsonArray);
    }
    return alertsJsonObject;
  }
}
