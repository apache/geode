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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * Class ClusterMembersRGraphService
 * 
 * This class contains implementations of getting List of Cluster members and
 * their details
 * 
 * @author Anchal G
 * @since version 7.5
 */
@Component
@Service("ClusterMembersRGraph")
// @Service("ClusterMembers")
@Scope("singleton")
public class ClusterMembersRGraphService implements PulseService {

  // String constants used for forming a json response
  private final String CLUSTER = "clustor";
  private final String MEMBER_COUNT = "memberCount";
  private final String ID = "id";
  private final String NAME = "name";
  private final String DATA = "data";
  private final String MEMORY_USAGE = "memoryUsage";
  private final String CPU_USAGE = "cpuUsage";
  private final String REGIONS = "regions";
  private final String HOST = "host";
  private final String PORT = "port";
  private final String CLIENTS = "clients";
  private final String GC_PAUSES = "gcPauses";
  private final String GATEWAY_SENDER = "gatewaySender";
  private final String GATEWAY_RECEIVER = "gatewayReceiver";
  private final String LOAD_AVG = "loadAvg";
  private final String SOCKETS = "sockets";
  private final String THREADS = "threads";
  private final String NUM_THREADS = "numThreads";

  private final String MEMBER_NODE_TYPE_NORMAL = "Normal";
  private final String MEMBER_NODE_TYPE_WARNING = "Warning";
  private final String MEMBER_NODE_TYPE_ERROR = "Error";
  private final String MEMBER_NODE_TYPE_SEVERE = "Severe";
  private final String CHILDREN = "children";

  // traversing the alert array list and members which have severe, error or
  // warnings
  // alerts saving them in three different arraylists
  private List<String> severeAlertList;
  private List<String> errorAlertsList;
  private List<String> warningAlertsList;

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // Reference to repository
    Repository repository = Repository.get();

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    try {
      // clucter's Members
      responseJSON.put(
          this.CLUSTER,
          getPhysicalServerJson(cluster, repository.getJmxHost(),
              repository.getJmxPort()));
      responseJSON.put(this.MEMBER_COUNT, cluster.getMemberCount());
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }

  /**
   * function used for getting all members details in format of JSON Object
   * array defined under a cluster. This function create json based on the
   * relation of physical host and members related to it.
   * 
   * @param cluster
   * @param host
   * @param port
   * @return Array list of JSON objects for required fields of members in
   *         cluster
   */
  private JSONObject getPhysicalServerJson(Cluster cluster, String host,
      String port) throws JSONException {
    Map<String, List<Cluster.Member>> physicalToMember = cluster
        .getPhysicalToMember();

    JSONObject clusterTopologyJSON = new JSONObject();

    clusterTopologyJSON.put(this.ID, cluster.getClusterId());
    clusterTopologyJSON.put(this.NAME, cluster.getClusterId());
    JSONObject data1 = new JSONObject();
    clusterTopologyJSON.put(this.DATA, data1);
    JSONArray childHostArray = new JSONArray();
    DecimalFormat df2 = new DecimalFormat(PulseConstants.DECIMAL_FORMAT_PATTERN);

    updateAlertLists(cluster);

    for (Map.Entry<String, List<Cluster.Member>> physicalToMem : physicalToMember
        .entrySet()) {
      String hostName = physicalToMem.getKey();
      Float hostCpuUsage = 0.0F;
      Long hostMemoryUsage = (long) 0;
      Double hostLoadAvg = 0.0;
      int hostNumThreads = 0;
      Long hostSockets = (long) 0;
      boolean hostSevere = false;
      boolean hostError = false;
      boolean hostWarning = false;
      String hostStatus;
      JSONObject childHostObject = new JSONObject();
      childHostObject.put(this.ID, hostName);
      childHostObject.put(this.NAME, hostName);

      JSONArray membersArray = new JSONArray();

      List<Cluster.Member> memberList = physicalToMem.getValue();
      for (Cluster.Member member : memberList) {
        JSONObject memberJSONObj = new JSONObject();

        memberJSONObj.put(this.ID, member.getId());
        memberJSONObj.put(this.NAME, member.getName());

        JSONObject memberData = new JSONObject();

        memberData.put("gemfireVersion", member.getGemfireVersion());

        Long currentHeap = member.getCurrentHeapSize();
        Long usedHeapSize = cluster.getUsedHeapSize();

        if (usedHeapSize > 0) {
          float heapUsage = (currentHeap.floatValue() / usedHeapSize
              .floatValue()) * 100;

          memberData.put(this.MEMORY_USAGE,
              Double.valueOf(df2.format(heapUsage)));
        } else
          memberData.put(this.MEMORY_USAGE, 0);

        Float currentCPUUsage = member.getCpuUsage();

        memberData.put(this.CPU_USAGE,
            Float.valueOf(df2.format(currentCPUUsage)));
        memberData.put(this.REGIONS, member.getMemberRegions().size());
        memberData.put(this.HOST, member.getHost());
        if ((member.getMemberPort() == null)
            || (member.getMemberPort().equals("")))
          memberData.put(this.PORT, "-");
        else
          memberData.put(this.PORT, member.getMemberPort());
        memberData.put(this.CLIENTS, member.getMemberClientsHMap().size());
        memberData.put(this.GC_PAUSES, member.getGarbageCollectionCount());
        memberData.put(this.NUM_THREADS, member.getNumThreads());

        // Host CPU Usage is aggregate of all members cpu usage
        // hostCpuUsage = hostCpuUsage + currentCPUUsage;
        hostCpuUsage = member.getHostCpuUsage();
        hostMemoryUsage = hostMemoryUsage + member.getCurrentHeapSize();
        hostLoadAvg = member.getLoadAverage();
        hostNumThreads = member.getNumThreads();
        hostSockets = member.getTotalFileDescriptorOpen();

        // defining the status of Member Icons for R Graph based on the alerts
        // created for that member
        String memberNodeType = "";
        // for severe alert
        if (severeAlertList.contains(member.getName())) {
          memberNodeType = getMemberNodeType(member,
              this.MEMBER_NODE_TYPE_SEVERE);
          if (!hostSevere) {
            hostSevere = true;
          }
        }
        // for error alerts
        else if (errorAlertsList.contains(member.getName())) {
          memberNodeType = getMemberNodeType(member,
              this.MEMBER_NODE_TYPE_ERROR);
          if (!hostError) {
            hostError = true;
          }
        }
        // for warning alerts
        else if (warningAlertsList.contains(member.getName())) {
          memberNodeType = getMemberNodeType(member,
              this.MEMBER_NODE_TYPE_WARNING);
          if (!hostWarning)
            hostWarning = true;
        } else {
          memberNodeType = getMemberNodeType(member,
              this.MEMBER_NODE_TYPE_NORMAL);
        }

        memberData.put("nodeType", memberNodeType);
        memberData.put("$type", memberNodeType);
        memberData.put(this.GATEWAY_SENDER, member.getGatewaySenderList()
            .size());
        if (member.getGatewayReceiver() != null) {
          memberData.put(this.GATEWAY_RECEIVER, "1");
        } else {
          memberData.put(this.GATEWAY_RECEIVER, "0");
        }
        memberJSONObj.put(this.DATA, memberData);
        memberJSONObj.put(this.CHILDREN, new JSONArray());
        membersArray.put(memberJSONObj);
      }
      JSONObject data = new JSONObject();

      data.put(this.LOAD_AVG, hostLoadAvg);
      data.put(this.SOCKETS, hostSockets);
      data.put(this.THREADS, hostNumThreads);
      data.put(this.CPU_USAGE, Double.valueOf(df2.format(hostCpuUsage)));
      data.put(this.MEMORY_USAGE, hostMemoryUsage);

      String hostNodeType;
      // setting physical host status
      if (hostSevere) {
        hostStatus = this.MEMBER_NODE_TYPE_SEVERE;
        hostNodeType = "hostSevereNode";
      } else if (hostError) {
        hostStatus = this.MEMBER_NODE_TYPE_ERROR;
        hostNodeType = "hostErrorNode";
      } else if (hostWarning) {
        hostStatus = this.MEMBER_NODE_TYPE_WARNING;
        hostNodeType = "hostWarningNode";
      } else {
        hostStatus = this.MEMBER_NODE_TYPE_NORMAL;
        hostNodeType = "hostNormalNode";
      }
      data.put("hostStatus", hostStatus);
      data.put("$type", hostNodeType);

      childHostObject.put(this.DATA, data);

      childHostObject.put(this.CHILDREN, membersArray);
      childHostArray.put(childHostObject);
    }
    clusterTopologyJSON.put(this.CHILDREN, childHostArray);

    return clusterTopologyJSON;
  }

  /**
   * used for getting member node type based on member's current state
   * 
   * @param member
   *          Member
   * @param memberState
   *          i.e Severe, Error, Warning or Normal
   * @return
   */
  private String getMemberNodeType(Cluster.Member member, String memberState) {
    String memberNodeType = "";

    if ((member.isLocator()) && (member.isServer()) && (member.isManager())) {
      memberNodeType = "memberLocatorManagerServer" + memberState + "Node";
    } else if ((member.isLocator()) && (member.isServer())
        && !(member.isManager())) {
      memberNodeType = "memberLocatorServer" + memberState + "Node";
    } else if ((member.isLocator()) && !(member.isServer())
        && (member.isManager())) {
      memberNodeType = "memberLocatorManager" + memberState + "Node";
    } else if ((member.isLocator()) && !(member.isServer())
        && !(member.isManager())) {
      memberNodeType = "memberLocator" + memberState + "Node";
    } else if (!(member.isLocator()) && (member.isServer())
        && (member.isManager())) {
      memberNodeType = "memberManagerServer" + memberState + "Node";
    } else if (!(member.isLocator()) && (member.isServer())
        && !(member.isManager())) {
      memberNodeType = "memberServer" + memberState + "Node";
    } else if (!(member.isLocator()) && !(member.isServer())
        && (member.isManager())) {
      memberNodeType = "memberManager" + memberState + "Node";
    } else if (!(member.isLocator()) && !(member.isServer())
        && !(member.isManager())) {
      memberNodeType = "member" + memberState + "Node";
    }
    return memberNodeType;
  }

  /**
   * function used for getting list of all the alerts and save the member names
   * in respective error, warning and severe alert lists
   * 
   * @param cluster
   */
  private void updateAlertLists(Cluster cluster) {

    severeAlertList = new ArrayList<String>();
    errorAlertsList = new ArrayList<String>();
    warningAlertsList = new ArrayList<String>();

    Cluster.Alert[] alertsList = cluster.getAlertsList();

    for (Cluster.Alert alert : alertsList) {
      // if alert is severe
      if (alert.getSeverity() == Cluster.Alert.SEVERE) {
        if (errorAlertsList.contains(alert.getMemberName())) {
          errorAlertsList.remove(alert.getMemberName());
        } else if (warningAlertsList.contains(alert.getMemberName())) {
          warningAlertsList.remove(alert.getMemberName());
        }
        if (!severeAlertList.contains(alert.getMemberName())) {
          severeAlertList.add(alert.getMemberName());
        }
      }
      // if alert is error
      else if (alert.getSeverity() == Cluster.Alert.ERROR) {
        if (!severeAlertList.contains(alert.getMemberName())) {
          if (warningAlertsList.contains(alert.getMemberName())) {
            warningAlertsList.remove(alert.getMemberName());
          }
          if (!errorAlertsList.contains(alert.getMemberName())) {
            errorAlertsList.add(alert.getMemberName());
          }
        }
      }
      // if alert is warning
      else if (alert.getSeverity() == Cluster.Alert.WARNING) {
        if (!severeAlertList.contains(alert.getMemberName())) {
          if (!errorAlertsList.contains(alert.getMemberName())) {
            if (!warningAlertsList.contains(alert.getMemberName())) {
              warningAlertsList.add(alert.getMemberName());
            }
          }
        }
      }
    }

  }
}
