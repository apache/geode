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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * Class ClusterMembersRGraphService
 *
 * This class contains implementations of getting List of Cluster members and their details
 *
 * @since GemFire version 7.5
 */
@Component
@Service("ClusterMembersRGraph")
@Scope("singleton")
public class ClusterMembersRGraphService implements PulseService {

  private final ObjectMapper mapper = new ObjectMapper();

  // String constants used for forming a json response
  private static final String CLUSTER = "clustor";
  private static final String MEMBER_COUNT = "memberCount";
  private static final String ID = "id";
  private static final String NAME = "name";
  private static final String DATA = "data";
  private static final String MEMORY_USAGE = "memoryUsage";
  private static final String CPU_USAGE = "cpuUsage";
  private static final String REGIONS = "regions";
  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String CLIENTS = "clients";
  private static final String GC_PAUSES = "gcPauses";
  private static final String GATEWAY_SENDER = "gatewaySender";
  private static final String GATEWAY_RECEIVER = "gatewayReceiver";
  private static final String LOAD_AVG = "loadAvg";
  private static final String SOCKETS = "sockets";
  private static final String THREADS = "threads";
  private static final String NUM_THREADS = "numThreads";

  private static final String MEMBER_NODE_TYPE_NORMAL = "Normal";
  private static final String MEMBER_NODE_TYPE_WARNING = "Warning";
  private static final String MEMBER_NODE_TYPE_ERROR = "Error";
  private static final String MEMBER_NODE_TYPE_SEVERE = "Severe";
  private static final String CHILDREN = "children";

  // traversing the alert array list and members which have severe, error or
  // warnings
  // alerts saving them in three different arraylists
  private List<String> severeAlertList;
  private List<String> errorAlertsList;
  private List<String> warningAlertsList;

  @Override
  public ObjectNode execute(final HttpServletRequest request) throws Exception {

    // Reference to repository
    Repository repository = Repository.get();

    // get cluster object
    Cluster cluster = repository.getCluster();

    // json object to be sent as response
    ObjectNode responseJSON = mapper.createObjectNode();

    // cluster's Members
    responseJSON.set(CLUSTER,
        getPhysicalServerJson(cluster));
    responseJSON.put(MEMBER_COUNT, cluster.getMemberCount());

    // Send json response
    return responseJSON;
  }

  /**
   * function used for getting all members details in format of JSON Object array defined under a
   * cluster. This function create json based on the relation of physical host and members related
   * to it.
   *
   * @return Array list of JSON objects for required fields of members in cluster
   */
  private ObjectNode getPhysicalServerJson(Cluster cluster) {
    Map<String, List<Cluster.Member>> physicalToMember = cluster.getPhysicalToMember();

    ObjectNode clusterTopologyJSON = mapper.createObjectNode();

    clusterTopologyJSON.put(ID, cluster.getClusterId());
    clusterTopologyJSON.put(NAME, cluster.getClusterId());
    ObjectNode data1 = mapper.createObjectNode();
    clusterTopologyJSON.set(DATA, data1);
    ArrayNode childHostArray = mapper.createArrayNode();

    updateAlertLists(cluster);

    for (Map.Entry<String, List<Cluster.Member>> physicalToMem : physicalToMember.entrySet()) {
      String hostName = physicalToMem.getKey();
      double hostCpuUsage = 0.0;
      long hostMemoryUsage = 0;
      double hostLoadAvg = 0.0;
      int hostNumThreads = 0;
      long hostSockets = 0;
      boolean hostSevere = false;
      boolean hostError = false;
      boolean hostWarning = false;
      String hostStatus;
      ObjectNode childHostObject = mapper.createObjectNode();
      childHostObject.put(ID, hostName);
      childHostObject.put(NAME, hostName);

      ArrayNode membersArray = mapper.createArrayNode();

      List<Cluster.Member> memberList = physicalToMem.getValue();
      for (Cluster.Member member : memberList) {
        ObjectNode memberJSONObj = mapper.createObjectNode();

        memberJSONObj.put(ID, member.getId());
        memberJSONObj.put(NAME, member.getName());

        ObjectNode memberData = mapper.createObjectNode();

        memberData.put("gemfireVersion", member.getGemfireVersion());

        long currentHeap = member.getCurrentHeapSize();
        long usedHeapSize = cluster.getUsedHeapSize();

        if (usedHeapSize > 0) {
          double heapUsage = ((currentHeap * 1D) / usedHeapSize) * 100;

          memberData.put(MEMORY_USAGE, TWO_PLACE_DECIMAL_FORMAT.format(heapUsage));
        } else
          memberData.put(MEMORY_USAGE, 0);

        double currentCPUUsage = member.getCpuUsage();

        memberData.put(CPU_USAGE, TWO_PLACE_DECIMAL_FORMAT.format(currentCPUUsage));
        memberData.put(REGIONS, member.getMemberRegions().size());
        memberData.put(HOST, member.getHost());
        if ((member.getMemberPort() == null) || (member.getMemberPort().equals(""))) {
          memberData.put(PORT, "-");
        } else {
          memberData.put(PORT, member.getMemberPort());
        }
        memberData.put(CLIENTS, member.getMemberClientsHMap().size());
        memberData.put(GC_PAUSES, member.getGarbageCollectionCount());
        memberData.put(NUM_THREADS, member.getNumThreads());

        // Host CPU Usage is aggregate of all members cpu usage
        // hostCpuUsage = hostCpuUsage + currentCPUUsage;
        hostCpuUsage = member.getHostCpuUsage();
        hostMemoryUsage = hostMemoryUsage + member.getCurrentHeapSize();
        hostLoadAvg = member.getLoadAverage();
        hostNumThreads = member.getNumThreads();
        hostSockets = member.getTotalFileDescriptorOpen();

        // defining the status of Member Icons for R Graph based on the alerts
        // created for that member
        String memberNodeType;
        // for severe alert
        if (severeAlertList.contains(member.getName())) {
          memberNodeType = getMemberNodeType(member, MEMBER_NODE_TYPE_SEVERE);
          if (!hostSevere) {
            hostSevere = true;
          }
        } else if (errorAlertsList.contains(member.getName())) {
          // for error alerts
          memberNodeType = getMemberNodeType(member, MEMBER_NODE_TYPE_ERROR);
          if (!hostError) {
            hostError = true;
          }
        }
        // for warning alerts
        else if (warningAlertsList.contains(member.getName())) {
          memberNodeType = getMemberNodeType(member, MEMBER_NODE_TYPE_WARNING);
          if (!hostWarning) {
            hostWarning = true;
          }
        } else {
          memberNodeType = getMemberNodeType(member, MEMBER_NODE_TYPE_NORMAL);
        }

        memberData.put("nodeType", memberNodeType);
        memberData.put("$type", memberNodeType);
        memberData.put(GATEWAY_SENDER, member.getGatewaySenderList().size());
        if (member.getGatewayReceiver() != null) {
          memberData.put(GATEWAY_RECEIVER, 1);
        } else {
          memberData.put(GATEWAY_RECEIVER, 0);
        }
        memberJSONObj.set(DATA, memberData);
        memberJSONObj.set(CHILDREN, mapper.createArrayNode());
        membersArray.add(memberJSONObj);
      }
      ObjectNode data = mapper.createObjectNode();

      data.put(LOAD_AVG, TWO_PLACE_DECIMAL_FORMAT.format(hostLoadAvg));
      data.put(SOCKETS, hostSockets);
      data.put(THREADS, hostNumThreads);
      data.put(CPU_USAGE, TWO_PLACE_DECIMAL_FORMAT.format(hostCpuUsage));
      data.put(MEMORY_USAGE, hostMemoryUsage);

      String hostNodeType;
      // setting physical host status
      if (hostSevere) {
        hostStatus = MEMBER_NODE_TYPE_SEVERE;
        hostNodeType = "hostSevereNode";
      } else if (hostError) {
        hostStatus = MEMBER_NODE_TYPE_ERROR;
        hostNodeType = "hostErrorNode";
      } else if (hostWarning) {
        hostStatus = MEMBER_NODE_TYPE_WARNING;
        hostNodeType = "hostWarningNode";
      } else {
        hostStatus = MEMBER_NODE_TYPE_NORMAL;
        hostNodeType = "hostNormalNode";
      }
      data.put("hostStatus", hostStatus);
      data.put("$type", hostNodeType);

      childHostObject.set(DATA, data);

      childHostObject.set(CHILDREN, membersArray);
      childHostArray.add(childHostObject);
    }
    clusterTopologyJSON.set(CHILDREN, childHostArray);

    return clusterTopologyJSON;
  }

  /**
   * used for getting member node type based on member's current state
   *
   * @param member Member
   * @param memberState i.e Severe, Error, Warning or Normal
   */
  private String getMemberNodeType(Cluster.Member member, String memberState) {
    String memberNodeType = "";

    if ((member.isLocator()) && (member.isServer()) && (member.isManager())) {
      memberNodeType = "memberLocatorManagerServer" + memberState + "Node";
    } else if ((member.isLocator()) && (member.isServer()) && !(member.isManager())) {
      memberNodeType = "memberLocatorServer" + memberState + "Node";
    } else if ((member.isLocator()) && !(member.isServer()) && (member.isManager())) {
      memberNodeType = "memberLocatorManager" + memberState + "Node";
    } else if ((member.isLocator()) && !(member.isServer()) && !(member.isManager())) {
      memberNodeType = "memberLocator" + memberState + "Node";
    } else if (!(member.isLocator()) && (member.isServer()) && (member.isManager())) {
      memberNodeType = "memberManagerServer" + memberState + "Node";
    } else if (!(member.isLocator()) && (member.isServer()) && !(member.isManager())) {
      memberNodeType = "memberServer" + memberState + "Node";
    } else if (!(member.isLocator()) && !(member.isServer()) && (member.isManager())) {
      memberNodeType = "memberManager" + memberState + "Node";
    } else if (!(member.isLocator()) && !(member.isServer()) && !(member.isManager())) {
      memberNodeType = "member" + memberState + "Node";
    }
    return memberNodeType;
  }

  /**
   * function used for getting list of all the alerts and save the member names in respective error,
   * warning and severe alert lists
   *
   */
  private void updateAlertLists(Cluster cluster) {

    severeAlertList = new ArrayList<>();
    errorAlertsList = new ArrayList<>();
    warningAlertsList = new ArrayList<>();

    Cluster.Alert[] alertsList = cluster.getAlertsList();

    for (Cluster.Alert alert : alertsList) {
      // if alert is severe
      if (alert.getSeverity() == Cluster.Alert.SEVERE) {
        if (!errorAlertsList.remove(alert.getMemberName())) {
          warningAlertsList.remove(alert.getMemberName());
        }
        if (!severeAlertList.contains(alert.getMemberName())) {
          severeAlertList.add(alert.getMemberName());
        }
      }
      // if alert is error
      else if (alert.getSeverity() == Cluster.Alert.ERROR) {
        if (!severeAlertList.contains(alert.getMemberName())) {
          warningAlertsList.remove(alert.getMemberName());
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
