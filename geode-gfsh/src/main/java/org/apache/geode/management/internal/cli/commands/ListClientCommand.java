/*
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
 */

package org.apache.geode.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListClientCommand extends GfshCommand {
  @CliCommand(value = CliStrings.LIST_CLIENTS, help = CliStrings.LIST_CLIENT__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_CLIENT})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listClient() throws Exception {
    ResultModel result = new ResultModel();

    TabularResultModel resultTable = result.addTable("clientList");
    String headerText = "Client List";
    resultTable.setHeader(headerText);

    ManagementService service = getManagementService();
    ObjectName[] cacheServers = service.getDistributedSystemMXBean().listCacheServerObjectNames();

    if (cacheServers.length == 0) {
      return ResultModel.createInfo(
          CliStrings.format(CliStrings.LIST_CLIENT_COULD_NOT_RETRIEVE_SERVER_LIST));
    }

    Map<String, List<String>> clientServerMap = new HashMap<>();

    for (ObjectName objName : cacheServers) {
      CacheServerMXBean serverMbean = service.getMBeanInstance(objName, CacheServerMXBean.class);
      String[] listOfClient = serverMbean.getClientIds();

      if (listOfClient == null || listOfClient.length == 0) {
        continue;
      }


      for (String clientName : listOfClient) {
        String serverDetails = "member=" + objName.getKeyProperty("member") + ",port="
            + objName.getKeyProperty("port");
        if (clientServerMap.containsKey(clientName)) {
          List<String> listServers = clientServerMap.get(clientName);
          listServers.add(serverDetails);
        } else {
          List<String> listServer = new ArrayList<>();
          listServer.add(serverDetails);
          clientServerMap.put(clientName, listServer);
        }
      }
    }

    if (clientServerMap.size() == 0) {
      return ResultModel.createInfo(
          CliStrings.format(CliStrings.LIST_COULD_NOT_RETRIEVE_CLIENT_LIST));
    }

    String memberSeparator = ";  ";

    for (Map.Entry<String, List<String>> pairs : clientServerMap.entrySet()) {
      String client = pairs.getKey();
      List<String> servers = pairs.getValue();
      StringBuilder serverListForClient = new StringBuilder();
      int serversSize = servers.size();
      int i = 0;
      for (String server : servers) {
        serverListForClient.append(server);
        if (i < serversSize - 1) {
          serverListForClient.append(memberSeparator);
        }
        i++;
      }
      resultTable.accumulate(CliStrings.LIST_CLIENT_COLUMN_Clients, client);
      resultTable.accumulate(CliStrings.LIST_CLIENT_COLUMN_SERVERS, serverListForClient.toString());
    }

    LogWrapper.getInstance(getCache()).info("list client result " + result);

    return result;
  }
}
