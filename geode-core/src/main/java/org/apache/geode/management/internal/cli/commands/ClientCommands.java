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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ClientHealthStatus;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.ContinuousQueryFunction;
import org.apache.geode.management.internal.cli.functions.ContinuousQueryFunction.ClientInfo;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.CompositeResultData.SectionResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * @since GemFire 8.0
 */
public class ClientCommands implements GfshCommand {

  @CliCommand(value = CliStrings.LIST_CLIENTS, help = CliStrings.LIST_CLIENT__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_CLIENT})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listClient() {
    Result result;

    try {
      CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
      SectionResultData section = compositeResultData.addSection("section1");

      TabularResultData resultTable = section.addTable("TableForClientList");
      String headerText = "ClientList";
      resultTable = resultTable.setHeader(headerText);

      InternalCache cache = getCache();
      ManagementService service = ManagementService.getExistingManagementService(cache);
      ObjectName[] cacheServers = service.getDistributedSystemMXBean().listCacheServerObjectNames();

      if (cacheServers.length == 0) {
        return ResultBuilder.createGemFireErrorResult(
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
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.LIST_COULD_NOT_RETRIEVE_CLIENT_LIST));
      }

      String memberSeparator = ";  ";

      for (Entry<String, List<String>> pairs : clientServerMap.entrySet()) {
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
        resultTable.accumulate(CliStrings.LIST_CLIENT_COLUMN_SERVERS,
            serverListForClient.toString());
      }
      result = ResultBuilder.buildResult(compositeResultData);

    } catch (Exception e) {
      LogWrapper.getInstance()
          .warning("Error in list clients. stack trace" + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings
          .format(CliStrings.LIST_CLIENT_COULD_NOT_RETRIEVE_CLIENT_LIST_0, e.getMessage()));
    }

    LogWrapper.getInstance().info("list client result " + result);

    return result;
  }

  @CliCommand(value = CliStrings.DESCRIBE_CLIENT, help = CliStrings.DESCRIBE_CLIENT__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_CLIENT})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result describeClient(@CliOption(key = CliStrings.DESCRIBE_CLIENT__ID, mandatory = true,
      help = CliStrings.DESCRIBE_CLIENT__ID__HELP) String clientId) {
    Result result;

    if (clientId.startsWith("\"")) {
      clientId = clientId.substring(1);
    }

    if (clientId.endsWith("\"")) {
      clientId = clientId.substring(0, clientId.length() - 2);
    }

    if (clientId.endsWith("\";")) {
      clientId = clientId.substring(0, clientId.length() - 2);
    }

    try {
      CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
      SectionResultData sectionResult = compositeResultData.addSection("InfoSection");
      InternalCache cache = getCache();

      ManagementService service = ManagementService.getExistingManagementService(cache);
      ObjectName[] cacheServers = service.getDistributedSystemMXBean().listCacheServerObjectNames();
      if (cacheServers.length == 0) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_SERVER_LIST));
      }

      ClientHealthStatus clientHealthStatus = null;

      for (ObjectName objName : cacheServers) {
        CacheServerMXBean serverMbean = service.getMBeanInstance(objName, CacheServerMXBean.class);
        List<String> listOfClient =
            new ArrayList<>(Arrays.asList((String[]) serverMbean.getClientIds()));
        if (listOfClient.contains(clientId)) {
          if (clientHealthStatus == null) {
            try {
              clientHealthStatus = serverMbean.showClientStats(clientId);
              if (clientHealthStatus == null) {
                return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                    CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_STATS_FOR_CLIENT_0, clientId));
              }
            } catch (Exception eee) {
              return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                  CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_STATS_FOR_CLIENT_0_REASON_1,
                  clientId, eee.getMessage()));
            }
          }
        }
      }

      if (clientHealthStatus == null) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.DESCRIBE_CLIENT__CLIENT__ID__NOT__FOUND__0, clientId));
      }

      Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache);
      String isDurable = null;
      List<String> primaryServers = new ArrayList<>();
      List<String> secondaryServers = new ArrayList<>();

      if (dsMembers.size() > 0) {
        ContinuousQueryFunction continuousQueryFunction = new ContinuousQueryFunction();
        FunctionService.registerFunction(continuousQueryFunction);
        List<?> resultList = (List<?>) CliUtil
            .executeFunction(continuousQueryFunction, clientId, dsMembers).getResult();
        for (Object aResultList : resultList) {
          try {
            Object object = aResultList;
            if (object instanceof Throwable) {
              LogWrapper.getInstance().warning(
                  "Exception in Describe Client " + ((Throwable) object).getMessage(),
                  ((Throwable) object));
              continue;
            }

            if (object != null) {
              ClientInfo objectResult = (ClientInfo) object;
              isDurable = objectResult.isDurable;

              if (objectResult.primaryServer != null && objectResult.primaryServer.length() > 0) {
                if (primaryServers.size() == 0) {
                  primaryServers.add(objectResult.primaryServer);
                } else {
                  primaryServers.add(" ,");
                  primaryServers.add(objectResult.primaryServer);
                }
              }

              if (objectResult.secondaryServer != null
                  && objectResult.secondaryServer.length() > 0) {
                if (secondaryServers.size() == 0) {
                  secondaryServers.add(objectResult.secondaryServer);
                } else {
                  secondaryServers.add(" ,");
                  secondaryServers.add(objectResult.secondaryServer);
                }
              }
            }
          } catch (Exception e) {
            LogWrapper.getInstance().info(CliStrings.DESCRIBE_CLIENT_ERROR_FETCHING_STATS_0 + " :: "
                + CliUtil.stackTraceAsString(e));
            return ResultBuilder.createGemFireErrorResult(CliStrings
                .format(CliStrings.DESCRIBE_CLIENT_ERROR_FETCHING_STATS_0, e.getMessage()));
          }
        }

        buildTableResult(sectionResult, clientHealthStatus, isDurable, primaryServers,
            secondaryServers);
        result = ResultBuilder.buildResult(compositeResultData);
      } else {
        return ResultBuilder.createGemFireErrorResult(CliStrings.DESCRIBE_CLIENT_NO_MEMBERS);
      }
    } catch (Exception e) {
      LogWrapper.getInstance()
          .info("Error in describe clients. stack trace" + CliUtil.stackTraceAsString(e));
      result = ResultBuilder.createGemFireErrorResult(CliStrings
          .format(CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_CLIENT_0, e.getMessage()));
    }
    LogWrapper.getInstance().info("describe client result " + result);
    return result;
  }

  private void buildTableResult(SectionResultData sectionResult,
      ClientHealthStatus clientHealthStatus, String isDurable, List<String> primaryServers,
      List<String> secondaryServers) {

    StringBuilder primServers = new StringBuilder();
    for (String primaryServer : primaryServers) {
      primServers.append(primaryServer);
    }

    StringBuilder secondServers = new StringBuilder();
    for (String secondServer : secondaryServers) {
      secondServers.append(secondServer);
    }
    if (clientHealthStatus != null) {
      sectionResult.addSeparator('-');
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS, primServers);
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_SECONDARY_SERVERS, secondServers);
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU, clientHealthStatus.getCpus());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS,
          clientHealthStatus.getNumOfCacheListenerCalls());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_GETS,
          clientHealthStatus.getNumOfGets());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_MISSES,
          clientHealthStatus.getNumOfMisses());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS,
          clientHealthStatus.getNumOfPuts());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS,
          clientHealthStatus.getNumOfThreads());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME,
          clientHealthStatus.getProcessCpuTime());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE,
          clientHealthStatus.getQueueSize());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME,
          clientHealthStatus.getUpTime());
      sectionResult.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE, isDurable);
      sectionResult.addSeparator('-');

      Map<String, String> poolStats = clientHealthStatus.getPoolStats();

      if (poolStats.size() > 0) {
        for (Entry<String, String> entry : poolStats.entrySet()) {
          TabularResultData poolStatsResultTable =
              sectionResult.addTable("Pool Stats For Pool Name = " + entry.getKey());
          poolStatsResultTable.setHeader("Pool Stats For Pool Name = " + entry.getKey());
          String poolStatsStr = entry.getValue();
          String str[] = poolStatsStr.split(";");

          LogWrapper.getInstance().info("describe client clientHealthStatus min conn="
              + str[0].substring(str[0].indexOf("=") + 1));
          LogWrapper.getInstance().info("describe client clientHealthStatus max conn ="
              + str[1].substring(str[1].indexOf("=") + 1));
          LogWrapper.getInstance().info("describe client clientHealthStatus redundancy ="
              + str[2].substring(str[2].indexOf("=") + 1));
          LogWrapper.getInstance().info("describe client clientHealthStatus CQs ="
              + str[3].substring(str[3].indexOf("=") + 1));

          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_MIN_CONN,
              str[0].substring(str[0].indexOf("=") + 1));
          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_MAX_CONN,
              str[1].substring(str[1].indexOf("=") + 1));
          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_REDUNDANCY,
              str[2].substring(str[2].indexOf("=") + 1));
          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_CQs,
              str[3].substring(str[3].indexOf("=") + 1));
        }
      }
    }
  }
}
