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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowMetricsCommand implements GfshCommand {
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = CliStrings.SHOW_METRICS, help = CliStrings.SHOW_METRICS__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_STATISTICS},
      interceptor = "org.apache.geode.management.internal.cli.commands.ShowMetricsCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result showMetrics(
      @CliOption(key = {CliStrings.MEMBER}, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.SHOW_METRICS__MEMBER__HELP) String memberNameOrId,
      @CliOption(key = {CliStrings.SHOW_METRICS__REGION}, optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.SHOW_METRICS__REGION__HELP) String regionName,
      @CliOption(key = {CliStrings.SHOW_METRICS__FILE},
          help = CliStrings.SHOW_METRICS__FILE__HELP) String export_to_report_to,
      @CliOption(key = {CliStrings.SHOW_METRICS__CACHESERVER__PORT},
          help = CliStrings.SHOW_METRICS__CACHESERVER__PORT__HELP) String cacheServerPortString,
      @CliOption(key = {CliStrings.SHOW_METRICS__CATEGORY},
          help = CliStrings.SHOW_METRICS__CATEGORY__HELP) String[] categories) {

    Result result;
    DistributedMember member = null;
    if (memberNameOrId != null) {
      member = getMember(memberNameOrId);
    }

    if (regionName != null) {
      if (memberNameOrId != null) {
        result = ResultBuilder.buildResult(
            getRegionMetricsFromMember(regionName, member, export_to_report_to, categories));
      } else {
        result = ResultBuilder
            .buildResult(getDistributedRegionMetrics(regionName, export_to_report_to, categories));
      }
    } else if (memberNameOrId != null) {
      int cacheServerPort = -1;
      if (cacheServerPortString != null) {
        cacheServerPort = Integer.parseInt(cacheServerPortString);
      }
      result = ResultBuilder
          .buildResult(getMemberMetrics(member, export_to_report_to, categories, cacheServerPort));
    } else {
      result = ResultBuilder.buildResult(getSystemWideMetrics(export_to_report_to, categories));
    }

    return result;
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String export_to_report_to = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__FILE);
      if (export_to_report_to != null && !export_to_report_to.endsWith(".csv")) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".csv"));
      }

      String regionName = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__REGION);
      String port = parseResult.getParamValueAsString(CliStrings.SHOW_METRICS__CACHESERVER__PORT);

      if (port != null) {
        try {
          Integer.parseInt(port);
        } catch (NumberFormatException nfe) {
          return ResultBuilder.createUserErrorResult("Invalid port");
        }
      }

      if (regionName != null && port != null) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.SHOW_METRICS__CANNOT__USE__REGION__WITH__CACHESERVERPORT);
      }

      String member = parseResult.getParamValueAsString(CliStrings.MEMBER);
      if (port != null && member == null) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.SHOW_METRICS__CANNOT__USE__CACHESERVERPORT);
      }

      return ResultBuilder.createInfoResult("OK");
    }
  }

  /**
   * Gets the system wide metrics
   *
   * @return ResultData with required System wide statistics or ErrorResultData if DS MBean is not
   *         found to gather metrics
   */
  private ResultData getSystemWideMetrics(String export_to_report_to, String[] categoriesArr) {
    final InternalCache cache = getCache();
    final ManagementService managementService = ManagementService.getManagementService(cache);
    DistributedSystemMXBean dsMxBean = managementService.getDistributedSystemMXBean();
    StringBuilder csvBuilder = null;
    if (dsMxBean != null) {

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      Map<String, Boolean> categoriesMap = getSystemMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not

        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }
      metricsTable.setHeader("Cluster-wide Metrics");

      if (categoriesMap.get("cluster")) {
        writeToTableAndCsv(metricsTable, "cluster", "totalHeapSize", dsMxBean.getTotalHeapSize(),
            csvBuilder);
      }

      if (categoriesMap.get("cache")) {
        writeToTableAndCsv(metricsTable, "cache", "totalRegionEntryCount",
            dsMxBean.getTotalRegionEntryCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalRegionCount", dsMxBean.getTotalRegionCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalMissCount", dsMxBean.getTotalMissCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalHitCount", dsMxBean.getTotalHitCount(),
            csvBuilder);
      }

      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage",
            dsMxBean.getTotalDiskUsage(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, ""/* 46608 */, "diskReadsRate",
            dsMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", dsMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency",
            dsMxBean.getDiskFlushAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBackupInProgress",
            dsMxBean.getTotalBackupInProgress(), csvBuilder);
      }

      if (categoriesMap.get("query")) {
        writeToTableAndCsv(metricsTable, "query", "activeCQCount", dsMxBean.getActiveCQCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "queryRequestRate", dsMxBean.getQueryRequestRate(),
            csvBuilder);
      }
      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder.toString(),
            "Cluster wide metrics exported to {0}.", false);
      }

      return crd;
    } else {
      String errorMessage =
          CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Distributed System MBean not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }
  }

  /**
   * Gets the Cluster wide metrics for a given member
   *
   * @return ResultData with required Member statistics or ErrorResultData if MemberMbean is not
   *         found to gather metrics
   * @throws ResultDataException if building result fails
   */
  private ResultData getMemberMetrics(DistributedMember distributedMember,
      String export_to_report_to, String[] categoriesArr, int cacheServerPort)
      throws ResultDataException {
    final InternalCache cache = getCache();
    final SystemManagementService managementService =
        (SystemManagementService) ManagementService.getManagementService(cache);

    ObjectName memberMBeanName = managementService.getMemberMBeanName(distributedMember);
    MemberMXBean memberMxBean =
        managementService.getMBeanInstance(memberMBeanName, MemberMXBean.class);
    ObjectName csMxBeanName;
    CacheServerMXBean csMxBean = null;

    if (memberMxBean != null) {

      if (cacheServerPort != -1) {
        csMxBeanName =
            managementService.getCacheServerMBeanName(cacheServerPort, distributedMember);
        csMxBean = managementService.getMBeanInstance(csMxBeanName, CacheServerMXBean.class);

        if (csMxBean == null) {
          ErrorResultData erd = ResultBuilder.createErrorResultData();
          erd.addLine(CliStrings.format(CliStrings.SHOW_METRICS__CACHE__SERVER__NOT__FOUND,
              cacheServerPort, MBeanJMXAdapter.getMemberNameOrId(distributedMember)));
          return erd;
        }
      }

      JVMMetrics jvmMetrics = memberMxBean.showJVMMetrics();

      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Member Metrics");
      StringBuilder csvBuilder = null;

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      Map<String, Boolean> categoriesMap = getMemberMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not
        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }

      /*
       * Member Metrics
       */
      // member, jvm, region, serialization, communication, function, transaction, diskstore, lock,
      // eviction, distribution
      if (categoriesMap.get("member")) {
        writeToTableAndCsv(metricsTable, "member", "upTime", memberMxBean.getMemberUpTime(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cpuUsage", memberMxBean.getCpuUsage(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "currentHeapSize", memberMxBean.getCurrentHeapSize(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "maximumHeapSize", memberMxBean.getMaximumHeapSize(),
            csvBuilder);
      }
      /*
       * JVM Metrics
       */
      if (categoriesMap.get("jvm")) {
        writeToTableAndCsv(metricsTable, "jvm ", "jvmThreads ", jvmMetrics.getTotalThreads(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "fileDescriptorLimit",
            memberMxBean.getFileDescriptorLimit(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalFileDescriptorOpen",
            memberMxBean.getTotalFileDescriptorOpen(), csvBuilder);
      }
      /*
       * Member wide region metrics
       */
      if (categoriesMap.get("region")) {
        writeToTableAndCsv(metricsTable, "region ", "totalRegionCount ",
            memberMxBean.getTotalRegionCount(), csvBuilder);
        String[] regionNames = memberMxBean.listRegions();
        if (regionNames != null) {
          for (int i = 0; i < regionNames.length; i++) {
            if (i == 0) {
              writeToTableAndCsv(metricsTable, "listOfRegions", regionNames[i].substring(1),
                  csvBuilder);
            } else {
              writeToTableAndCsv(metricsTable, "", regionNames[i].substring(1), csvBuilder);
            }
          }
        }

        String[] rootRegionNames = memberMxBean.getRootRegionNames();
        if (rootRegionNames != null) {
          for (int i = 0; i < rootRegionNames.length; i++) {
            if (i == 0) {
              writeToTableAndCsv(metricsTable, "rootRegions", rootRegionNames[i], csvBuilder);
            } else {
              writeToTableAndCsv(metricsTable, "", rootRegionNames[i], csvBuilder);
            }
          }
        }
        writeToTableAndCsv(metricsTable, "", "totalRegionEntryCount",
            memberMxBean.getTotalRegionEntryCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketCount", memberMxBean.getTotalBucketCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalPrimaryBucketCount",
            memberMxBean.getTotalPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsAvgLatency", memberMxBean.getGetsAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsAvgLatency", memberMxBean.getPutsAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", memberMxBean.getCreatesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", memberMxBean.getDestroysRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllAvgLatency", memberMxBean.getPutAllAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalMissCount", memberMxBean.getTotalMissCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalHitCount", memberMxBean.getTotalHitCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", memberMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", memberMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheWriterCallsAvgLatency",
            memberMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
            memberMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalLoadsCompleted",
            memberMxBean.getTotalLoadsCompleted(), csvBuilder);
      }

      /*
       * SERIALIZATION
       */
      if (categoriesMap.get("serialization")) {
        writeToTableAndCsv(metricsTable, "serialization", "serializationRate",
            memberMxBean.getSerializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "serializationLatency",
            memberMxBean.getSerializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationRate",
            memberMxBean.getDeserializationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationLatency",
            memberMxBean.getDeserializationLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "deserializationAvgLatency",
            memberMxBean.getDeserializationAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "PDXDeserializationAvgLatency",
            memberMxBean.getPDXDeserializationAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "PDXDeserializationRate",
            memberMxBean.getPDXDeserializationRate(), csvBuilder);
      }

      /*
       * Communication Metrics
       */
      if (categoriesMap.get("communication")) {
        writeToTableAndCsv(metricsTable, "communication", "bytesSentRate",
            memberMxBean.getBytesSentRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "bytesReceivedRate",
            memberMxBean.getBytesReceivedRate(), csvBuilder);
        String[] connectedGatewayReceivers = memberMxBean.listConnectedGatewayReceivers();
        writeToTableAndCsv(metricsTable, "connectedGatewayReceivers", connectedGatewayReceivers,
            csvBuilder);

        String[] connectedGatewaySenders = memberMxBean.listConnectedGatewaySenders();
        writeToTableAndCsv(metricsTable, "connectedGatewaySenders", connectedGatewaySenders,
            csvBuilder);
      }

      /*
       * Member wide function metrics
       */
      if (categoriesMap.get("function")) {
        writeToTableAndCsv(metricsTable, "function", "numRunningFunctions",
            memberMxBean.getNumRunningFunctions(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "functionExecutionRate",
            memberMxBean.getFunctionExecutionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numRunningFunctionsHavingResults",
            memberMxBean.getNumRunningFunctionsHavingResults(), csvBuilder);
      }

      /*
       * totalTransactionsCount currentTransactionalThreadIds transactionCommitsAvgLatency
       * transactionCommittedTotalCount transactionRolledBackTotalCount transactionCommitsRate
       */
      if (categoriesMap.get("transaction")) {
        writeToTableAndCsv(metricsTable, "transaction", "totalTransactionsCount",
            memberMxBean.getTotalTransactionsCount(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "transactionCommitsAvgLatency",
            memberMxBean.getTransactionCommitsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionCommittedTotalCount",
            memberMxBean.getTransactionCommittedTotalCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionRolledBackTotalCount",
            memberMxBean.getTransactionRolledBackTotalCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "transactionCommitsRate",
            memberMxBean.getTransactionCommitsRate(), csvBuilder);
      }
      /*
       * Member wide disk metrics
       */
      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage",
            memberMxBean.getTotalDiskUsage(), csvBuilder); // deadcoded to workaround bug 46397
        writeToTableAndCsv(metricsTable, ""/* 46608 */, "diskReadsRate",
            memberMxBean.getDiskReadsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", memberMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency",
            memberMxBean.getDiskFlushAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalQueueSize",
            memberMxBean.getTotalDiskTasksWaiting(), csvBuilder); // deadcoded to workaround bug
        // 46397
        writeToTableAndCsv(metricsTable, "", "totalBackupInProgress",
            memberMxBean.getTotalBackupInProgress(), csvBuilder);
      }
      /*
       * Member wide Lock
       */
      if (categoriesMap.get("lock")) {
        writeToTableAndCsv(metricsTable, "lock", "lockWaitsInProgress",
            memberMxBean.getLockWaitsInProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalLockWaitTime",
            memberMxBean.getTotalLockWaitTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalNumberOfLockService",
            memberMxBean.getTotalNumberOfLockService(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "requestQueues", memberMxBean.getLockRequestQueues(),
            csvBuilder);
      }
      /*
       * Eviction
       */
      if (categoriesMap.get("eviction")) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
            memberMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", memberMxBean.getLruDestroyRate(),
            csvBuilder);
      }
      /*
       * Distribution
       */
      if (categoriesMap.get("distribution")) {
        writeToTableAndCsv(metricsTable, "distribution", "getInitialImagesInProgress",
            memberMxBean.getInitialImagesInProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getInitialImageTime",
            memberMxBean.getInitialImageTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getInitialImageKeysReceived",
            memberMxBean.getInitialImageKeysReceived(), csvBuilder);
      }

      /*
       * OffHeap
       */
      if (categoriesMap.get("offheap")) {
        writeToTableAndCsv(metricsTable, "offheap", "maxMemory", memberMxBean.getOffHeapMaxMemory(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "freeMemory", memberMxBean.getOffHeapFreeMemory(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "usedMemory", memberMxBean.getOffHeapUsedMemory(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "objects", memberMxBean.getOffHeapObjects(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "fragmentation",
            memberMxBean.getOffHeapFragmentation(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "compactionTime",
            memberMxBean.getOffHeapCompactionTime(), csvBuilder);
      }

      /*
       * CacheServer stats
       */
      if (csMxBean != null) {
        writeToTableAndCsv(metricsTable, "cache-server", "clientConnectionCount",
            csMxBean.getClientConnectionCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "hostnameForClients", csMxBean.getHostNameForClients(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getRequestAvgLatency",
            csMxBean.getGetRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRequestAvgLatency",
            csMxBean.getPutRequestAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalConnectionsTimedOut",
            csMxBean.getTotalConnectionsTimedOut(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "threadQueueSize", csMxBean.getPutRequestAvgLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "connectionThreads", csMxBean.getConnectionThreads(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "connectionLoad", csMxBean.getConnectionLoad(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "loadPerConnection", csMxBean.getLoadPerConnection(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "queueLoad", csMxBean.getQueueLoad(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "loadPerQueue", csMxBean.getLoadPerQueue(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getRequestRate", csMxBean.getGetRequestRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRequestRate", csMxBean.getPutRequestRate(),
            csvBuilder);

        /*
         * Notification
         */
        writeToTableAndCsv(metricsTable, "notification", "numClientNotificationRequests",
            csMxBean.getNumClientNotificationRequests(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "clientNotificationRate",
            csMxBean.getClientNotificationRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "clientNotificationAvgLatency",
            csMxBean.getClientNotificationAvgLatency(), csvBuilder);

        /*
         * Query
         */
        writeToTableAndCsv(metricsTable, "query", "activeCQCount", csMxBean.getActiveCQCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "query", "queryRequestRate",
            csMxBean.getQueryRequestRate(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "indexCount", csMxBean.getIndexCount(), csvBuilder);

        String[] indexList = csMxBean.getIndexList();
        writeToTableAndCsv(metricsTable, "index list", indexList, csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalIndexMaintenanceTime",
            csMxBean.getTotalIndexMaintenanceTime(), csvBuilder);
      }

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder != null ? csvBuilder.toString() : null,
            "Member metrics exported to {0}.", false);
      }
      return crd;

    } else {
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Member MBean for "
          + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }
  }

  /**
   * Gets the Cluster-wide metrics for a region
   *
   * @return ResultData containing the table
   * @throws ResultDataException if building result fails
   */
  private ResultData getDistributedRegionMetrics(String regionName, String export_to_report_to,
      String[] categoriesArr) throws ResultDataException {

    final InternalCache cache = getCache();
    final ManagementService managementService = ManagementService.getManagementService(cache);

    DistributedRegionMXBean regionMxBean = managementService.getDistributedRegionMXBean(regionName);

    if (regionMxBean != null) {
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Cluster-wide Region Metrics");
      StringBuilder csvBuilder = null;

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      Map<String, Boolean> categoriesMap = getSystemRegionMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not
        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }
      /*
       * General System metrics
       */
      // cluster, region, partition , diskstore, callback, eviction
      if (categoriesMap.get("cluster")) {
        writeToTableAndCsv(metricsTable, "cluster", "member count", regionMxBean.getMemberCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "region entry count",
            regionMxBean.getSystemRegionEntryCount(), csvBuilder);
      }

      if (categoriesMap.get("region")) {
        writeToTableAndCsv(metricsTable, "region", "lastModifiedTime",
            regionMxBean.getLastModifiedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lastAccessedTime", regionMxBean.getLastAccessedTime(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "missCount", regionMxBean.getMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitCount", regionMxBean.getHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitRatio", regionMxBean.getHitRatio(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", regionMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", regionMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", regionMxBean.getCreatesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", regionMxBean.getDestroyRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(),
            csvBuilder);
      }

      if (categoriesMap.get("partition")) {
        writeToTableAndCsv(metricsTable, "partition", "putLocalRate",
            regionMxBean.getPutLocalRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteRate", regionMxBean.getPutRemoteRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteLatency", regionMxBean.getPutRemoteLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteAvgLatency",
            regionMxBean.getPutRemoteAvgLatency(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "bucketCount", regionMxBean.getBucketCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "primaryBucketCount",
            regionMxBean.getPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numBucketsWithoutRedundancy",
            regionMxBean.getNumBucketsWithoutRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketSize", regionMxBean.getTotalBucketSize(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "averageBucketSize", regionMxBean.getAvgBucketSize(),
            csvBuilder);
      }
      /*
       * Disk store
       */
      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk",
            regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskReadsRate", regionMxBean.getDiskReadsRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress",
            regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(),
            csvBuilder);

      }
      /*
       * LISTENER
       */
      if (categoriesMap.get("callback")) {
        writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency",
            regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
            regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
      }

      /*
       * Eviction
       */
      if (categoriesMap.get("eviction")) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
            regionMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", regionMxBean.getLruDestroyRate(),
            csvBuilder);
      }

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder != null ? csvBuilder.toString() : null,
            "Aggregate Region Metrics exported to {0}.", false);
      }

      return crd;
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR,
          "Distributed Region MBean for " + regionName + " not found");
      erd.addLine(errorMessage);
      return erd;
    }
  }

  /**
   * Gets the metrics of region on a given member
   *
   * @return ResultData with required Region statistics or ErrorResultData if Region MBean is not
   *         found to gather metrics
   * @throws ResultDataException if building result fails
   */
  private ResultData getRegionMetricsFromMember(String regionName,
      DistributedMember distributedMember, String export_to_report_to, String[] categoriesArr)
      throws ResultDataException {

    final InternalCache cache = getCache();
    final SystemManagementService managementService =
        (SystemManagementService) ManagementService.getManagementService(cache);

    ObjectName regionMBeanName =
        managementService.getRegionMBeanName(distributedMember, regionName);
    RegionMXBean regionMxBean =
        managementService.getMBeanInstance(regionMBeanName, RegionMXBean.class);

    if (regionMxBean != null) {
      CompositeResultData crd = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = crd.addSection();
      TabularResultData metricsTable = section.addTable();
      metricsTable.setHeader("Metrics for region:" + regionName + " On Member "
          + MBeanJMXAdapter.getMemberNameOrId(distributedMember));
      StringBuilder csvBuilder = null;

      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        csvBuilder = new StringBuilder();
        csvBuilder.append("Category");
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
        csvBuilder.append(',');
        csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
        csvBuilder.append('\n');
      }

      /*
       * Region Metrics
       */
      Map<String, Boolean> categoriesMap = getRegionMetricsCategories();

      if (categoriesArr != null && categoriesArr.length != 0) {
        Set<String> categories = createSet(categoriesArr);
        Set<String> checkSet = new HashSet<>(categoriesMap.keySet());
        Set<String> userCategories = getSetDifference(categories, checkSet);

        // Checking if the categories specified by the user are valid or not
        if (userCategories.isEmpty()) {
          for (String category : checkSet) {
            categoriesMap.put(category, false);
          }
          for (String category : categories) {
            categoriesMap.put(category.toLowerCase(), true);
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append("Invalid Categories\n");

          for (String category : userCategories) {
            sb.append(category);
            sb.append('\n');
          }
          return ResultBuilder.createErrorResultData().addLine(sb.toString());
        }
      }

      if (categoriesMap.get("region")) {
        writeToTableAndCsv(metricsTable, "region", "lastModifiedTime",
            regionMxBean.getLastModifiedTime(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lastAccessedTime", regionMxBean.getLastAccessedTime(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "missCount", regionMxBean.getMissCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitCount", regionMxBean.getHitCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "hitRatio", regionMxBean.getHitRatio(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "getsRate", regionMxBean.getGetsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putsRate", regionMxBean.getPutsRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "createsRate", regionMxBean.getCreatesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "destroyRate", regionMxBean.getDestroyRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(),
            csvBuilder);
      }

      if (categoriesMap.get("partition")) {
        writeToTableAndCsv(metricsTable, "partition", "putLocalRate",
            regionMxBean.getPutLocalRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteRate", regionMxBean.getPutRemoteRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteLatency", regionMxBean.getPutRemoteLatency(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "putRemoteAvgLatency",
            regionMxBean.getPutRemoteAvgLatency(), csvBuilder);

        writeToTableAndCsv(metricsTable, "", "bucketCount", regionMxBean.getBucketCount(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "primaryBucketCount",
            regionMxBean.getPrimaryBucketCount(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "configuredRedundancy",
            regionMxBean.getConfiguredRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "actualRedundancy", regionMxBean.getActualRedundancy(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "numBucketsWithoutRedundancy",
            regionMxBean.getNumBucketsWithoutRedundancy(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalBucketSize", regionMxBean.getTotalBucketSize(),
            csvBuilder);
      }
      /*
       * Disk store
       */
      if (categoriesMap.get("diskstore")) {
        writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk",
            regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
        writeToTableAndCsv(metricsTable, "diskReadsRate", "" + regionMxBean.getDiskReadsRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(),
            csvBuilder);
        writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress",
            regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(),
            csvBuilder);
      }
      /*
       * LISTENER
       */
      if (categoriesMap.get("callback")) {
        writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency",
            regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
            regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
      }

      /*
       * Eviction
       */
      if (categoriesMap.get("eviction")) {
        writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
            regionMxBean.getLruEvictionRate(), csvBuilder);
        writeToTableAndCsv(metricsTable, "", "lruDestroyRate", regionMxBean.getLruDestroyRate(),
            csvBuilder);
      }
      if (export_to_report_to != null && !export_to_report_to.isEmpty()) {
        crd.addAsFile(export_to_report_to, csvBuilder != null ? csvBuilder.toString() : null,
            "Region Metrics exported to {0}.", false);
      }

      return crd;
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR,
          "Region MBean for " + regionName + " on member "
              + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      erd.addLine(errorMessage);
      return erd;
    }
  }

  /***
   * Writes an entry to a TabularResultData and writes a comma separated entry to a string builder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      long metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    if (csvBuilder != null) {
      csvBuilder.append(type);
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      double metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    if (csvBuilder != null) {
      csvBuilder.append(type);
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  private Set<String> createSet(String[] categories) {
    Set<String> categoriesSet = new HashSet<>();
    Collections.addAll(categoriesSet, categories);
    return categoriesSet;
  }

  private Set<String> getSetDifference(Set<String> set1, Set<String> set2) {
    Set<String> setDifference = new HashSet<>();
    for (String element : set1) {
      if (!(set2.contains(element.toLowerCase()))) {
        setDifference.add(element);
      }
    }
    return setDifference;
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String metricName,
      String[] metricValue, StringBuilder csvBuilder) {
    if (metricValue != null) {
      for (int i = 0; i < metricValue.length; i++) {
        if (i == 0) {
          writeToTableAndCsv(metricsTable, metricName, metricValue[i], csvBuilder);
        } else {
          writeToTableAndCsv(metricsTable, "", metricValue[i], csvBuilder);
        }
      }
    }
  }

  /**
   * Writes to a TabularResultData and also appends a CSV string to a String builder
   */
  private void writeToTableAndCsv(TabularResultData metricsTable, String metricName,
      String metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, "");
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    if (csvBuilder != null) {
      csvBuilder.append("");
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  /**
   * Defines and returns map of categories for Region Metrics
   *
   * @return map with categories for region metrics and display flag set to true
   */
  private Map<String, Boolean> getRegionMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<>();

    categories.put("region", true);
    categories.put("partition", true);
    categories.put("diskstore", true);
    categories.put("callback", true);
    categories.put("gatewayreceiver", true);
    categories.put("distribution", true);
    categories.put("query", true);
    categories.put("eviction", true);
    return categories;
  }

  /**
   * Defines and returns map of categories for System metrics.
   *
   * @return map with categories for system metrics and display flag set to true
   */
  private Map<String, Boolean> getSystemMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<>();
    categories.put("cluster", true);
    categories.put("cache", true);
    categories.put("diskstore", true);
    categories.put("query", true);
    return categories;
  }

  /**
   * Defines and returns map of categories for system-wide region metrics
   *
   * @return map with categories for system wide region metrics and display flag set to true
   */
  private Map<String, Boolean> getSystemRegionMetricsCategories() {
    Map<String, Boolean> categories = getRegionMetricsCategories();
    categories.put("cluster", true);
    return categories;
  }

  /**
   * Defines and returns map of categories for member metrics
   *
   * @return map with categories for member metrics and display flag set to true
   */
  private Map<String, Boolean> getMemberMetricsCategories() {
    Map<String, Boolean> categories = new HashMap<>();
    categories.put("member", true);
    categories.put("jvm", true);
    categories.put("region", true);
    categories.put("serialization", true);
    categories.put("communication", true);
    categories.put("function", true);
    categories.put("transaction", true);
    categories.put("diskstore", true);
    categories.put("lock", true);
    categories.put("eviction", true);
    categories.put("distribution", true);
    categories.put("offheap", true);
    return categories;
  }
}
