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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.ObjectName;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
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
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.ResultDataException;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowMetricsCommand extends InternalGfshCommand {
  enum Category {
    cache,
    cacheserver,
    callback,
    cluster,
    communication,
    diskstore,
    distribution,
    eviction,
    function,
    jvm,
    lock,
    offheap,
    member,
    notification,
    partition,
    query,
    region,
    serialization,
    transaction
  }

  static final List<Category> REGION_METRIC_CATEGORIES = Arrays.asList(Category.callback,
      Category.diskstore, Category.eviction, Category.partition, Category.region);

  static final List<Category> SYSTEM_METRIC_CATEGORIES =
      Arrays.asList(Category.cache, Category.cluster, Category.diskstore, Category.query);

  static final List<Category> SYSTEM_REGION_METRIC_CATEGORIES = Arrays.asList(Category.callback,
      Category.cluster, Category.diskstore, Category.eviction, Category.partition, Category.region);

  static final List<Category> MEMBER_METRIC_CATEGORIES =
      Arrays.asList(Category.communication, Category.diskstore, Category.distribution,
          Category.eviction, Category.function, Category.jvm, Category.lock, Category.member,
          Category.offheap, Category.region, Category.serialization, Category.transaction);

  static final List<Category> MEMBER_WITH_PORT_METRIC_CATEGORIES =
      Arrays.asList(Category.cacheserver, Category.communication, Category.diskstore,
          Category.distribution, Category.eviction, Category.function, Category.jvm, Category.lock,
          Category.member, Category.notification, Category.offheap, Category.query, Category.region,
          Category.serialization, Category.transaction);

  @CliCommand(value = CliStrings.SHOW_METRICS, help = CliStrings.SHOW_METRICS__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_STATISTICS},
      interceptor = "org.apache.geode.management.internal.cli.commands.ShowMetricsInterceptor")
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
          help = CliStrings.SHOW_METRICS__CACHESERVER__PORT__HELP) Integer rawCacheServerPort,
      @CliOption(key = {CliStrings.SHOW_METRICS__CATEGORY},
          help = CliStrings.SHOW_METRICS__CATEGORY__HELP) String[] categories) {

    DistributedMember member = memberNameOrId == null ? null : getMember(memberNameOrId);
    StringBuilder csvBuilder =
        StringUtils.isEmpty(export_to_report_to) ? null : prepareCsvBuilder();

    ResultData resultData;
    if (regionName != null && memberNameOrId != null) {
      resultData = getRegionMetricsFromMember(regionName, member, export_to_report_to, categories,
          csvBuilder);
    } else if (regionName != null) {
      resultData =
          getDistributedRegionMetrics(regionName, export_to_report_to, categories, csvBuilder);
    } else if (memberNameOrId != null) {
      int cacheServerPort = rawCacheServerPort == null ? -1 : rawCacheServerPort;
      resultData =
          getMemberMetrics(member, export_to_report_to, categories, cacheServerPort, csvBuilder);
    } else {
      resultData = getSystemWideMetrics(export_to_report_to, categories, csvBuilder);
    }

    return ResultBuilder.buildResult(resultData);
  }

  /**
   * Gets the system wide metrics
   *
   * @return ResultData with required System wide statistics or ErrorResultData if DS MBean is not
   *         found to gather metrics
   */
  private ResultData getSystemWideMetrics(String export_to_report_to, String[] categoriesArr,
      StringBuilder csvBuilder) {
    final ManagementService managementService = getManagementService();
    DistributedSystemMXBean dsMxBean = managementService.getDistributedSystemMXBean();
    if (dsMxBean == null) {
      String errorMessage =
          CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Distributed System MBean not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }

    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    CompositeResultData.SectionResultData section = crd.addSection();
    TabularResultData metricsTable = section.addTable();

    Set<Category> categoriesToDisplay = ArrayUtils.isNotEmpty(categoriesArr)
        ? getCategorySet(categoriesArr) : new HashSet<>(SYSTEM_METRIC_CATEGORIES);

    metricsTable.setHeader("Cluster-wide Metrics");

    writeSystemWideMetricValues(dsMxBean, csvBuilder, metricsTable, categoriesToDisplay);
    if (StringUtils.isNotEmpty(export_to_report_to)) {
      crd.addAsFile(export_to_report_to, csvBuilder.toString(),
          "Cluster wide metrics exported to {0}.", false);
    }

    return crd;
  }

  /**
   * Gets the Cluster wide metrics for a given member
   *
   * @return ResultData with required Member statistics or ErrorResultData if MemberMbean is not
   *         found to gather metrics
   * @throws ResultDataException if building result fails
   */
  private ResultData getMemberMetrics(DistributedMember distributedMember,
      String export_to_report_to, String[] categoriesArr, int cacheServerPort,
      StringBuilder csvBuilder) throws ResultDataException {
    final SystemManagementService managementService =
        (SystemManagementService) getManagementService();

    ObjectName memberMBeanName = managementService.getMemberMBeanName(distributedMember);
    MemberMXBean memberMxBean =
        managementService.getMBeanInstance(memberMBeanName, MemberMXBean.class);
    ObjectName csMxBeanName;
    CacheServerMXBean csMxBean = null;

    if (memberMxBean == null) {
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR, "Member MBean for "
          + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      return ResultBuilder.createErrorResultData().addLine(errorMessage);
    }

    if (cacheServerPort != -1) {
      csMxBeanName = managementService.getCacheServerMBeanName(cacheServerPort, distributedMember);
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

    List<Category> fullCategories =
        csMxBean != null ? MEMBER_WITH_PORT_METRIC_CATEGORIES : MEMBER_METRIC_CATEGORIES;
    Set<Category> categoriesToDisplay = ArrayUtils.isNotEmpty(categoriesArr)
        ? getCategorySet(categoriesArr) : new HashSet<>(fullCategories);

    writeMemberMetricValues(memberMxBean, jvmMetrics, metricsTable, csvBuilder,
        categoriesToDisplay);
    if (csMxBean != null) {
      writeCacheServerMetricValues(csMxBean, metricsTable, csvBuilder, categoriesToDisplay);
    }

    if (StringUtils.isNotEmpty(export_to_report_to)) {
      crd.addAsFile(export_to_report_to, csvBuilder != null ? csvBuilder.toString() : null,
          "Member metrics exported to {0}.", false);
    }
    return crd;

  }

  /**
   * Gets the Cluster-wide metrics for a region
   *
   * @return ResultData containing the table
   * @throws ResultDataException if building result fails
   */
  private ResultData getDistributedRegionMetrics(String regionName, String export_to_report_to,
      String[] categoriesArr, StringBuilder csvBuilder) throws ResultDataException {

    final ManagementService managementService = getManagementService();

    DistributedRegionMXBean regionMxBean = managementService.getDistributedRegionMXBean(regionName);

    if (regionMxBean == null) {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR,
          "Distributed Region MBean for " + regionName + " not found");
      erd.addLine(errorMessage);
      return erd;
    }

    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    CompositeResultData.SectionResultData section = crd.addSection();
    TabularResultData metricsTable = section.addTable();
    metricsTable.setHeader("Cluster-wide Region Metrics");

    Set<Category> categoriesToDisplay = ArrayUtils.isNotEmpty(categoriesArr)
        ? getCategorySet(categoriesArr) : new HashSet<>(SYSTEM_REGION_METRIC_CATEGORIES);

    writeSystemRegionMetricValues(regionMxBean, metricsTable, csvBuilder, categoriesToDisplay);

    if (StringUtils.isNotEmpty(export_to_report_to)) {
      crd.addAsFile(export_to_report_to, csvBuilder != null ? csvBuilder.toString() : null,
          "Aggregate Region Metrics exported to {0}.", false);
    }

    return crd;
  }

  /**
   * Gets the metrics of region on a given member
   *
   * @return ResultData with required Region statistics or ErrorResultData if Region MBean is not
   *         found to gather metrics
   * @throws ResultDataException if building result fails
   */
  private ResultData getRegionMetricsFromMember(String regionName,
      DistributedMember distributedMember, String export_to_report_to, String[] categoriesArr,
      StringBuilder csvBuilder) throws ResultDataException {

    final SystemManagementService managementService =
        (SystemManagementService) getManagementService();

    ObjectName regionMBeanName =
        managementService.getRegionMBeanName(distributedMember, regionName);
    RegionMXBean regionMxBean =
        managementService.getMBeanInstance(regionMBeanName, RegionMXBean.class);

    if (regionMxBean == null) {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      String errorMessage = CliStrings.format(CliStrings.SHOW_METRICS__ERROR,
          "Region MBean for " + regionName + " on member "
              + MBeanJMXAdapter.getMemberNameOrId(distributedMember) + " not found");
      erd.addLine(errorMessage);
      return erd;
    }

    CompositeResultData crd = ResultBuilder.createCompositeResultData();
    CompositeResultData.SectionResultData section = crd.addSection();
    TabularResultData metricsTable = section.addTable();
    metricsTable.setHeader("Metrics for region:" + regionName + " On Member "
        + MBeanJMXAdapter.getMemberNameOrId(distributedMember));

    Set<Category> categoriesToDisplay = ArrayUtils.isNotEmpty(categoriesArr)
        ? getCategorySet(categoriesArr) : new HashSet<>(REGION_METRIC_CATEGORIES);

    writeRegionMetricValues(regionMxBean, metricsTable, csvBuilder, categoriesToDisplay);
    if (StringUtils.isNotEmpty(export_to_report_to)) {
      crd.addAsFile(export_to_report_to, csvBuilder != null ? csvBuilder.toString() : null,
          "Region Metrics exported to {0}.", false);
    }

    return crd;
  }

  private void writeSystemWideMetricValues(DistributedSystemMXBean dsMxBean,
      StringBuilder csvBuilder, TabularResultData metricsTable, Set<Category> categoriesToDisplay) {
    if (categoriesToDisplay.contains(Category.cluster)) {
      writeToTableAndCsv(metricsTable, "cluster", "totalHeapSize", dsMxBean.getTotalHeapSize(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.cache)) {
      writeToTableAndCsv(metricsTable, "cache", "totalRegionEntryCount",
          dsMxBean.getTotalRegionEntryCount(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalRegionCount", dsMxBean.getTotalRegionCount(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalMissCount", dsMxBean.getTotalMissCount(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalHitCount", dsMxBean.getTotalHitCount(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.diskstore)) {
      writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage", dsMxBean.getTotalDiskUsage(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskReadsRate", dsMxBean.getDiskReadsRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskWritesRate", dsMxBean.getDiskWritesRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency", dsMxBean.getDiskFlushAvgLatency(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalBackupInProgress",
          dsMxBean.getTotalBackupInProgress(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.query)) {
      writeToTableAndCsv(metricsTable, "query", "activeCQCount", dsMxBean.getActiveCQCount(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "queryRequestRate", dsMxBean.getQueryRequestRate(),
          csvBuilder);
    }
  }

  private void writeMemberMetricValues(MemberMXBean memberMxBean, JVMMetrics jvmMetrics,
      TabularResultData metricsTable, StringBuilder csvBuilder, Set<Category> categoriesToDisplay) {
    if (categoriesToDisplay.contains(Category.member)) {
      writeToTableAndCsv(metricsTable, "member", "upTime", memberMxBean.getMemberUpTime(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "cpuUsage", memberMxBean.getCpuUsage(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "currentHeapSize", memberMxBean.getCurrentHeapSize(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "maximumHeapSize", memberMxBean.getMaximumHeapSize(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.jvm)) {
      writeToTableAndCsv(metricsTable, "jvm", "jvmThreads ", jvmMetrics.getTotalThreads(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "fileDescriptorLimit",
          memberMxBean.getFileDescriptorLimit(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalFileDescriptorOpen",
          memberMxBean.getTotalFileDescriptorOpen(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.region)) {
      writeToTableAndCsv(metricsTable, "region", "totalRegionCount ",
          memberMxBean.getTotalRegionCount(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "listOfRegions",
          Arrays.stream(memberMxBean.listRegions()).map(s -> s.substring(1)).toArray(String[]::new),
          csvBuilder);

      writeToTableAndCsv(metricsTable, "", "rootRegions", memberMxBean.getRootRegionNames(),
          csvBuilder);

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
    if (categoriesToDisplay.contains(Category.serialization)) {
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
    if (categoriesToDisplay.contains(Category.communication)) {
      writeToTableAndCsv(metricsTable, "communication", "bytesSentRate",
          memberMxBean.getBytesSentRate(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "bytesReceivedRate", memberMxBean.getBytesReceivedRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "connectedGatewayReceivers",
          memberMxBean.listConnectedGatewayReceivers(), csvBuilder);

      writeToTableAndCsv(metricsTable, "", "connectedGatewaySenders",
          memberMxBean.listConnectedGatewaySenders(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.function)) {
      writeToTableAndCsv(metricsTable, "function", "numRunningFunctions",
          memberMxBean.getNumRunningFunctions(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "functionExecutionRate",
          memberMxBean.getFunctionExecutionRate(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "numRunningFunctionsHavingResults",
          memberMxBean.getNumRunningFunctionsHavingResults(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.transaction)) {
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
    if (categoriesToDisplay.contains(Category.diskstore)) {
      writeToTableAndCsv(metricsTable, "diskstore", "totalDiskUsage",
          memberMxBean.getTotalDiskUsage(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskReadsRate", memberMxBean.getDiskReadsRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskWritesRate", memberMxBean.getDiskWritesRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "flushTimeAvgLatency",
          memberMxBean.getDiskFlushAvgLatency(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalQueueSize",
          memberMxBean.getTotalDiskTasksWaiting(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalBackupInProgress",
          memberMxBean.getTotalBackupInProgress(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.lock)) {
      writeToTableAndCsv(metricsTable, "lock", "lockWaitsInProgress",
          memberMxBean.getLockWaitsInProgress(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalLockWaitTime", memberMxBean.getTotalLockWaitTime(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalNumberOfLockService",
          memberMxBean.getTotalNumberOfLockService(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "requestQueues", memberMxBean.getLockRequestQueues(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.eviction)) {
      writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
          memberMxBean.getLruEvictionRate(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "lruDestroyRate", memberMxBean.getLruDestroyRate(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.distribution)) {
      writeToTableAndCsv(metricsTable, "distribution", "getInitialImagesInProgress",
          memberMxBean.getInitialImagesInProgress(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "getInitialImageTime",
          memberMxBean.getInitialImageTime(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "getInitialImageKeysReceived",
          memberMxBean.getInitialImageKeysReceived(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.offheap)) {
      writeToTableAndCsv(metricsTable, "offheap", "maxMemory", memberMxBean.getOffHeapMaxMemory(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "freeMemory", memberMxBean.getOffHeapFreeMemory(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "usedMemory", memberMxBean.getOffHeapUsedMemory(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "objects", memberMxBean.getOffHeapObjects(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "fragmentation", memberMxBean.getOffHeapFragmentation(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "compactionTime",
          memberMxBean.getOffHeapCompactionTime(), csvBuilder);
    }
  }

  private void writeCacheServerMetricValues(CacheServerMXBean csMxBean,
      TabularResultData metricsTable, StringBuilder csvBuilder, Set<Category> categoriesToDisplay) {
    if (categoriesToDisplay.contains(Category.cacheserver)) {

      writeToTableAndCsv(metricsTable, "cacheserver", "clientConnectionCount",
          csMxBean.getClientConnectionCount(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "hostnameForClients", csMxBean.getHostNameForClients(),
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
      writeToTableAndCsv(metricsTable, "", "loadPerQueue", csMxBean.getLoadPerQueue(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "getRequestRate", csMxBean.getGetRequestRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "putRequestRate", csMxBean.getPutRequestRate(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.notification)) {
      writeToTableAndCsv(metricsTable, "notification", "numClientNotificationRequests",
          csMxBean.getNumClientNotificationRequests(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "clientNotificationRate",
          csMxBean.getClientNotificationRate(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "clientNotificationAvgLatency",
          csMxBean.getClientNotificationAvgLatency(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.query)) {
      writeToTableAndCsv(metricsTable, "query", "activeCQCount", csMxBean.getActiveCQCount(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "queryRequestRate", csMxBean.getQueryRequestRate(),
          csvBuilder);

      writeToTableAndCsv(metricsTable, "", "indexCount", csMxBean.getIndexCount(), csvBuilder);

      writeToTableAndCsv(metricsTable, "", "index list", csMxBean.getIndexList(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalIndexMaintenanceTime",
          csMxBean.getTotalIndexMaintenanceTime(), csvBuilder);
    }
  }

  private void writeSystemRegionMetricValues(DistributedRegionMXBean regionMxBean,
      TabularResultData metricsTable, StringBuilder csvBuilder, Set<Category> categoriesToDisplay) {
    if (categoriesToDisplay.contains(Category.cluster)) {
      writeToTableAndCsv(metricsTable, "cluster", "member count", regionMxBean.getMemberCount(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "region entry count",
          regionMxBean.getSystemRegionEntryCount(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.region)) {
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
      writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.partition)) {
      writeToTableAndCsv(metricsTable, "partition", "putLocalRate", regionMxBean.getPutLocalRate(),
          csvBuilder);
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
    if (categoriesToDisplay.contains(Category.diskstore)) {
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
    if (categoriesToDisplay.contains(Category.callback)) {
      writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency",
          regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
          regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.eviction)) {
      writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
          regionMxBean.getLruEvictionRate(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "lruDestroyRate", regionMxBean.getLruDestroyRate(),
          csvBuilder);
    }
  }

  private void writeRegionMetricValues(RegionMXBean regionMxBean, TabularResultData metricsTable,
      StringBuilder csvBuilder, Set<Category> categoriesToDisplay) {
    if (categoriesToDisplay.contains(Category.region)) {
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
      writeToTableAndCsv(metricsTable, "", "putAllRate", regionMxBean.getPutAllRate(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.partition)) {
      writeToTableAndCsv(metricsTable, "partition", "putLocalRate", regionMxBean.getPutLocalRate(),
          csvBuilder);
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
    if (categoriesToDisplay.contains(Category.diskstore)) {
      writeToTableAndCsv(metricsTable, "diskstore", "totalEntriesOnlyOnDisk",
          regionMxBean.getTotalEntriesOnlyOnDisk(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskReadsRate", "" + regionMxBean.getDiskReadsRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskWritesRate", regionMxBean.getDiskWritesRate(),
          csvBuilder);
      writeToTableAndCsv(metricsTable, "", "totalDiskWriteInProgress",
          regionMxBean.getTotalDiskWritesProgress(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "diskTaskWaiting", regionMxBean.getDiskTaskWaiting(),
          csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.callback)) {
      writeToTableAndCsv(metricsTable, "callback", "cacheWriterCallsAvgLatency",
          regionMxBean.getCacheWriterCallsAvgLatency(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "cacheListenerCallsAvgLatency",
          regionMxBean.getCacheListenerCallsAvgLatency(), csvBuilder);
    }
    if (categoriesToDisplay.contains(Category.eviction)) {
      writeToTableAndCsv(metricsTable, "eviction", "lruEvictionRate",
          regionMxBean.getLruEvictionRate(), csvBuilder);
      writeToTableAndCsv(metricsTable, "", "lruDestroyRate", regionMxBean.getLruDestroyRate(),
          csvBuilder);
    }
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      String metricValue, StringBuilder csvBuilder) {
    metricsTable.accumulate(CliStrings.SHOW_METRICS__TYPE__HEADER, type);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__METRIC__HEADER, metricName);
    metricsTable.accumulate(CliStrings.SHOW_METRICS__VALUE__HEADER, metricValue);

    writeToCsvIfNecessary(type, metricName, String.valueOf(metricValue), csvBuilder);
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      String[] metricValue, StringBuilder csvBuilder) {
    if (ArrayUtils.isEmpty(metricValue)) {
      return;
    }

    for (int i = 0; i < metricValue.length; i++) {
      if (i == 0) {
        writeToTableAndCsv(metricsTable, type, metricName, metricValue[i], csvBuilder);
      } else {
        writeToTableAndCsv(metricsTable, "", "", metricValue[i], csvBuilder);
      }
    }
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      long metricValue, StringBuilder csvBuilder) {
    writeToTableAndCsv(metricsTable, type, metricName, String.valueOf(metricValue), csvBuilder);
  }

  private void writeToTableAndCsv(TabularResultData metricsTable, String type, String metricName,
      double metricValue, StringBuilder csvBuilder) {
    writeToTableAndCsv(metricsTable, type, metricName, String.valueOf(metricValue), csvBuilder);
  }

  private StringBuilder prepareCsvBuilder() {
    StringBuilder csvBuilder = new StringBuilder();
    csvBuilder.append(CliStrings.SHOW_METRICS__TYPE__HEADER);
    csvBuilder.append(',');
    csvBuilder.append(CliStrings.SHOW_METRICS__METRIC__HEADER);
    csvBuilder.append(',');
    csvBuilder.append(CliStrings.SHOW_METRICS__VALUE__HEADER);
    csvBuilder.append('\n');
    return csvBuilder;
  }

  private void writeToCsvIfNecessary(String type, String metricName, String metricValue,
      StringBuilder csvBuilder) {
    if (csvBuilder != null) {
      csvBuilder.append(type);
      csvBuilder.append(',');
      csvBuilder.append(metricName);
      csvBuilder.append(',');
      csvBuilder.append(metricValue);
      csvBuilder.append('\n');
    }
  }

  private Set<Category> getCategorySet(String[] categories) {
    return Stream.of(categories).map(String::toLowerCase).map(Category::valueOf)
        .collect(Collectors.toSet());
  }
}
