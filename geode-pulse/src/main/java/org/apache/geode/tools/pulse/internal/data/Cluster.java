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

package org.apache.geode.tools.pulse.internal.data;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.remote.JMXConnector;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class Cluster This class is the Data Model for the data used for the Pulse Web UI.
 *
 * @since GemFire version 7.0.Beta 2012-09-23
 */
public class Cluster extends Thread {
  private static final int POLL_INTERVAL = 5000;
  public static final int MAX_SAMPLE_SIZE = 180;
  public static final int ALERTS_MAX_SIZE = 1000;
  public static final int PAGE_ALERTS_MAX_SIZE = 100;

  private static final Logger logger = LogManager.getLogger();
  private final ResourceBundle resourceBundle = Repository.get().getResourceBundle();

  private String jmxUserName;
  private String serverName;
  private String port;
  private JMXConnector jmxConnector;
  private int stale = 0;
  private double loadPerSec;
  private CountDownLatch clusterHasBeenInitialized;


  // start: fields defined in System MBean
  private IClusterUpdater updater = null;
  private DataBrowser dataBrowser = null;
  private int memberCount;
  private long clientConnectionCount;
  private int locatorCount;
  private int totalRegionCount;
  private long totalHeapSize = 0L;
  private long totalRegionEntryCount;
  private int currentQueryCount;
  private long totalBytesOnDisk;
  private double diskReadsRate;
  private double diskWritesRate;
  private double writePerSec;
  private double readPerSec;
  private double queriesPerSec;
  private int avgDiskStorage;
  private int avgDiskWritesRate;
  private int runningFunctionCount;
  private long registeredCQCount;
  private int subscriptionCount;
  private int serverCount;
  private int txnCommittedCount;
  private int txnRollbackCount;
  private long usedHeapSize = 0L;
  private long garbageCollectionCount = 0L;
  private int clusterId;
  private int notificationPageNumber = 1;
  private volatile boolean connectedFlag;
  private String connectionErrorMsg = "";

  private Set<String> deletedMembers = new HashSet<>();

  private Map<String, List<Cluster.Member>> physicalToMember = new HashMap<>();

  private Map<String, Cluster.Member> membersHMap = new HashMap<>();

  private Set<String> deletedRegions = new HashSet<>();

  private Map<String, Cluster.Region> clusterRegionMap = new ConcurrentHashMap<>();

  private List<Cluster.Alert> alertsList = new ArrayList<>();

  private CircularFifoBuffer totalBytesOnDiskTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer throughoutWritesTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer throughoutReadsTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer writePerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer readPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer queriesPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer memoryUsageTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private CircularFifoBuffer garbageCollectionTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
  private long previousJVMPauseCount = 0L;

  private HashMap<String, Boolean> wanInformation = new HashMap<>();
  private Map<String, Cluster.Statement> clusterStatementMap = new ConcurrentHashMap<>();

  public static final int CLUSTER_STAT_TOTAL_BYTES_ON_DISK = 0;
  public static final int CLUSTER_STAT_THROUGHPUT_WRITES = 1;
  public static final int CLUSTER_STAT_THROUGHPUT_READS = 2;
  public static final int CLUSTER_STAT_WRITES_PER_SECOND = 3;
  public static final int CLUSTER_STAT_READ_PER_SECOND = 4;
  public static final int CLUSTER_STAT_QUERIES_PER_SECOND = 5;
  public static final int CLUSTER_STAT_MEMORY_USAGE = 6;
  public static final int CLUSTER_STAT_GARBAGE_COLLECTION = 7;

  // end: fields defined in System MBean

  // used for updating member's client data
  public static long LAST_UPDATE_TIME = 0;

  private boolean stopUpdates = false;

  public Object[] getStatisticTrend(int trendId) {

    Object[] returnArray = null;
    switch (trendId) {
      case CLUSTER_STAT_TOTAL_BYTES_ON_DISK:
        synchronized (totalBytesOnDiskTrend) {
          returnArray = totalBytesOnDiskTrend.toArray();
        }

        break;

      case CLUSTER_STAT_THROUGHPUT_READS:
        synchronized (throughoutReadsTrend) {
          returnArray = throughoutReadsTrend.toArray();
        }
        break;

      case CLUSTER_STAT_THROUGHPUT_WRITES:
        synchronized (throughoutWritesTrend) {
          returnArray = throughoutWritesTrend.toArray();
        }
        break;

      case CLUSTER_STAT_WRITES_PER_SECOND:
        synchronized (writePerSecTrend) {
          returnArray = writePerSecTrend.toArray();
        }
        break;

      case CLUSTER_STAT_READ_PER_SECOND:
        synchronized (readPerSecTrend) {
          returnArray = readPerSecTrend.toArray();
        }
        break;

      case CLUSTER_STAT_QUERIES_PER_SECOND:
        synchronized (queriesPerSecTrend) {
          returnArray = queriesPerSecTrend.toArray();
        }
        break;

      case CLUSTER_STAT_MEMORY_USAGE:
        synchronized (memoryUsageTrend) {
          returnArray = memoryUsageTrend.toArray();
        }
        break;

      case CLUSTER_STAT_GARBAGE_COLLECTION:
        synchronized (garbageCollectionTrend) {
          returnArray = garbageCollectionTrend.toArray();
        }
        break;
    }

    return returnArray;
  }

  /**
   * Member Inner Class
   *
   *
   */
  public static class Member {

    // start: fields defined in MBean
    private String gemfireVersion;
    private boolean manager;
    private int totalRegionCount;
    private String host;
    private String hostnameForClients;
    private String bindAddress;
    private long currentHeapSize;
    private long maxHeapSize;
    private int avgHeapUsage;
    private long OffHeapFreeSize;
    private long OffHeapUsedSize;
    private long totalBytesOnDisk;
    private String memberPort;

    private double cpuUsage = 0.0d;
    private double hostCpuUsage = 0.0d;
    private long uptime;
    private String name;
    private double getsRate;
    private double putsRate;
    private boolean isCache;
    private boolean isGateway;
    private boolean isLocator;
    private boolean isServer;
    private double loadAverage;
    private int numThreads;
    private long totalFileDescriptorOpen;
    private long garbageCollectionCount = 0L;
    private double throughputWrites;
    private double throughputReads;
    private long totalDiskUsage;
    private String queueBacklog;
    private String id;
    private long numSqlfireClients = 0;

    private List<String> serverGroups = new ArrayList<>();
    private List<String> redundancyZones = new ArrayList<>();

    private CircularFifoBuffer cpuUsageSamples = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer heapUsageSamples = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private HashMap<String, Cluster.Region> memberRegions = new HashMap<>();
    private HashMap<String, Cluster.Client> memberClientsHMap =
        new HashMap<>();
    private CircularFifoBuffer totalBytesOnDiskSamples = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer getsPerSecond = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer putsPerSecond = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer throughputWritesTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer throughputReadsTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer garbageCollectionSamples = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private long previousJVMPauseCount = 0L;

    private Cluster.GatewayReceiver gatewayReceiver = null;
    private List<Cluster.GatewaySender> gatewaySenderList = new ArrayList<>();
    private List<Cluster.AsyncEventQueue> asyncEventQueueList =
        new ArrayList<>();
    // end: fields defined in MBean

    public static final int MEMBER_STAT_GARBAGE_COLLECTION = 0;
    public static final int MEMBER_STAT_HEAP_USAGE_SAMPLE = 1;
    public static final int MEMBER_STAT_CPU_USAGE_SAMPLE = 2;
    public static final int MEMBER_STAT_GETS_PER_SECOND = 3;
    public static final int MEMBER_STAT_PUTS_PER_SECOND = 4;
    public static final int MEMBER_STAT_THROUGHPUT_WRITES = 5;
    public static final int MEMBER_STAT_THROUGHPUT_READS = 6;

    public Cluster.Region[] getMemberRegionsList() {
      Cluster.Region[] memberReg;
      synchronized (memberRegions) {
        memberReg = new Cluster.Region[memberRegions.size()];
        memberReg = memberRegions.values().toArray(memberReg);
      }

      return memberReg;
    }

    public Cluster.Client[] getMemberClients() {
      Cluster.Client[] memberClients;
      synchronized (memberClientsHMap) {
        memberClients = new Cluster.Client[memberClientsHMap.size()];
        memberClients = memberClientsHMap.values().toArray(memberClients);
      }

      return memberClients;
    }

    public Cluster.GatewaySender[] getMemberGatewaySenders() {
      Cluster.GatewaySender[] memberGWS;
      synchronized (gatewaySenderList) {
        memberGWS = new Cluster.GatewaySender[gatewaySenderList.size()];
        memberGWS = gatewaySenderList.toArray(memberGWS);
      }
      return memberGWS;
    }

    public Cluster.AsyncEventQueue[] getMemberAsyncEventQueueList() {
      Cluster.AsyncEventQueue[] memberAEQ;
      synchronized (asyncEventQueueList) {
        memberAEQ = new Cluster.AsyncEventQueue[asyncEventQueueList.size()];
        memberAEQ = asyncEventQueueList.toArray(memberAEQ);
      }
      return memberAEQ;
    }

    public Object[] getMemberStatisticTrend(int trendId) {
      Object[] returnArray = null;
      switch (trendId) {
        case MEMBER_STAT_GARBAGE_COLLECTION:
          synchronized (garbageCollectionSamples) {
            returnArray = garbageCollectionSamples.toArray();
          }

          break;

        case MEMBER_STAT_HEAP_USAGE_SAMPLE:
          synchronized (heapUsageSamples) {
            returnArray = heapUsageSamples.toArray();
          }
          break;

        case MEMBER_STAT_CPU_USAGE_SAMPLE:
          synchronized (cpuUsageSamples) {
            returnArray = cpuUsageSamples.toArray();
          }
          break;

        case MEMBER_STAT_GETS_PER_SECOND:
          synchronized (getsPerSecond) {
            returnArray = getsPerSecond.toArray();
          }
          break;

        case MEMBER_STAT_PUTS_PER_SECOND:
          synchronized (putsPerSecond) {
            returnArray = putsPerSecond.toArray();
          }
          break;

        case MEMBER_STAT_THROUGHPUT_WRITES:
          synchronized (throughputWritesTrend) {
            returnArray = throughputWritesTrend.toArray();
          }
          break;

        case MEMBER_STAT_THROUGHPUT_READS:
          synchronized (throughputReadsTrend) {
            returnArray = throughputReadsTrend.toArray();
          }
          break;
      }

      return returnArray;
    }

    public String getGemfireVersion() {
      return gemfireVersion;
    }

    public void setGemfireVersion(String gemfireVersion) {
      this.gemfireVersion = gemfireVersion;
    }

    public String getMemberPort() {
      return memberPort;
    }

    public void setMemberPort(String memberPort) {
      this.memberPort = memberPort;
    }

    public double getThroughputWrites() {
      return throughputWrites;
    }

    public void setThroughputWrites(double throughputWrites) {
      this.throughputWrites = throughputWrites;
    }

    public double getThroughputReads() {
      return throughputReads;
    }

    public void setThroughputReads(double throughputReads) {
      this.throughputReads = throughputReads;
    }

    public long getTotalDiskUsage() {
      return totalDiskUsage;
    }

    public void setTotalDiskUsage(long totalDiskUsage) {
      this.totalDiskUsage = totalDiskUsage;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public double getLoadAverage() {
      return loadAverage;
    }

    public void setLoadAverage(Double loadAverage) {
      this.loadAverage = loadAverage;
    }

    public String getHost() {
      return host;
    }

    public String getHostnameForClients() {
      if (StringUtils.isNotBlank(hostnameForClients))
        return hostnameForClients;
      else if (StringUtils.isNotBlank(bindAddress))
        return bindAddress;
      return null;
    }

    public long getUptime() {
      return uptime;
    }

    public String getQueueBacklog() {
      return queueBacklog;
    }

    public HashMap<String, Cluster.Region> getMemberRegions() {
      return memberRegions;
    }

    public void setMemberRegions(HashMap<String, Cluster.Region> memberRegions) {
      this.memberRegions = memberRegions;
    }

    public long getCurrentHeapSize() {
      return currentHeapSize;
    }

    public void setCurrentHeapSize(long currentHeapSize) {
      this.currentHeapSize = currentHeapSize;
    }

    public long getMaxHeapSize() {
      return maxHeapSize;
    }

    public void setMaxHeapSize(long maxHeapSize) {
      this.maxHeapSize = maxHeapSize;
    }

    public boolean isManager() {
      return manager;
    }

    public void setManager(boolean manager) {
      this.manager = manager;
    }

    public int getAvgHeapUsage() {
      return avgHeapUsage;
    }

    public void setAvgHeapUsage(int avgHeapUsage) {
      this.avgHeapUsage = avgHeapUsage;
    }

    public long getOffHeapFreeSize() {
      return OffHeapFreeSize;
    }

    public void setOffHeapFreeSize(long offHeapFreeSize) {
      OffHeapFreeSize = offHeapFreeSize;
    }

    public long getOffHeapUsedSize() {
      return OffHeapUsedSize;
    }

    public void setOffHeapUsedSize(long offHeapUsedSize) {
      OffHeapUsedSize = offHeapUsedSize;
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public void setHostnameForClients(String hostnameForClients) {
      this.hostnameForClients = hostnameForClients;
    }

    public void setBindAddress(String bindAddress) {
      this.bindAddress = bindAddress;
    }

    public void setUptime(long uptime) {
      this.uptime = uptime;
    }

    public void setQueueBacklog(String queueBacklog) {
      this.queueBacklog = queueBacklog;
    }

    public int getTotalRegionCount() {
      return totalRegionCount;
    }

    public void setTotalRegionCount(int totalRegionCount) {
      this.totalRegionCount = totalRegionCount;
    }

    public long getTotalBytesOnDisk() {
      return totalBytesOnDisk;
    }

    public void setTotalBytesOnDisk(long totalBytesOnDisk) {
      this.totalBytesOnDisk = totalBytesOnDisk;
    }

    public double getCpuUsage() {
      return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
      this.cpuUsage = cpuUsage;
    }

    public double getHostCpuUsage() {
      return hostCpuUsage;
    }

    public void setHostCpuUsage(double hostCpuUsage) {
      this.hostCpuUsage = hostCpuUsage;
    }

    public double getGetsRate() {
      return getsRate;
    }

    public void setGetsRate(double getsRate) {
      this.getsRate = getsRate;
    }

    public double getPutsRate() {
      return putsRate;
    }

    public void setPutsRate(double putsRate) {
      this.putsRate = putsRate;
    }

    public HashMap<String, Cluster.Client> getMemberClientsHMap() {
      return memberClientsHMap;
    }

    public void setMemberClientsHMap(HashMap<String, Cluster.Client> memberClientsHMap) {
      this.memberClientsHMap = memberClientsHMap;
    }

    public boolean isCache() {
      return isCache;
    }

    public void setCache(boolean isCache) {
      this.isCache = isCache;
    }

    public boolean isGateway() {
      return isGateway;
    }

    public void setGateway(boolean isGateway) {
      this.isGateway = isGateway;
    }

    public int getNumThreads() {
      return numThreads;
    }

    public void setNumThreads(int numThreads) {
      this.numThreads = numThreads;
    }

    public long getTotalFileDescriptorOpen() {
      return totalFileDescriptorOpen;
    }

    public void setTotalFileDescriptorOpen(long totalFileDescriptorOpen) {
      this.totalFileDescriptorOpen = totalFileDescriptorOpen;
    }

    public long getGarbageCollectionCount() {
      return garbageCollectionCount;
    }

    public void setGarbageCollectionCount(long garbageCollectionCount) {
      this.garbageCollectionCount = garbageCollectionCount;
    }

    public boolean isLocator() {
      return isLocator;
    }

    public void setLocator(boolean isLocator) {
      this.isLocator = isLocator;
    }

    public Cluster.GatewayReceiver getGatewayReceiver() {
      return gatewayReceiver;
    }

    public void setGatewayReceiver(Cluster.GatewayReceiver gatewayReceiver) {
      this.gatewayReceiver = gatewayReceiver;
    }

    public List<Cluster.GatewaySender> getGatewaySenderList() {
      return gatewaySenderList;
    }

    public void setGatewaySenderList(List<Cluster.GatewaySender> gatewaySenderList) {
      this.gatewaySenderList = gatewaySenderList;
    }

    public List<Cluster.AsyncEventQueue> getAsyncEventQueueList() {
      return asyncEventQueueList;
    }

    public void setAsyncEventQueueList(List<Cluster.AsyncEventQueue> asyncEventQueueList) {
      this.asyncEventQueueList = asyncEventQueueList;
    }

    public boolean isServer() {
      return isServer;
    }

    public void setServer(boolean isServer) {
      this.isServer = isServer;
    }

    public List<String> getServerGroups() {
      return serverGroups;
    }

    public void setServerGroups(List<String> serverGroups) {
      this.serverGroups = serverGroups;
    }

    public List<String> getRedundancyZones() {
      return redundancyZones;
    }

    public void setRedundancyZones(List<String> redundancyZones) {
      this.redundancyZones = redundancyZones;
    }

    public CircularFifoBuffer getCpuUsageSamples() {
      return cpuUsageSamples;
    }

    public void setCpuUsageSamples(CircularFifoBuffer cpuUsageSamples) {
      this.cpuUsageSamples = cpuUsageSamples;
    }

    public CircularFifoBuffer getHeapUsageSamples() {
      return heapUsageSamples;
    }

    public void setHeapUsageSamples(CircularFifoBuffer heapUsageSamples) {
      this.heapUsageSamples = heapUsageSamples;
    }

    public CircularFifoBuffer getTotalBytesOnDiskSamples() {
      return totalBytesOnDiskSamples;
    }

    public void setTotalBytesOnDiskSamples(CircularFifoBuffer totalBytesOnDiskSamples) {
      this.totalBytesOnDiskSamples = totalBytesOnDiskSamples;
    }

    public CircularFifoBuffer getGetsPerSecond() {
      return getsPerSecond;
    }

    public void setGetsPerSecond(CircularFifoBuffer getsPerSecond) {
      this.getsPerSecond = getsPerSecond;
    }

    public CircularFifoBuffer getPutsPerSecond() {
      return putsPerSecond;
    }

    public void setPutsPerSecond(CircularFifoBuffer putsPerSecond) {
      this.putsPerSecond = putsPerSecond;
    }

    public CircularFifoBuffer getThroughputWritesTrend() {
      return throughputWritesTrend;
    }

    public void setThroughputWritesTrend(CircularFifoBuffer throughputWritesTrend) {
      this.throughputWritesTrend = throughputWritesTrend;
    }

    public CircularFifoBuffer getThroughputReadsTrend() {
      return throughputReadsTrend;
    }

    public void setThroughputReadsTrend(CircularFifoBuffer throughputReadsTrend) {
      this.throughputReadsTrend = throughputReadsTrend;
    }

    public CircularFifoBuffer getGarbageCollectionSamples() {
      return garbageCollectionSamples;
    }

    public void setGarbageCollectionSamples(CircularFifoBuffer garbageCollectionSamples) {
      this.garbageCollectionSamples = garbageCollectionSamples;
    }

    public long getPreviousJVMPauseCount() {
      return previousJVMPauseCount;
    }

    public void setPreviousJVMPauseCount(long previousJVMPauseCount) {
      this.previousJVMPauseCount = previousJVMPauseCount;
    }

    public long getNumSqlfireClients() {
      return numSqlfireClients;
    }

    public void setNumSqlfireClients(long numSqlfireClients) {
      this.numSqlfireClients = numSqlfireClients;
    }

    public void updateMemberClientsHMap(HashMap<String, Cluster.Client> memberClientsHM) {
      if (Cluster.LAST_UPDATE_TIME == 0) {
        Cluster.LAST_UPDATE_TIME = System.nanoTime();
      }

      long systemNanoTime = System.nanoTime();

      for (Map.Entry<String, Cluster.Client> entry : memberClientsHM.entrySet()) {
        String clientId = entry.getKey();
        Cluster.Client client = entry.getValue();

        if (memberClientsHMap.get(clientId) != null) {
          Client existingClient = memberClientsHMap.get(clientId);
          Client updatedClient = memberClientsHM.get(clientId);

          existingClient.setConnected(updatedClient.isConnected());
          existingClient.setGets(updatedClient.getGets());
          existingClient.setPuts(updatedClient.getPuts());
          existingClient.setCpus(updatedClient.getCpus());
          existingClient.setQueueSize(updatedClient.getQueueSize());
          existingClient.setStatus(updatedClient.getStatus());
          existingClient.setThreads(updatedClient.getThreads());
          existingClient.setClientCQCount(updatedClient.getClientCQCount());
          existingClient.setSubscriptionEnabled(updatedClient.isSubscriptionEnabled());
          long elapsedTime = updatedClient.getUptime() - existingClient.getUptime();
          existingClient.setUptime(updatedClient.getUptime());

          // set cpu usage
          long currCPUTime = updatedClient.getProcessCpuTime();
          long lastCPUTime = existingClient.getProcessCpuTime();

          double newCPUTime = 0;
          if (elapsedTime > 0) {
            newCPUTime = (double) (((currCPUTime - lastCPUTime) / elapsedTime) / 1_000_000_000L);
          }

          double newCPUUsage = 0;
          int availableCpus = updatedClient.getCpus();
          if (availableCpus > 0) {
            newCPUUsage = newCPUTime / availableCpus;
          }

          existingClient.setCpuUsage(newCPUUsage);
          existingClient.setProcessCpuTime(currCPUTime);
        } else {
          // Add client to clients list
          memberClientsHMap.put(clientId, client);
        }
      }

      // Remove unwanted entries from clients list
      HashMap<String, Cluster.Client> memberClientsHMapNew = new HashMap<>();
      for (Map.Entry<String, Cluster.Client> entry : memberClientsHMap.entrySet()) {
        String clientId = entry.getKey();
        if (memberClientsHM.get(clientId) != null) {
          memberClientsHMapNew.put(clientId, memberClientsHMap.get(clientId));
        }
      }
      // replace existing memberClientsHMap by memberClientsHMapNew
      setMemberClientsHMap(memberClientsHMapNew);

      // update last update time
      Cluster.LAST_UPDATE_TIME = systemNanoTime;
    }
  }

  /**
   * Member Inner Class
   *
   *
   */
  public static class Statement {

    private String queryDefn;
    private long numTimesCompiled;
    private long numExecution;
    private long numExecutionsInProgress;
    private long numTimesGlobalIndexLookup;
    private long numRowsModified;
    private long parseTime;
    private long bindTime;
    private long optimizeTime;
    private long routingInfoTime;
    private long generateTime;
    private long totalCompilationTime;
    private long executionTime;
    private long projectionTime;
    private long totalExecutionTime;
    private long rowsModificationTime;
    private long qNNumRowsSeen;
    private long qNMsgSendTime;
    private long qNMsgSerTime;
    private long qNRespDeSerTime;

    public static String[] getGridColumnNames() {
      return new String[] {PulseConstants.MBEAN_COLNAME_QUERYDEFINITION,
          PulseConstants.MBEAN_COLNAME_NUMEXECUTION,
          PulseConstants.MBEAN_COLNAME_TOTALEXECUTIONTIME,
          PulseConstants.MBEAN_COLNAME_NUMEXECUTIONSINPROGRESS,
          PulseConstants.MBEAN_COLNAME_NUMTIMESCOMPILED,
          PulseConstants.MBEAN_COLNAME_NUMTIMESGLOBALINDEXLOOKUP,
          PulseConstants.MBEAN_COLNAME_NUMROWSMODIFIED, PulseConstants.MBEAN_COLNAME_PARSETIME,
          PulseConstants.MBEAN_COLNAME_BINDTIME, PulseConstants.MBEAN_COLNAME_OPTIMIZETIME,
          PulseConstants.MBEAN_COLNAME_ROUTINGINFOTIME, PulseConstants.MBEAN_COLNAME_GENERATETIME,
          PulseConstants.MBEAN_COLNAME_TOTALCOMPILATIONTIME,
          PulseConstants.MBEAN_COLNAME_EXECUTIONTIME, PulseConstants.MBEAN_COLNAME_PROJECTIONTIME,
          PulseConstants.MBEAN_COLNAME_ROWSMODIFICATIONTIME,
          PulseConstants.MBEAN_COLNAME_QNNUMROWSSEEN, PulseConstants.MBEAN_COLNAME_QNMSGSENDTIME,
          PulseConstants.MBEAN_COLNAME_QNMSGSERTIME, PulseConstants.MBEAN_COLNAME_QNRESPDESERTIME};
    }

    public static String[] getGridColumnAttributes() {
      return new String[] {PulseConstants.MBEAN_ATTRIBUTE_QUERYDEFINITION,
          PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTION,
          PulseConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME,
          PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS,
          PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED,
          PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
          PulseConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED, PulseConstants.MBEAN_ATTRIBUTE_PARSETIME,
          PulseConstants.MBEAN_ATTRIBUTE_BINDTIME, PulseConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME,
          PulseConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME,
          PulseConstants.MBEAN_ATTRIBUTE_GENERATETIME,
          PulseConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME,
          PulseConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME,
          PulseConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME,

          PulseConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME,
          PulseConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN,
          PulseConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME, PulseConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME,
          PulseConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME};
    }

    public static int[] getGridColumnWidths() {
      return new int[] {300, 150, 160, 180, 150, 200, 150, 130, 130, 160, 140, 180, 170,
          160, 130, 190, 170, 170, 170, 200};
    }

    /**
     * @return the numTimesCompiled
     */
    public String getQueryDefinition() {
      return queryDefn;
    }

    /**
     * @param queryDefn the query to set
     */
    public void setQueryDefinition(String queryDefn) {
      this.queryDefn = queryDefn;
    }

    /**
     * @return the numTimesCompiled
     */
    public long getNumTimesCompiled() {
      return numTimesCompiled;
    }

    /**
     * @param numTimesCompiled the numTimesCompiled to set
     */
    public void setNumTimesCompiled(long numTimesCompiled) {
      this.numTimesCompiled = numTimesCompiled;
    }

    /**
     * @return the numExecution
     */
    public long getNumExecution() {
      return numExecution;
    }

    /**
     * @param numExecution the numExecution to set
     */
    public void setNumExecution(long numExecution) {
      this.numExecution = numExecution;
    }

    /**
     * @return the numExecutionsInProgress
     */
    public long getNumExecutionsInProgress() {
      return numExecutionsInProgress;
    }

    /**
     * @param numExecutionsInProgress the numExecutionsInProgress to set
     */
    public void setNumExecutionsInProgress(long numExecutionsInProgress) {
      this.numExecutionsInProgress = numExecutionsInProgress;
    }

    /**
     * @return the numTimesGlobalIndexLookup
     */
    public long getNumTimesGlobalIndexLookup() {
      return numTimesGlobalIndexLookup;
    }

    /**
     * @param numTimesGlobalIndexLookup the numTimesGlobalIndexLookup to set
     */
    public void setNumTimesGlobalIndexLookup(long numTimesGlobalIndexLookup) {
      this.numTimesGlobalIndexLookup = numTimesGlobalIndexLookup;
    }

    /**
     * @return the numRowsModified
     */
    public long getNumRowsModified() {
      return numRowsModified;
    }

    /**
     * @param numRowsModified the numRowsModified to set
     */
    public void setNumRowsModified(long numRowsModified) {
      this.numRowsModified = numRowsModified;
    }

    /**
     * @return the parseTime
     */
    public long getParseTime() {
      return parseTime;
    }

    /**
     * @param parseTime the parseTime to set
     */
    public void setParseTime(long parseTime) {
      this.parseTime = parseTime;
    }

    /**
     * @return the bindTime
     */
    public long getBindTime() {
      return bindTime;
    }

    /**
     * @param bindTime the bindTime to set
     */
    public void setBindTime(long bindTime) {
      this.bindTime = bindTime;
    }

    /**
     * @return the optimizeTime
     */
    public long getOptimizeTime() {
      return optimizeTime;
    }

    /**
     * @param optimizeTime the optimizeTime to set
     */
    public void setOptimizeTime(long optimizeTime) {
      this.optimizeTime = optimizeTime;
    }

    /**
     * @return the routingInfoTime
     */
    public long getRoutingInfoTime() {
      return routingInfoTime;
    }

    /**
     * @param routingInfoTime the routingInfoTime to set
     */
    public void setRoutingInfoTime(long routingInfoTime) {
      this.routingInfoTime = routingInfoTime;
    }

    /**
     * @return the generateTime
     */
    public long getGenerateTime() {
      return generateTime;
    }

    /**
     * @param generateTime the generateTime to set
     */
    public void setGenerateTime(long generateTime) {
      this.generateTime = generateTime;
    }

    /**
     * @return the totalCompilationTime
     */
    public long getTotalCompilationTime() {
      return totalCompilationTime;
    }

    /**
     * @param totalCompilationTime the totalCompilationTime to set
     */
    public void setTotalCompilationTime(long totalCompilationTime) {
      this.totalCompilationTime = totalCompilationTime;
    }

    /**
     * @return the executionTime
     */
    public long getExecutionTime() {
      return executionTime;
    }

    /**
     * @param executionTime the executionTime to set
     */
    public void setExecutionTime(long executionTime) {
      this.executionTime = executionTime;
    }

    /**
     * @return the projectionTime
     */
    public long getProjectionTime() {
      return projectionTime;
    }

    /**
     * @param projectionTime the projectionTime to set
     */
    public void setProjectionTime(long projectionTime) {
      this.projectionTime = projectionTime;
    }

    /**
     * @return the totalExecutionTime
     */
    public long getTotalExecutionTime() {
      return totalExecutionTime;
    }

    /**
     * @param totalExecutionTime the totalExecutionTime to set
     */
    public void setTotalExecutionTime(long totalExecutionTime) {
      this.totalExecutionTime = totalExecutionTime;
    }

    /**
     * @return the rowsModificationTime
     */
    public long getRowsModificationTime() {
      return rowsModificationTime;
    }

    /**
     * @param rowsModificationTime the rowsModificationTime to set
     */
    public void setRowsModificationTime(long rowsModificationTime) {
      this.rowsModificationTime = rowsModificationTime;
    }

    /**
     * @return the qNNumRowsSeen
     */
    public long getqNNumRowsSeen() {
      return qNNumRowsSeen;
    }

    /**
     * @param qNNumRowsSeen the qNNumRowsSeen to set
     */
    public void setqNNumRowsSeen(long qNNumRowsSeen) {
      this.qNNumRowsSeen = qNNumRowsSeen;
    }

    /**
     * @return the qNMsgSendTime
     */
    public long getqNMsgSendTime() {
      return qNMsgSendTime;
    }

    /**
     * @param qNMsgSendTime the qNMsgSendTime to set
     */
    public void setqNMsgSendTime(long qNMsgSendTime) {
      this.qNMsgSendTime = qNMsgSendTime;
    }

    /**
     * @return the qNMsgSerTime
     */
    public long getqNMsgSerTime() {
      return qNMsgSerTime;
    }

    /**
     * @param qNMsgSerTime the qNMsgSerTime to set
     */
    public void setqNMsgSerTime(long qNMsgSerTime) {
      this.qNMsgSerTime = qNMsgSerTime;
    }

    /**
     * @return the qNRespDeSerTime
     */
    public long getqNRespDeSerTime() {
      return qNRespDeSerTime;
    }

    /**
     * @param qNRespDeSerTime the qNRespDeSerTime to set
     */
    public void setqNRespDeSerTime(long qNRespDeSerTime) {
      this.qNRespDeSerTime = qNRespDeSerTime;
    }
  }

  public static class RegionOnMember {

    public static final int REGION_ON_MEMBER_STAT_GETS_PER_SEC_TREND = 0;
    public static final int REGION_ON_MEMBER_STAT_PUTS_PER_SEC_TREND = 1;
    public static final int REGION_ON_MEMBER_STAT_DISK_READS_PER_SEC_TREND = 3;
    public static final int REGION_ON_MEMBER_STAT_DISK_WRITES_PER_SEC_TREND = 4;

    private String regionFullPath;
    private String memberName;
    private long entrySize;
    private long entryCount;
    private double getsRate;
    private double putsRate;
    private double diskGetsRate;
    private double diskPutsRate;
    private int localMaxMemory;

    private CircularFifoBuffer getsPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer putsPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer diskReadsPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer diskWritesPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);

    /**
     * @return the entrySize
     */
    public long getEntrySize() {
      return entrySize;
    }

    /**
     * @param entrySize the entrySize to set
     */
    public void setEntrySize(long entrySize) {
      this.entrySize = entrySize;
    }

    /**
     * @return the entryCount
     */
    public long getEntryCount() {
      return entryCount;
    }

    /**
     * @param entryCount the entryCount to set
     */
    public void setEntryCount(long entryCount) {
      this.entryCount = entryCount;
    }

    /**
     * @return the putsRate
     */
    public double getPutsRate() {
      return putsRate;
    }

    /**
     * @param putsRate the putsRate to set
     */
    public void setPutsRate(double putsRate) {
      this.putsRate = putsRate;
    }

    /**
     * @return the getsRate
     */
    public double getGetsRate() {
      return getsRate;
    }

    /**
     * @param getsRate the getsRate to set
     */
    public void setGetsRate(double getsRate) {
      this.getsRate = getsRate;
    }

    /**
     * @return the diskGetsRate
     */
    public double getDiskGetsRate() {
      return diskGetsRate;
    }

    /**
     * @param diskGetsRate the diskGetsRate to set
     */
    public void setDiskGetsRate(double diskGetsRate) {
      this.diskGetsRate = diskGetsRate;
    }

    /**
     * @return the diskPutsRate
     */
    public double getDiskPutsRate() {
      return diskPutsRate;
    }

    /**
     * @param diskPutsRate the diskPutsRate to set
     */
    public void setDiskPutsRate(double diskPutsRate) {
      this.diskPutsRate = diskPutsRate;
    }

    /**
     * @return the local maximum memory
     */
    public int getLocalMaxMemory() {
      return localMaxMemory;
    }

    public void setLocalMaxMemory(int localMaxMemory) {
      this.localMaxMemory = localMaxMemory;
    }

    /**
     * @return the getsPerSecTrend
     */
    public CircularFifoBuffer getGetsPerSecTrend() {
      return getsPerSecTrend;
    }

    /**
     * @param getsPerSecTrend the getsPerSecTrend to set
     */
    public void setGetsPerSecTrend(CircularFifoBuffer getsPerSecTrend) {
      this.getsPerSecTrend = getsPerSecTrend;
    }

    /**
     * @return the putsPerSecTrend
     */
    public CircularFifoBuffer getPutsPerSecTrend() {
      return putsPerSecTrend;
    }

    /**
     * @param putsPerSecTrend the putsPerSecTrend to set
     */
    public void setPutsPerSecTrend(CircularFifoBuffer putsPerSecTrend) {
      this.putsPerSecTrend = putsPerSecTrend;
    }

    /**
     * @return the diskReadsPerSecTrend
     */
    public CircularFifoBuffer getDiskReadsPerSecTrend() {
      return diskReadsPerSecTrend;
    }

    /**
     * @param diskReadsPerSecTrend the diskReadsPerSecTrend to set
     */
    public void setDiskReadsPerSecTrend(CircularFifoBuffer diskReadsPerSecTrend) {
      this.diskReadsPerSecTrend = diskReadsPerSecTrend;
    }

    /**
     * @return the diskWritesPerSecTrend
     */
    public CircularFifoBuffer getDiskWritesPerSecTrend() {
      return diskWritesPerSecTrend;
    }

    /**
     * @param diskWritesPerSecTrend the diskWritesPerSecTrend to set
     */
    public void setDiskWritesPerSecTrend(CircularFifoBuffer diskWritesPerSecTrend) {
      this.diskWritesPerSecTrend = diskWritesPerSecTrend;
    }

    public Object[] getRegionOnMemberStatisticTrend(int trendId) {

      Object[] returnArray = null;
      switch (trendId) {
        case REGION_ON_MEMBER_STAT_GETS_PER_SEC_TREND:
          synchronized (getsPerSecTrend) {
            returnArray = getsPerSecTrend.toArray();
          }
          break;

        case REGION_ON_MEMBER_STAT_PUTS_PER_SEC_TREND:
          synchronized (putsPerSecTrend) {
            returnArray = putsPerSecTrend.toArray();
          }
          break;

        case REGION_ON_MEMBER_STAT_DISK_READS_PER_SEC_TREND:
          synchronized (diskReadsPerSecTrend) {
            returnArray = diskReadsPerSecTrend.toArray();
          }
          break;

        case REGION_ON_MEMBER_STAT_DISK_WRITES_PER_SEC_TREND:
          synchronized (diskWritesPerSecTrend) {
            returnArray = diskWritesPerSecTrend.toArray();
          }
          break;
      }

      return returnArray;
    }

    /**
     * @return the regionFullPath
     */
    public String getRegionFullPath() {
      return regionFullPath;
    }

    /**
     * @param regionFullPath the regionFullPath to set
     */
    public void setRegionFullPath(String regionFullPath) {
      this.regionFullPath = regionFullPath;
    }

    /**
     * @return the memberName
     */
    public String getMemberName() {
      return memberName;
    }

    /**
     * @param memberName the memberName to set
     */
    public void setMemberName(String memberName) {
      this.memberName = memberName;
    }
  }

  /**
   * Region Inner Class
   *
   *
   */
  public static class Region {
    // start: fields defined in MBean
    private String fullPath;
    private double diskReadsRate;
    private double diskWritesRate;
    private double getsRate;
    private double putsRate;
    private double lruEvictionRate;
    private String regionType;
    private long systemRegionEntryCount;
    private int memberCount;
    private String name;
    private boolean persistentEnabled;
    private long entrySize;
    private boolean wanEnabled;
    private int emptyNode;
    private long diskUsage;
    private String scope;
    private String diskStoreName;
    private boolean diskSynchronous;
    private boolean enableOffHeapMemory;
    private String compressionCodec = "";

    private List<String> memberName = new ArrayList<>();
    private List<RegionOnMember> regionOnMembers = new ArrayList<>();
    private CircularFifoBuffer getsPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer putsPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer diskReadsPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);
    private CircularFifoBuffer diskWritesPerSecTrend = new CircularFifoBuffer(MAX_SAMPLE_SIZE);

    public static final int REGION_STAT_GETS_PER_SEC_TREND = 0;
    public static final int REGION_STAT_PUTS_PER_SEC_TREND = 1;
    public static final int REGION_STAT_DISK_READS_PER_SEC_TREND = 3;
    public static final int REGION_STAT_DISK_WRITES_PER_SEC_TREND = 4;

    // end: fields defined in MBean

    public Object[] getRegionStatisticTrend(int trendId) {

      Object[] returnArray = null;
      switch (trendId) {
        case REGION_STAT_GETS_PER_SEC_TREND:
          synchronized (getsPerSecTrend) {
            returnArray = getsPerSecTrend.toArray();
          }
          break;

        case REGION_STAT_PUTS_PER_SEC_TREND:
          synchronized (putsPerSecTrend) {
            returnArray = putsPerSecTrend.toArray();
          }
          break;

        case REGION_STAT_DISK_READS_PER_SEC_TREND:
          synchronized (diskReadsPerSecTrend) {
            returnArray = diskReadsPerSecTrend.toArray();
          }
          break;

        case REGION_STAT_DISK_WRITES_PER_SEC_TREND:
          synchronized (diskWritesPerSecTrend) {
            returnArray = diskWritesPerSecTrend.toArray();
          }
          break;
      }

      return returnArray;
    }

    public boolean isDiskSynchronous() {
      return diskSynchronous;
    }

    public void setDiskSynchronous(boolean diskSynchronous) {
      this.diskSynchronous = diskSynchronous;
    }

    public String getDiskStoreName() {
      return diskStoreName;
    }

    public void setDiskStoreName(String diskStoreName) {
      this.diskStoreName = diskStoreName;
    }

    public String getScope() {
      return scope;
    }

    public void setScope(String scope) {
      this.scope = scope;
    }

    public int getEmptyNode() {
      return emptyNode;
    }

    public void setEmptyNode(int emptyNode) {
      this.emptyNode = emptyNode;
    }

    public long getDiskUsage() {
      return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
      this.diskUsage = diskUsage;
    }

    public void setEntrySize(long entrySize) {
      this.entrySize = entrySize;
    }

    public boolean getWanEnabled() {
      return wanEnabled;
    }

    public void setWanEnabled(boolean wanEnabled) {
      this.wanEnabled = wanEnabled;
    }

    public boolean getPersistentEnabled() {
      return persistentEnabled;
    }

    public void setPersistentEnabled(boolean persistentEnabled) {
      this.persistentEnabled = persistentEnabled;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getEntrySize() {
      return entrySize;
    }

    public List<String> getMemberName() {
      return memberName;
    }

    public void setMemberName(List<String> memberName) {
      this.memberName = memberName;
    }

    public String getFullPath() {
      return fullPath;
    }

    public void setFullPath(String fullPath) {
      this.fullPath = fullPath;
    }

    public double getDiskReadsRate() {
      return diskReadsRate;
    }

    public void setDiskReadsRate(double diskReadsRate) {
      this.diskReadsRate = diskReadsRate;
    }

    public double getDiskWritesRate() {
      return diskWritesRate;
    }

    public void setDiskWritesRate(double diskWritesRate) {
      this.diskWritesRate = diskWritesRate;
    }

    public CircularFifoBuffer getDiskReadsPerSecTrend() {
      return diskReadsPerSecTrend;
    }

    public void setDiskReadsPerSecTrend(CircularFifoBuffer diskReadsPerSecTrend) {
      this.diskReadsPerSecTrend = diskReadsPerSecTrend;
    }

    public CircularFifoBuffer getDiskWritesPerSecTrend() {
      return diskWritesPerSecTrend;
    }

    public void setDiskWritesPerSecTrend(CircularFifoBuffer diskWritesPerSecTrend) {
      this.diskWritesPerSecTrend = diskWritesPerSecTrend;
    }

    public double getGetsRate() {
      return getsRate;
    }

    public void setGetsRate(double getsRate) {
      this.getsRate = getsRate;
    }

    public double getLruEvictionRate() {
      return lruEvictionRate;
    }

    public void setLruEvictionRate(double lruEvictionRate) {
      this.lruEvictionRate = lruEvictionRate;
    }

    public String getRegionType() {
      return regionType;
    }

    public void setRegionType(String regionType) {
      this.regionType = regionType;
    }

    public long getSystemRegionEntryCount() {
      return systemRegionEntryCount;
    }

    public void setSystemRegionEntryCount(long systemRegionEntryCount) {
      this.systemRegionEntryCount = systemRegionEntryCount;
    }

    public int getMemberCount() {
      return memberCount;
    }

    public void setMemberCount(int memberCount) {
      this.memberCount = memberCount;
    }

    public double getPutsRate() {
      return putsRate;
    }

    public void setPutsRate(double putsRate) {
      this.putsRate = putsRate;
    }

    public CircularFifoBuffer getGetsPerSecTrend() {
      return getsPerSecTrend;
    }

    public void setGetsPerSecTrend(CircularFifoBuffer getsPerSecTrend) {
      this.getsPerSecTrend = getsPerSecTrend;
    }

    public CircularFifoBuffer getPutsPerSecTrend() {
      return putsPerSecTrend;
    }

    public void setPutsPerSecTrend(CircularFifoBuffer putsPerSecTrend) {
      this.putsPerSecTrend = putsPerSecTrend;
    }

    public boolean isEnableOffHeapMemory() {
      return enableOffHeapMemory;
    }

    public void setEnableOffHeapMemory(boolean enableOffHeapMemory) {
      this.enableOffHeapMemory = enableOffHeapMemory;
    }

    public String getCompressionCodec() {
      return compressionCodec;
    }

    public void setCompressionCodec(String compressionCodec) {
      this.compressionCodec = compressionCodec;
    }

    public Cluster.RegionOnMember[] getRegionOnMembers() {
      Cluster.RegionOnMember[] regionOnMembers = null;
      synchronized (this.regionOnMembers) {
        regionOnMembers = new Cluster.RegionOnMember[this.regionOnMembers.size()];
        regionOnMembers = this.regionOnMembers.toArray(regionOnMembers);
      }

      return regionOnMembers;
    }

    /**
     * @param regionOnMembers the regionOnMembers to set
     */
    public void setRegionOnMembers(List<RegionOnMember> regionOnMembers) {
      this.regionOnMembers = regionOnMembers;
    }
  }

  /**
   * Alert Inner Class
   *
   *
   */
  public static class Alert {
    public static final int SEVERE = 0;
    public static final int ERROR = 1;
    public static final int WARNING = 2;
    public static final int INFO = 3;

    public static AtomicInteger ALERT_ID_CTR = new AtomicInteger();

    private int id;
    private Date timestamp;
    private int severity;
    private String memberName;
    private String description;
    private boolean isAcknowledged;
    private String iso8601Ts;

    public String getIso8601Ts() {
      return iso8601Ts;
    }

    public void setIso8601Ts(String iso8601Ts) {
      this.iso8601Ts = iso8601Ts;
    }

    public boolean isAcknowledged() {
      return isAcknowledged;
    }

    public void setAcknowledged(boolean isAcknowledged) {
      this.isAcknowledged = isAcknowledged;
    }

    public Date getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(Date timestamp) {
      this.timestamp = timestamp;
      iso8601Ts = formatToISOTimestamp(timestamp);
    }

    public int getSeverity() {
      return severity;
    }

    public void setSeverity(int severity) {
      this.severity = severity;
    }

    public String getMemberName() {
      return memberName;
    }

    public void setMemberName(String memberName) {
      this.memberName = memberName;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public static int nextID() {
      /*
       * int id = -1; synchronized (Alert.class) { ALERT_ID_CTR = ALERT_ID_CTR + 1; id =
       * ALERT_ID_CTR; }
       */
      return ALERT_ID_CTR.incrementAndGet();
    }

    private static DateFormat df =
        new SimpleDateFormat(PulseConstants.PULSE_NOTIFICATION_ALERT_DATE_PATTERN);

    public static String formatToISOTimestamp(Date date) {
      TimeZone tz = TimeZone.getTimeZone("UTC");
      df.setTimeZone(tz);
      return df.format(date);
    }

  }

  /**
   * Client Inner Class
   *
   *
   */
  public static class Client {

    private String id;
    private String name;
    private String host;
    private int queueSize;
    private double cpuUsage;
    private long uptime;
    private int threads;
    private int gets;
    private int puts;
    private int cpus;
    private int clientCQCount;
    private long processCpuTime;
    private String status;
    private boolean isConnected = false;
    private boolean isSubscriptionEnabled = false;

    public String getId() {
      return id;
    }

    public int getGets() {
      return gets;
    }

    public int getPuts() {
      return puts;
    }

    public int getClientCQCount() {
      return clientCQCount;
    }

    public void setClientCQCount(int clientCQCount) {
      this.clientCQCount = clientCQCount;
    }

    public boolean isSubscriptionEnabled() {
      return isSubscriptionEnabled;
    }

    public void setSubscriptionEnabled(boolean isSubscriptionEnabled) {
      this.isSubscriptionEnabled = isSubscriptionEnabled;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public int getQueueSize() {
      return queueSize;
    }

    public void setQueueSize(int queueSize) {
      this.queueSize = queueSize;
    }

    public double getCpuUsage() {
      return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
      this.cpuUsage = cpuUsage;
    }

    public void setGets(int gets) {
      this.gets = gets;
    }

    public void setPuts(int puts) {
      this.puts = puts;
    }

    public long getUptime() {
      return uptime;
    }

    public void setUptime(long uptime) {
      this.uptime = uptime;
    }

    public int getThreads() {
      return threads;
    }

    public void setThreads(int threads) {
      this.threads = threads;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public int getCpus() {
      return cpus;
    }

    public void setCpus(int cpus) {
      this.cpus = cpus;
    }

    public long getProcessCpuTime() {
      return processCpuTime;
    }

    public void setProcessCpuTime(long processCpuTime) {
      this.processCpuTime = processCpuTime;
    }

    public boolean isConnected() {
      return isConnected;
    }

    public void setConnected(boolean isConnected) {
      this.isConnected = isConnected;
    }

  }

  /**
   * Gateway Receiver Inner Class
   *
   *
   */
  public static class GatewayReceiver {

    private int listeningPort;
    private double linkThroughput;
    private long avgBatchProcessingTime;
    private String id;
    private int queueSize;
    private Boolean status;
    private int batchSize;

    public int getListeningPort() {
      return listeningPort;
    }

    public void setListeningPort(int listeningPort) {
      this.listeningPort = listeningPort;
    }

    public double getLinkThroughput() {
      return linkThroughput;
    }

    public void setLinkThroughput(double linkThroughput) {
      this.linkThroughput = linkThroughput;
    }

    public long getAvgBatchProcessingTime() {
      return avgBatchProcessingTime;
    }

    public void setAvgBatchProcessingTime(long avgBatchProcessingTime) {
      this.avgBatchProcessingTime = avgBatchProcessingTime;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getQueueSize() {
      return queueSize;
    }

    public void setQueueSize(int queueSize) {
      this.queueSize = queueSize;
    }

    public Boolean getStatus() {
      return status;
    }

    public void setStatus(Boolean status) {
      this.status = status;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }
  }

  /**
   * Gateway Sender Inner class
   *
   *
   */
  public static class GatewaySender {

    private double linkThroughput;
    private String id;
    private int queueSize;
    private Boolean status;
    private boolean primary;
    private boolean senderType;
    private int batchSize;
    private boolean persistenceEnabled;
    private int remoteDSId;
    private int eventsExceedingAlertThreshold;

    public double getLinkThroughput() {
      return linkThroughput;
    }

    public void setLinkThroughput(double linkThroughput) {
      this.linkThroughput = linkThroughput;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getQueueSize() {
      return queueSize;
    }

    public void setQueueSize(int queueSize) {
      this.queueSize = queueSize;
    }

    public Boolean getStatus() {
      return status;
    }

    public void setStatus(Boolean status) {
      this.status = status;
    }

    public boolean getPrimary() {
      return primary;
    }

    public void setPrimary(boolean primary) {
      this.primary = primary;
    }

    public boolean getSenderType() {
      return senderType;
    }

    public void setSenderType(boolean senderType) {
      this.senderType = senderType;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public boolean getPersistenceEnabled() {
      return persistenceEnabled;
    }

    public void setPersistenceEnabled(boolean persistenceEnabled) {
      this.persistenceEnabled = persistenceEnabled;
    }

    /**
     * @return the remoteDSId
     */
    public int getRemoteDSId() {
      return remoteDSId;
    }

    /**
     * @param remoteDSId the remoteDSId to set
     */
    public void setRemoteDSId(int remoteDSId) {
      this.remoteDSId = remoteDSId;
    }

    /**
     * @return the eventsExceedingAlertThreshold
     */
    public int getEventsExceedingAlertThreshold() {
      return eventsExceedingAlertThreshold;
    }

    /**
     * @param eventsExceedingAlertThreshold the eventsExceedingAlertThreshold to set
     */
    public void setEventsExceedingAlertThreshold(int eventsExceedingAlertThreshold) {
      this.eventsExceedingAlertThreshold = eventsExceedingAlertThreshold;
    }
  }

  /**
   * Async Event Queue Inner class
   *
   *
   */
  public static class AsyncEventQueue {

    private String id;
    private boolean primary;
    private boolean parallel;
    private int batchSize;
    private long batchTimeInterval;
    private boolean batchConflationEnabled;
    private String asyncEventListener;
    private int eventQueueSize;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public boolean getPrimary() {
      return primary;
    }

    public void setPrimary(boolean primary) {
      this.primary = primary;
    }

    /**
     * @return the parallel
     */
    public boolean isParallel() {
      return parallel;
    }

    /**
     * @param parallel the parallel to set
     */
    public void setParallel(boolean parallel) {
      this.parallel = parallel;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    /**
     * @return the batchTimeInterval
     */
    public long getBatchTimeInterval() {
      return batchTimeInterval;
    }

    /**
     * @param batchTimeInterval the batchTimeInterval to set
     */
    public void setBatchTimeInterval(long batchTimeInterval) {
      this.batchTimeInterval = batchTimeInterval;
    }

    /**
     * @return the batchConflationEnabled
     */
    public boolean isBatchConflationEnabled() {
      return batchConflationEnabled;
    }

    /**
     * @param batchConflationEnabled the batchConflationEnabled to set
     */
    public void setBatchConflationEnabled(boolean batchConflationEnabled) {
      this.batchConflationEnabled = batchConflationEnabled;
    }

    /**
     * @return the asyncEventListener
     */
    public String getAsyncEventListener() {
      return asyncEventListener;
    }

    /**
     * @param asyncEventListener the asyncEventListener to set
     */
    public void setAsyncEventListener(String asyncEventListener) {
      this.asyncEventListener = asyncEventListener;
    }

    /**
     * @return the eventQueueSize
     */
    public int getEventQueueSize() {
      return eventQueueSize;
    }

    /**
     * @param eventQueueSize the eventQueueSize to set
     */
    public void setEventQueueSize(int eventQueueSize) {
      this.eventQueueSize = eventQueueSize;
    }
  }

  /**
   * Default constructor only used for testing
   */
  public Cluster() {}


  /**
   * This function is used for calling getUpdator function of ClusterDataFactory and starting the
   * thread for updating the Cluster details.
   *
   * @param host host name
   * @param port port
   * @param userName pulse user name
   */
  public Cluster(String host, String port, String userName) {
    serverName = host;
    this.port = port;
    jmxUserName = userName;

    updater = new JMXDataUpdater(serverName, port, this);
    clusterHasBeenInitialized = new CountDownLatch(1);
    if (Boolean.getBoolean(PulseConstants.SYSTEM_PROPERTY_PULSE_EMBEDDED)) {
      setDaemon(true);
    }
  }

  public void waitForInitialization(long timeout, TimeUnit unit) throws InterruptedException {
    clusterHasBeenInitialized.await(timeout, unit);
  }

  /**
   * thread run method for updating the cluster data
   */
  @Override
  public void run() {
    try {
      while (!stopUpdates && isConnectedFlag()) {
        try {
          if (!updateData()) {
            stale++;
          } else {
            stale = 0;
          }
        } catch (Exception e) {
          logger.info("Exception Occurred while updating cluster data : ", e);
        }

        clusterHasBeenInitialized.countDown();
        try {
          Thread.sleep(POLL_INTERVAL);
        } catch (InterruptedException e) {
          logger.info("InterruptedException Occurred : ", e);
        }
      }

      logger.info("{} :: {}:{}", resourceBundle.getString("LOG_MSG_STOP_THREAD_UPDATES"),
          serverName, port);
    } finally {
      clusterHasBeenInitialized.countDown();
    }
  }

  /**
   * calling updateData
   *
   * @return true if update was successful. false if it failed.
   */
  private boolean updateData() {
    // This will eventually call JMX. Currently we will update this with
    // some dummy data.
    // Connect if required or hold a connection. If unable to connect,
    // return false
    logger.debug("{} :: {}:{}", resourceBundle.getString("LOG_MSG_CLUSTER_DATA_IS_UPDATING"),
        serverName, port);
    return updater.updateData();
  }

  /**
   * for stopping the update thread
   */
  public void stopThread() {
    stopUpdates = true;

    try {
      join();
    } catch (InterruptedException e) {
      logger.info("InterruptedException occurred while stoping cluster thread : ", e);
    }
  }

  public Map<String, Cluster.Member> getMembersHMap() {
    return membersHMap;
  }

  public void setMembersHMap(HashMap<String, Cluster.Member> membersHMap) {
    this.membersHMap = membersHMap;
  }

  public Map<String, Boolean> getWanInformation() {
    synchronized (wanInformation) {
      @SuppressWarnings("unchecked")
      Map<String, Boolean> wanMap = (Map<String, Boolean>) wanInformation.clone();
      return wanMap;
    }
  }

  // Returns actual wanInformation object reference
  public Map<String, Boolean> getWanInformationObject() {
    return wanInformation;
  }

  public String getJmxUserName() {
    return jmxUserName;
  }

  public String getConnectionErrorMsg() {
    return connectionErrorMsg;
  }

  public void setConnectionErrorMsg(String connectionErrorMsg) {
    this.connectionErrorMsg = connectionErrorMsg;
  }

  public String getServerName() {
    return serverName;
  }

  public boolean isConnectedFlag() {
    return connectedFlag;
  }

  public void setConnectedFlag(boolean connectedFlag) {
    this.connectedFlag = connectedFlag;
  }

  public String getPort() {
    return port;
  }

  public int getStale() {
    return stale;
  }

  public double getWritePerSec() {
    return writePerSec;
  }

  public void setWritePerSec(double writePerSec) {
    this.writePerSec = writePerSec;
  }

  public double getReadPerSec() {
    return readPerSec;
  }

  public void setReadPerSec(double readPerSec) {
    this.readPerSec = readPerSec;
  }

  public double getQueriesPerSec() {
    return queriesPerSec;
  }

  public void setQueriesPerSec(double queriesPerSec) {
    this.queriesPerSec = queriesPerSec;
  }

  public double getLoadPerSec() {
    return loadPerSec;
  }

  public void setLoadPerSec(double loadPerSec) {
    this.loadPerSec = loadPerSec;
  }

  public int getNotificationPageNumber() {
    return notificationPageNumber;
  }

  public void setNotificationPageNumber(int notificationPageNumber) {
    this.notificationPageNumber = notificationPageNumber;
  }

  public void setStale(int stale) {
    this.stale = stale;
  }

  public boolean isStopUpdates() {
    return stopUpdates;
  }

  public void setStopUpdates(boolean stopUpdates) {
    this.stopUpdates = stopUpdates;
  }

  public long getUsedHeapSize() {
    return usedHeapSize;
  }

  public void setUsedHeapSize(long usedHeapSize) {
    this.usedHeapSize = usedHeapSize;
  }

  public int getServerCount() {
    return serverCount;
  }

  public void setServerCount(int serverCount) {
    this.serverCount = serverCount;
  }

  public int getTxnCommittedCount() {
    return txnCommittedCount;
  }

  public void setTxnCommittedCount(int txnCommittedCount) {
    this.txnCommittedCount = txnCommittedCount;
  }

  public int getTxnRollbackCount() {
    return txnRollbackCount;
  }

  public void setTxnRollbackCount(int txnRollbackCount) {
    this.txnRollbackCount = txnRollbackCount;
  }

  public int getRunningFunctionCount() {
    return runningFunctionCount;
  }

  public long getRegisteredCQCount() {
    return registeredCQCount;
  }

  public int getSubscriptionCount() {
    return subscriptionCount;
  }

  public void setSubscriptionCount(int subscriptionCount) {
    this.subscriptionCount = subscriptionCount;
  }

  public void setRegisteredCQCount(long registeredCQCount) {
    this.registeredCQCount = registeredCQCount;
  }

  public void setRunningFunctionCount(int runningFunctionCount) {
    this.runningFunctionCount = runningFunctionCount;
  }

  public Map<String, Cluster.Region> getClusterRegions() {
    return clusterRegionMap;
  }

  public Cluster.Region getClusterRegion(String regionFullPath) {
    return clusterRegionMap.get(regionFullPath);
  }

  public void setClusterRegions(Map<String, Region> clusterRegionMap) {
    this.clusterRegionMap = clusterRegionMap;
  }

  public Map<String, Cluster.Statement> getClusterStatements() {
    return clusterStatementMap;
  }

  public void setClusterStatements(Map<String, Statement> clusterStatementMap) {
    this.clusterStatementMap = clusterStatementMap;
  }

  public Alert[] getAlertsList() {
    Alert[] list;
    synchronized (alertsList) {
      list = new Alert[alertsList.size()];
      list = alertsList.toArray(list);
    }

    return list;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public Set<String> getDeletedMembers() {
    return deletedMembers;
  }

  public void setDeletedMembers(Set<String> deletedMembers) {
    this.deletedMembers = deletedMembers;
  }

  public Set<String> getDeletedRegions() {
    return deletedRegions;
  }

  public void setDeletedRegions(Set<String> deletedRegions) {
    this.deletedRegions = deletedRegions;
  }

  public Map<String, List<Member>> getPhysicalToMember() {
    Map<String, List<Member>> ptom;
    // synchronized (physicalToMember) {
    ptom = physicalToMember;
    // }
    return ptom;
  }

  public void setPhysicalToMember(HashMap<String, List<Member>> physicalToMember) {
    // synchronized (this.physicalToMember) {
    this.physicalToMember = physicalToMember;
    // }
  }

  public int getMemberCount() {
    return memberCount;
  }

  public void setMemberCount(int memberCount) {
    this.memberCount = memberCount;
  }

  public long getClientConnectionCount() {
    return clientConnectionCount;
  }

  public void setClientConnectionCount(long clientConnectionCount) {
    this.clientConnectionCount = clientConnectionCount;
  }

  public int getClusterId() {
    return clusterId;
  }

  public void setClusterId(int clusterId) {
    this.clusterId = clusterId;
  }

  public int getLocatorCount() {
    return locatorCount;
  }

  public void setLocatorCount(int locatorCount) {
    this.locatorCount = locatorCount;
  }

  public int getTotalRegionCount() {
    return totalRegionCount;
  }

  public void setTotalRegionCount(int totalRegionCount) {
    this.totalRegionCount = totalRegionCount;
  }

  public long getTotalHeapSize() {
    return totalHeapSize;
  }

  public void setTotalHeapSize(long totalHeapSize) {
    this.totalHeapSize = totalHeapSize;
  }

  public long getTotalRegionEntryCount() {
    return totalRegionEntryCount;
  }

  public void setTotalRegionEntryCount(long totalRegionEntryCount) {
    this.totalRegionEntryCount = totalRegionEntryCount;
  }

  public int getCurrentQueryCount() {
    return currentQueryCount;
  }

  public void setCurrentQueryCount(int currentQueryCount) {
    this.currentQueryCount = currentQueryCount;
  }

  public long getTotalBytesOnDisk() {
    return totalBytesOnDisk;
  }

  public void setTotalBytesOnDisk(long totalBytesOnDisk) {
    this.totalBytesOnDisk = totalBytesOnDisk;
  }

  public double getDiskReadsRate() {
    return diskReadsRate;
  }

  public void setDiskReadsRate(double diskReadsRate) {
    this.diskReadsRate = diskReadsRate;
  }

  public double getDiskWritesRate() {
    return diskWritesRate;
  }

  public void setDiskWritesRate(double diskWritesRate) {
    this.diskWritesRate = diskWritesRate;
  }

  public int getAvgDiskStorage() {
    return avgDiskStorage;
  }

  public void setAvgDiskStorage(int avgDiskStorage) {
    this.avgDiskStorage = avgDiskStorage;
  }

  public int getAvgDiskWritesRate() {
    return avgDiskWritesRate;
  }

  public void setAvgDiskWritesRate(int avgDiskWritesRate) {
    this.avgDiskWritesRate = avgDiskWritesRate;
  }

  public CircularFifoBuffer getWritePerSecTrend() {
    return writePerSecTrend;
  }

  public void setWritePerSecTrend(CircularFifoBuffer writePerSecTrend) {
    this.writePerSecTrend = writePerSecTrend;
  }

  public long getGarbageCollectionCount() {
    return garbageCollectionCount;
  }

  public void setGarbageCollectionCount(long garbageCollectionCount) {
    this.garbageCollectionCount = garbageCollectionCount;
  }

  public CircularFifoBuffer getTotalBytesOnDiskTrend() {
    return totalBytesOnDiskTrend;
  }

  public void setTotalBytesOnDiskTrend(CircularFifoBuffer totalBytesOnDiskTrend) {
    this.totalBytesOnDiskTrend = totalBytesOnDiskTrend;
  }

  public CircularFifoBuffer getThroughoutWritesTrend() {
    return throughoutWritesTrend;
  }

  public void setThroughoutWritesTrend(CircularFifoBuffer throughoutWritesTrend) {
    this.throughoutWritesTrend = throughoutWritesTrend;
  }

  public CircularFifoBuffer getThroughoutReadsTrend() {
    return throughoutReadsTrend;
  }

  public void setThroughoutReadsTrend(CircularFifoBuffer throughoutReadsTrend) {
    this.throughoutReadsTrend = throughoutReadsTrend;
  }

  public CircularFifoBuffer getReadPerSecTrend() {
    return readPerSecTrend;
  }

  public void setReadPerSecTrend(CircularFifoBuffer readPerSecTrend) {
    this.readPerSecTrend = readPerSecTrend;
  }

  public CircularFifoBuffer getQueriesPerSecTrend() {
    return queriesPerSecTrend;
  }

  public void setQueriesPerSecTrend(CircularFifoBuffer queriesPerSecTrend) {
    this.queriesPerSecTrend = queriesPerSecTrend;
  }

  public CircularFifoBuffer getMemoryUsageTrend() {
    return memoryUsageTrend;
  }

  public void setMemoryUsageTrend(CircularFifoBuffer memoryUsageTrend) {
    this.memoryUsageTrend = memoryUsageTrend;
  }

  public CircularFifoBuffer getGarbageCollectionTrend() {
    return garbageCollectionTrend;
  }

  public void setGarbageCollectionTrend(CircularFifoBuffer garbageCollectionSamples) {
    garbageCollectionTrend = garbageCollectionSamples;
  }

  public long getPreviousJVMPauseCount() {
    return previousJVMPauseCount;
  }

  public void setPreviousJVMPauseCount(long previousJVMPauseCount) {
    this.previousJVMPauseCount = previousJVMPauseCount;
  }

  public DataBrowser getDataBrowser() {
    // Initialize dataBrowser if null
    if (dataBrowser == null) {
      dataBrowser = new DataBrowser();
    }
    return dataBrowser;
  }

  public void setDataBrowser(DataBrowser dataBrowser) {
    this.dataBrowser = dataBrowser;
  }

  public ObjectNode executeQuery(String queryText, String members, int limit) {
    // Execute data browser query
    return updater.executeQuery(queryText, members, limit);
  }

  public ArrayNode getQueryHistoryByUserId(String userId) {
    return getDataBrowser().getQueryHistoryByUserId(userId);
  }

  public boolean addQueryInHistory(String queryText, String userName) {
    return getDataBrowser().addQueryInHistory(queryText, userName);
  }

  public boolean deleteQueryById(String userId, String queryId) {
    return getDataBrowser().deleteQueryById(userId, queryId);
  }

  public void connectToGemFire(String password) {
    jmxConnector = updater.connect(getJmxUserName(), password);

    // if connected
    if (jmxConnector != null) {
      // Start Thread
      start();
      try {
        waitForInitialization(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public JMXConnector getJMXConnector() {
    return jmxConnector;
  }

  public void clearAlerts(int alertType, boolean isClearAll) {

    List<Alert> toDelete = new ArrayList<>();

    if (alertType == -1) {
      synchronized (alertsList) {
        if (isClearAll) {
          // Remove all alerts
          alertsList.clear();
          setNotificationPageNumber(1);
        } else {
          // Remove only acknowledged alerts
          for (Alert alert : alertsList) {
            if (alert.isAcknowledged()) {
              toDelete.add(alert);
            }
          }
          alertsList.removeAll(toDelete);
          toDelete.clear();
        }
      }
    } else {
      synchronized (alertsList) {
        for (Alert alert : alertsList) {
          if (alert.getSeverity() == alertType) {
            if (isClearAll) {
              // Remove all alerts of alertType
              toDelete.add(alert);
            } else if (alert.isAcknowledged()) {
              // Remove only acknowledged alerts of alertType
              toDelete.add(alert);
            }
          }
        }
        alertsList.removeAll(toDelete);
        toDelete.clear();
      }
    }
  }

  public void acknowledgeAlert(int alertId) {
    synchronized (alertsList) {
      for (Cluster.Alert alert : alertsList) {
        if (alert.getId() == alertId) {
          alert.setAcknowledged(true);
          break;
        }
      }
    }
  }

  public void addAlert(Alert alert) {
    synchronized (alertsList) {
      alertsList.add(alert);
      if (alertsList.size() > Cluster.ALERTS_MAX_SIZE)
        alertsList.remove(0);
    }
  }

  /**
   * Key should be memberId + memberName Where the ':' in member id and member names are replaced by
   * '-'
   *
   *
   * @return the Member for a given key
   */
  public Cluster.Member getMember(String memberKey) {
    Cluster.Member member;

    synchronized (membersHMap) {
      member = membersHMap.get(memberKey);
    }

    return member;
  }

  public Cluster.Member[] getMembers() {
    Cluster.Member[] members;
    synchronized (membersHMap) {
      members = new Cluster.Member[membersHMap.size()];
      members = membersHMap.values().toArray(members);
    }
    return members;
  }

  public Cluster.Statement[] getStatements() {
    Cluster.Statement[] statements;
    synchronized (clusterStatementMap) {
      statements = new Cluster.Statement[clusterStatementMap.size()];
      statements = clusterStatementMap.values().toArray(statements);
    }
    return statements;
  }

  public void removeClusterRegion(String regionFullPath) {
    clusterRegionMap.remove(regionFullPath);
  }

  public void addClusterRegion(String regionFullPath, Region region) {
    clusterRegionMap.put(regionFullPath, region);
  }

  public void removeClusterStatement(String name) {
    clusterStatementMap.remove(name);
  }

  public void addClusterStatement(String name, Statement stmt) {
    clusterStatementMap.put(name, stmt);
  }
}
