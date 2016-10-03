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
package org.apache.geode.tools.pulse.testbed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Cluster.Alert;
import org.apache.geode.tools.pulse.internal.data.Cluster.Client;
import org.apache.geode.tools.pulse.internal.data.Cluster.GatewayReceiver;
import org.apache.geode.tools.pulse.internal.data.Cluster.GatewaySender;
import org.apache.geode.tools.pulse.internal.data.Cluster.Member;
import org.apache.geode.tools.pulse.internal.data.Cluster.Region;
import org.apache.geode.tools.pulse.internal.data.IClusterUpdater;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.data.Repository;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Locator;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Peer;
import org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.ResourceBundle;

public class PropMockDataUpdater implements IClusterUpdater {
  private static final int MAX_HOSTS = 40;
  private static final PulseLogWriter LOGGER = PulseLogWriter.getLogger();
  private final ResourceBundle resourceBundle = Repository.get().getResourceBundle();
  private static final int POLL_INTERVAL = 5000;
  public static final int MAX_SAMPLE_SIZE = 180;
  public static final int ALERTS_MAX_SIZE = 1000;
  public static final int PAGE_ALERTS_MAX_SIZE = 100;

  private Cluster cluster= null;
  private TestBed testbed;
  private final String testbedFile = System.getProperty("pulse.propMockDataUpdaterFile");;

  private final ObjectMapper mapper = new ObjectMapper();

  public PropMockDataUpdater(Cluster cluster) {
    this.cluster = cluster;
    try {
      loadPropertiesFile();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadPropertiesFile() throws FileNotFoundException, IOException{
    this.testbed = new TestBed(testbedFile,true);
  }

  /**
   * function used for updating Cluster data
   * for Mock
   */
  @Override
  public boolean updateData() {
    cluster.setConnectedFlag(true);
    Random r = new Random(System.currentTimeMillis());
    long totalHeapSize = Math.abs(r.nextInt(3200 - 2048) + 2048);
    cluster.setTotalHeapSize(totalHeapSize);
    long usedHeapSize  = Math.abs(r.nextInt(2048));
    cluster.setUsedHeapSize(usedHeapSize);
    double writePerSec = Math.abs(r.nextInt(100));
    cluster.setWritePerSec(writePerSec);

    //propfile
    cluster.setSubscriptionCount(testbed.getRootDs().getClients().size());
    cluster.setRegisteredCQCount((long) testbed.getRootDs().getCQs().size());
    cluster.setRunningFunctionCount(testbed.getRootDs().getFunction().size());


    cluster.setClusterId( Math.abs(r.nextInt(100)));
    cluster.getWritePerSecTrend().add(writePerSec);
    cluster.setDiskWritesRate(writePerSec);

    long garbageCollectionCount = Math.abs(r.nextInt(100));
    cluster.setGarbageCollectionCount(garbageCollectionCount);
    cluster.getGarbageCollectionTrend().add(garbageCollectionCount);

    long readPerSec = Math.abs(r.nextInt(100));
    cluster.setReadPerSec(readPerSec);
    cluster.getReadPerSecTrend().add(readPerSec);

    long diskReadsRate = readPerSec;cluster.setDiskReadsRate(diskReadsRate);
    cluster.setDiskReadsRate(readPerSec);
    long queriesPerSec = Math.abs(r.nextInt(100));
    cluster.setQueriesPerSec(queriesPerSec);
    cluster.getQueriesPerSecTrend().add(queriesPerSec);

    long loadPerSec = Math.abs(r.nextInt(100));
    cluster.setLoadPerSec(loadPerSec);
    cluster.setTotalHeapSize(totalHeapSize);
    long totalBytesOnDisk = totalHeapSize;
    cluster.setTotalBytesOnDisk(totalBytesOnDisk);

    cluster.getTotalBytesOnDiskTrend().add(totalBytesOnDisk);

    cluster.getMemoryUsageTrend().add(usedHeapSize);
    cluster.getThroughoutWritesTrend().add(writePerSec);

    cluster.setMemberCount(0);

    Map<String,Cluster.Member>  membersHMap = cluster.getMembersHMap();
    List<Cluster.Region> regionsList = (List<Cluster.Region>)cluster.getClusterRegions().values();
    Map<String, Boolean> wanInformation = cluster.getWanInformation();

    // Create 3 members first time around
    int locatorCount=0;
    if (membersHMap.size() == 0) {
      for(Locator locator : testbed.getRootDs().getLocators()){
        String id = "(Launcher_Locator-1099-13-40-24-5368)-"+locatorCount++;
        String name = locator.getName();
        membersHMap.put(id+name, initializeMember(id,name, true, true, true, false, locator.getHost()));
      }
      cluster.setLocatorCount(testbed.getRootDs().getLocators().size());

      int serverCount=0;
      for(Server server : testbed.getRootDs().getServers()){
        String id = "(Launcher_Server-1099-13-40-24-5368)-"+serverCount++;
        String name = server.getName();
        membersHMap.put(id+name, initializeMember(id,name, false, true, false, true, server.getHost()));
      }
      cluster.setServerCount(testbed.getRootDs().getServers().size());

      int peerCount=0;
      for(Peer peer : testbed.getRootDs().getPeers()){
        String id = "(Launcher_Peer-1099-13-40-24-5368)-"+peerCount++;
        String name = peer.getName();
        membersHMap.put( id+name, initializeMember(id,name, false, true, false, false, peer.getHost()));
      }

      for(Entry<String, Member> memberSet : membersHMap.entrySet())
      {
        HashMap<String,Cluster.Region> memberRegions = new HashMap<String,Cluster.Region>();
        HashMap<String,Cluster.Client> memberClientsHM = new HashMap<String,Cluster.Client>();

        Random randomGenerator = new Random();

        //Read from property file
        int randomInt = (randomGenerator.nextInt(5)) + 1;
        List<org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Region> thisMemberRegions = testbed.getRootDs().getRegions(memberSet.getValue().getName());

        int regionExists = 0;
        int index=0;
        for (org.apache.geode.tools.pulse.testbed.GemFireDistributedSystem.Region thisMemberRegion : thisMemberRegions) {
          Region region = initMemberRegion(index++,thisMemberRegion.getName(),memberSet.getValue().getName(),
              thisMemberRegion.getEntryCount(),thisMemberRegion.getType(), thisMemberRegion.getMembers().size()); //read from property file
          if (regionsList.size() > 0) {
            for (Region clusterRegion : regionsList) {
              if ((region.getName()).equals(clusterRegion.getName())) {
                clusterRegion.getMemberName().add(memberSet.getValue().getName());
                //clusterRegion.memberCount = clusterRegion.memberCount + 1;
                //int mcount = clusterRegion.getMemberCount() + 1;
                //clusterRegion.setMemberCount(mcount);
                regionExists = 1;
                break;
              }
            }
            if (regionExists == 0){
              regionsList.add(region);
            }
          } else{
            regionsList.add(region);
          }
          memberRegions.put(region.getFullPath(),region);
          //totalRegionCount = regionsList.size();
          cluster.setTotalRegionCount(regionsList.size());
        }
        membersHMap.get(memberSet.getKey()).setMemberRegions(memberRegions);

        if (memberSet.getValue().isCache()) {
          Client client = initMemberClient(0, memberSet.getValue().getHost()); //read from prop File
          memberClientsHM.put(client.getId(), client);
          randomInt = randomGenerator.nextInt(10);
          for (int y = 1; y < randomInt; y++) {
            Client newClient = initMemberClient(y, memberSet.getValue()
                .getHost());
            memberClientsHM.put(newClient.getId(), newClient);
          }
          membersHMap.get(memberSet.getKey()).updateMemberClientsHMap(memberClientsHM);
          /*clientConnectionCount = clientConnectionCount
              + membersHMap.get(memberSet.getKey()).getMemberClientsHMap().size();*/
          long clientConnectionCount = cluster.getClientConnectionCount() + membersHMap.get(memberSet.getKey()).getMemberClientsHMap().size();
          cluster.setClientConnectionCount(clientConnectionCount);
        }

      }
    }
    wanInformation.clear(); //read from property file
    int wanInfoSize = Math.abs(r.nextInt(10));
    wanInfoSize++;
    for (int i = 0; i < wanInfoSize; i++) {
      String name = "Mock Cluster" + i;
      Boolean value = false;
      if (i % 2 == 0){
        value = true;
      }
      wanInformation.put(name, value);
    }
    //memberCount = membersHMap.size();
    cluster.setMemberCount(membersHMap.size());

    totalHeapSize = 0;
    for(Entry<String, Member> memberSet : membersHMap.entrySet())
    {
      refresh(membersHMap.get(memberSet.getKey()));
      Member member = membersHMap.get(memberSet.getKey());
      totalHeapSize += member.getCurrentHeapSize();
    }

    for (Region region : regionsList) {
      region.setGetsRate((Math.abs(r.nextInt(100))) + 1);
      region.setPutsRate((Math.abs(r.nextInt(100))) +1);
      region.getGetsPerSecTrend().add(region.getGetsRate());
      region.getPutsPerSecTrend().add(region.getPutsRate());
    }

    return true;
  }


  private Region initMemberRegion(int count, String regionName, String memName, int entryCount, String type, int memberCount) {
    Region memberRegion = new Region();
    memberRegion.setName(regionName);
    memberRegion.setFullPath("/"+regionName);
    Random randomGenerator = new Random();
    memberRegion.setSystemRegionEntryCount(entryCount);
    // memberRegion.setEntrySize("N/A");
    memberRegion.setEntrySize(Math.abs(randomGenerator.nextInt(10)));
    memberRegion.setDiskStoreName("ABC");
    memberRegion.setScope("DISTRIBUTED_NO_ACK");
    memberRegion.setDiskSynchronous(true);
    memberRegion.setRegionType(type);
    if(type.contains("PERSISTENT"))
      memberRegion.setPersistentEnabled(true);
    else
      memberRegion.setPersistentEnabled(false);
    if (count % 2 == 0){
      memberRegion.setWanEnabled(true);
    }
    else{
      memberRegion.setWanEnabled(false);
    }
    memberRegion.setWanEnabled(true);
    /*memberRegion.setSystemRegionEntryCount(Long.valueOf(String.valueOf(Math
        .abs(randomGenerator.nextInt(100)))));*/
    memberRegion.getMemberName().add(memName);
    memberRegion.setMemberCount(memberCount);
    return memberRegion;
  }


  private Client initMemberClient(int count, String host) {

    Client memberClient = new Client();
    Random r = new Random(System.currentTimeMillis());
    memberClient.setName("Name_" + count);
    long processCpuTime = (long) (r.nextDouble() * 100);
    memberClient.setProcessCpuTime(processCpuTime);
    memberClient.setCpuUsage(0);
    memberClient.setGets(Math.abs(r.nextInt(100)));
    memberClient.setHost(host);
    memberClient.setId(String.valueOf(1000 + count));
    memberClient.setPuts(Math.abs(r.nextInt(100)));
    memberClient.setCpus(Math.abs(r.nextInt(20)));
    memberClient.setQueueSize(Math.abs(r.nextInt(100)));
    if ((count % 2) == 0){
      memberClient.setStatus("up");
    }
    else{
      memberClient.setStatus("down");
    }
    memberClient.setThreads(Math.abs(r.nextInt(100)));
    memberClient
        .setUptime(Math.abs(System.currentTimeMillis() - r.nextLong()));

    return memberClient;
  }

  private Member initializeMember(String id, String name, boolean manager,
      boolean isCache, boolean isLocator, boolean isServer, String host) {
    Member m = new Member();

    m.setId(id);
    m.setName(name);

    //m.setHost(getHostName(System.currentTimeMillis()));
    m.setHost(host);

    m.setMaxHeapSize(247);

    Random r = new Random(System.currentTimeMillis());

    m.setCache(isCache);
    m.setLocator(isLocator);
    m.setServer(isServer);
    m.setManager(manager);

    m.setLoadAverage((double) Math.abs(r.nextInt(100)));
    m.setNumThreads(Math.abs(r.nextInt(100)));
    m.setGarbageCollectionCount((long) Math.abs(r.nextInt(100)));
    m.getGarbageCollectionSamples().add(m.getGarbageCollectionCount());

    m.setTotalFileDescriptorOpen((long) Math.abs(r.nextInt(100)));
    m.setTotalDiskUsage(Math.abs(r.nextInt(100)));


    m.setThroughputWrites(Math.abs(r.nextInt(10)));
    m.getThroughputWritesTrend().add(m.getThroughputWrites());

    GatewayReceiver gatewayReceiver = m.getGatewayReceiver();
    String port  = cluster.getPort();
    if(port==null || "".equals(port))
      port = "1099";
    gatewayReceiver.setListeningPort(Integer.parseInt(port));
    gatewayReceiver.setLinkThroughput(Math.abs(r.nextInt(10)));
    gatewayReceiver.setAvgBatchProcessingTime((long) Math.abs(r.nextInt(10)));
    gatewayReceiver.setId(String.valueOf(Math.abs(r.nextInt(10))));
    gatewayReceiver.setQueueSize(Math.abs(r.nextInt(10)));
    gatewayReceiver.setStatus(true);
    gatewayReceiver.setBatchSize(Math.abs(r.nextInt(10)));

    int gatewaySenderCount = Math.abs(r.nextInt(10));

    List<GatewaySender> list = m.getGatewaySenderList();

    for (int i = 0; i < gatewaySenderCount; i++) {
      list.add(createGatewaySenderCount(r));
    }

    Map<String, List<Member>> physicalToMember = cluster.getPhysicalToMember();

    List<Cluster.Member> memberArrList = physicalToMember.get(m.getHost());
    if (memberArrList != null){
      memberArrList.add(m);
    }
    else {
      ArrayList<Cluster.Member> memberList = new ArrayList<Cluster.Member>();
      memberList.add(m);
      physicalToMember.put(m.getHost(), memberList);
    }
    int memberCount = cluster.getMemberCount();memberCount++;cluster.setMemberCount(memberCount);
    return m;
  }

  private GatewaySender createGatewaySenderCount(Random r) {

    GatewaySender gatewaySender = new GatewaySender();

    gatewaySender.setBatchSize(Math.abs(r.nextInt(10)));
    gatewaySender.setId(String.valueOf(Math.abs(r.nextInt(10))));
    gatewaySender.setLinkThroughput(Math.abs(r.nextInt(10)));
    gatewaySender.setPersistenceEnabled(true);
    gatewaySender.setPrimary(true);
    gatewaySender.setQueueSize(Math.abs(r.nextInt(10)));
    gatewaySender.setSenderType(false);
    gatewaySender.setStatus(true);

    return gatewaySender;
  }

  /*
  private String getHostName(long rndSeed) {
    Random rnd = new Random(rndSeed);
    String hName = null;

    int index = Math.abs(rnd.nextInt(MAX_HOSTS));

    ArrayList<String> hostNames = cluster.getHostNames();

    if (hostNames.size() <= index) {
      hName = "host" + hostNames.size();
      hostNames.add(hName);
    } else {
      hName = hostNames.get(index);
    }

    Map<String, ArrayList<Member>> physicalToMember = cluster.getPhysicalToMember();

    ArrayList<Member> memberArrList = physicalToMember.get(hName);
    if (memberArrList != null) {
      if (memberArrList.size() > 4){
        hName = getHostName(rndSeed + rnd.nextLong());
      }
    }
    return hName;
  }*/

  private void refresh(Member m) {
    if(LOGGER.infoEnabled()){
      LOGGER.info(resourceBundle.getString("LOG_MSG_REFRESHING_MEMBER_DATA")+" : " + m.getName());
    }

    Random r = new Random(System.currentTimeMillis());

    m.setUptime(System.currentTimeMillis());
    m.setQueueBacklog("" + Math.abs(r.nextInt(500)));
    m.setCurrentHeapSize(Math.abs(r.nextInt(Math.abs((int) m.getMaxHeapSize()))));
    m.setTotalDiskUsage(Math.abs(r.nextInt(100)));

    double cpuUsage = r.nextDouble() * 100;
    m.getCpuUsageSamples().add(cpuUsage);
    m.setCpuUsage(cpuUsage);

    m.getHeapUsageSamples().add(m.getCurrentHeapSize());
    m.setLoadAverage((double) Math.abs(r.nextInt(100)));
    m.setNumThreads(Math.abs(r.nextInt(100)));
    m.setGarbageCollectionCount((long) Math.abs(r.nextInt(100)));
    m.getGarbageCollectionSamples().add(m.getGarbageCollectionCount());

    m.setTotalFileDescriptorOpen((long) Math.abs(r.nextInt(100)));

    m.setThroughputWrites(Math.abs(r.nextInt(10)));
    m.getThroughputWritesTrend().add(m.getThroughputWrites());

    m.setGetsRate(Math.abs(r.nextInt(5000)));
    m.getGetsPerSecond().add(m.getGetsRate());

    m.setPutsRate(Math.abs(r.nextInt(5000)));
    m.getPutsPerSecond().add(m.getPutsRate());

    Alert[] alerts = cluster.getAlertsList();
    List<Alert> alertsList = Arrays.asList(alerts);

    if (r.nextBoolean()) {
      // Generate alerts
      if (r.nextBoolean()) {
        if (r.nextInt(10) > 5) {
          alertsList.add(createAlert(Alert.SEVERE, m.getName(), alertsList.size()));
          if(alertsList.size() > ALERTS_MAX_SIZE){
            alertsList.remove(0);
          }
        }
      }

      if (r.nextBoolean()) {
        if (r.nextInt(10) > 5) {
          alertsList.add(createAlert(Alert.ERROR, m.getName(), alertsList.size()));
          if(alertsList.size() > ALERTS_MAX_SIZE){
            alertsList.remove(0);
          }
        }
      }

      if (r.nextBoolean()) {
        if (r.nextInt(10) > 5) {
          alertsList.add(createAlert(Alert.WARNING, m.getName(), alertsList.size()));
          if(alertsList.size() > ALERTS_MAX_SIZE){
            alertsList.remove(0);
          }
        }
      }
    }
  }

  private Alert createAlert(int sev, String memberName, int index) {

    Alert alert = new Alert();
    alert.setSeverity(sev);
    alert.setId(index);
    alert.setMemberName(memberName);
    alert.setTimestamp(new Date());

    switch (sev) {
    case Alert.SEVERE:
      alert.setDescription(PulseConstants.ALERT_DESC_SEVERE);
      break;
    case Alert.ERROR:
      alert.setDescription(PulseConstants.ALERT_DESC_ERROR);
      break;
    case Alert.WARNING:
      alert.setDescription(PulseConstants.ALERT_DESC_WARNING);
      break;
    }
    return alert;
  }

  @Override
  public ObjectNode executeQuery(String queryText, String members, int limit) {
    // TODO for Sushant/Sachin - Add implementation for MockUpdater for Automation
    return null;
  }

}
