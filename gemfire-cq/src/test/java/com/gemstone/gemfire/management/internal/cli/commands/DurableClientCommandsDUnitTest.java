/*
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
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.Properties;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;



public class DurableClientCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;
  final String regionName = "stocks";
  final String cq1 = "cq1";
  final String cq2 = "cq2";
  final String cq3 = "cq3";
  final String clientName = "dc1";
  
  public DurableClientCommandsDUnitTest(String name) {
    super(name);
  }
 
  public void testListDurableClientCqs() throws Exception {
    setupSystem();
    setupCqs();
    
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, clientName);
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    assertTrue(resultAsString.contains(cq1));
    assertTrue(resultAsString.contains(cq2));
    assertTrue(resultAsString.contains(cq3));
    
    closeCq(cq1);
    closeCq(cq2);
    closeCq(cq3);
    
    csb = new CommandStringBuilder(CliStrings.LIST_DURABLE_CQS);
    csb.addOption(CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, clientName);
    commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    String errorMessage = CliStrings.format(CliStrings.LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT, clientName);
    assertTrue(resultAsString.contains(errorMessage));
  }
  
  public void testCloseDurableClients() throws Exception {
    setupSystem();
    setupCqs();
    closeDurableClient();
    
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, clientName);
    String commandString = csb.toString();
    long giveUpTime = System.currentTimeMillis() + 20000;
    CommandResult commandResult = null;
    String resultAsString = null;
    do {
      writeToLog("Command String : ", commandString);
      commandResult = executeCommand(commandString);
      resultAsString = commandResultToString(commandResult);
    } while (resultAsString.contains("Cannot close a running durable client")
        && giveUpTime > System.currentTimeMillis());
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    
    //Execute again to see the error condition
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    String errorMessage = CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, clientName);
    assertTrue(resultAsString.contains(errorMessage));
  }
  
  
  public void testCloseDurableCQ() throws Exception{
    setupSystem();
    setupCqs();
  
    closeDurableClient();
    
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, clientName);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, cq1);
    String commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    
    csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CQS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID, clientName);
    csb.addOption(CliStrings.CLOSE_DURABLE_CQS__NAME, cq1);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result : ", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    
  }
    
//  public void testRepeat() throws Exception {
//    long endTime = System.currentTimeMillis() + (75 * 60000);
//    while (endTime > System.currentTimeMillis()) {
//      testCountSubscriptionQueueSize();
//      tearDown();
//      setUp();
//    }
//    testCountSubscriptionQueueSize();
//  }
//  
  public void testCountSubscriptionQueueSize() throws Exception {
    setupSystem();
    setupCqs();
    doPuts(regionName, Host.getHost(0).getVM(1));
    
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, clientName);
    String commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    assertTrue(resultAsString.contains("4"));
    
    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, clientName);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, cq3);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    
    //CLOSE all the  cqs
    closeCq(cq1);
    closeCq(cq2);
    closeCq(cq3);
    
    //Run the commands again
    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, clientName);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME, cq1);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    String errorMessage = CliStrings.format(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_NOT_FOUND, clientName, cq1);
    assertTrue(resultAsString.contains(errorMessage));
    
    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, clientName);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.OK.equals(commandResult.getStatus()));
    
    //Disconnect the client
    closeDurableClient();
    
    //Close the client
    csb = new CommandStringBuilder(CliStrings.CLOSE_DURABLE_CLIENTS);
    csb.addOption(CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, clientName);
    commandString = csb.toString();
    // since it can take the server a bit to know that the client has disconnected
    // we loop here
    long giveUpTime = System.currentTimeMillis() + 20000;
    do {
      writeToLog("Command String : ", commandString);
      commandResult = executeCommand(commandString);
      resultAsString = commandResultToString(commandResult);
    } while (resultAsString.contains("Cannot close a running durable client")
        && giveUpTime > System.currentTimeMillis());
    writeToLog("Command Result :\n", resultAsString);
    assertTrue("failed executing" + commandString + "; result = "+resultAsString, Status.OK.equals(commandResult.getStatus()));
    
    csb = new CommandStringBuilder(CliStrings.COUNT_DURABLE_CQ_EVENTS);
    csb.addOption(CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, clientName);
    commandString = csb.toString();
    writeToLog("Command String : ", commandString);
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command Result :\n", resultAsString);
    assertTrue(Status.ERROR.equals(commandResult.getStatus()));
    assertTrue(resultAsString.contains(CliStrings.format(CliStrings.NO_CLIENT_FOUND_WITH_CLIENT_ID, clientName)));
  }
  
  private void writeToLog(String text, String resultAsString) {
    getLogWriter().info(getUniqueName() + ": " + text + "\n" + resultAsString);
  }
  
  private void setupSystem() throws Exception {
    disconnectAllFromDS();
    final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    createDefaultSetup(getServerProperties());
    
    final VM manager = Host.getHost(0).getVM(0);
    final VM server1 = Host.getHost(0).getVM(1);
    final VM client1 = Host.getHost(0).getVM(2);
    
    startCacheServer(server1, port[0], false, regionName);
    startDurableClient(client1, server1, port[0], clientName, "300");
  }
  
  /**
   * Close the cq from the client-side 
   * @param cqName , Name of the cq which is to be close.
   */
  private void closeCq(final String cqName) {
    final VM vm2 = Host.getHost(0).getVM(2);
    vm2.invoke(new SerializableCallable() {
      public Object call() {
        QueryService qs = getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();
        try {
         qs.getCq(cqName).close();
          
        }
        catch (CqException e) {
          e.printStackTrace();
          return false;
        }
        return true;
      }
    });
  }
  
  private void setupCqs() {
    final VM vm2 = Host.getHost(0).getVM(2);
    vm2.invoke(new SerializableCallable() {
      public Object call() {
        QueryService qs = getCache().getQueryService();
        CqAttributesFactory cqAf = new CqAttributesFactory();
        try {
          qs.newCq(cq1, "select * from /" + regionName, cqAf.create(), true).execute();
          qs.newCq(cq2, "select * from /" + regionName + " where id = 1", cqAf.create(), true).execute();
          qs.newCq(cq3, "select * from /" + regionName + " where id > 2", cqAf.create(), true).execute();
        }
        catch (CqException e) {
          e.printStackTrace();
          return false;
        }
        catch (CqExistsException e) {
          e.printStackTrace();

          return false;
        }
        catch(RegionNotFoundException e) {
          e.printStackTrace();

          return false; 
        }
        return true;
      }
    });
  }
  
  private void startCacheServer(VM server, final int port, 
      final boolean createPR, final String regionName) throws Exception {

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getServerProperties());
        
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        AttributesFactory factory = new AttributesFactory();
        if (createPR) {
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          paf.setTotalNumBuckets(11);
          factory.setPartitionAttributes(paf.create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        } else {
          assertTrue(region instanceof DistributedRegion);
        }
        CacheServer cacheServer = getCache().addCacheServer();
        cacheServer.setPort(port);
        cacheServer.start();
       
        return null;
      }
    });
  }
  
  private void startDurableClient(VM client, final VM server, final int port,
      final String durableClientId, final String durableClientTimeout) {
    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        Properties props = getClientProps(durableClientId, durableClientTimeout);
        getSystem(props);
        
        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(server.getHost()), port);
        ccf.setPoolSubscriptionEnabled(true);
        
        ClientCache cache = (ClientCache)getClientCache(ccf);
      }
    });
  }
  
  /* Does few puts on the region on the server
   * 
   */
  
  private void doPuts(final String regionName, VM server) {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        Region region = cache.getRegion(regionName);
        Portfolio p1 = new Portfolio();
        p1.ID = 1;
        p1.names = new String [] {"AAPL", "VMW"};
        
        Portfolio p2 = new Portfolio();
        p2.ID = 2;
        p2.names = new String [] {"EMC", "IBM"};
        
        Portfolio p3 = new Portfolio();
        p3.ID = 5;
        p3.names = new String [] {"DOW","TON"};
        
        Portfolio p4 = new Portfolio();
        p4.ID = 5;
        p4.names = new String [] {"ABC", "EBAY"};
        
        region.put("p1", p1);
        region.put("p2", p2);
        region.put("p3", p3);
        region.put("p4", p4);
        return null;
      }
    });
  }
  
  //Closes the durable-client from the client side.
  private void closeDurableClient() {
    final VM client = Host.getHost(0).getVM(2);
      client.invoke(new CacheSerializableRunnable("Stop client") {
        public void run2() throws CacheException {
          ClientCacheFactory.getAnyInstance().close(true);
        }
      });
  }
  
  protected Properties getClientProps(String durableClientId, String durableClientTimeout) {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    p.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME, durableClientId);
    p.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, String.valueOf(durableClientTimeout));
    return p;
  }

  protected Properties getServerProperties() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["+getDUnitLocatorPort()+"]");
    return p;
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    Host.getHost(0).getVM(0).invoke(CacheServerTestUtil.class, "closeCache");
    Host.getHost(0).getVM(1).invoke(CacheServerTestUtil.class, "closeCache");
    Host.getHost(0).getVM(2).invoke(CacheServerTestUtil.class, "closeCache");
  }
}
