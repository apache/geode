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
package org.apache.geode.internal.cache.wan.serial;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.WanTest;

// The tests here are to validate changes introduced because a distributed deadlock
// was found that caused issues for a production customer.
//
// There are 4 tests which use sender gateways with primaries on different
// JVM's. Two tests use replicated and two use partition regions and the
// the tests vary the conserve-sockets.
//
// currently the 4th test using PR, conserve-sockets=true hangs/fails and is commented
// out to prevent issues
@Category({WanTest.class})
public class SerialGatewaySenderDistributedDeadlockDUnitTest extends WANTestBase {

  public SerialGatewaySenderDistributedDeadlockDUnitTest() {
    super();
  }

  // Uses replicated regions and conserve-sockets=false
  @Test
  public void testPrimarySendersOnDifferentVMsReplicated() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstPeerLocator(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCachesWith(Boolean.FALSE, nyPort, lnPort);

    createSerialSenders();

    createReplicatedRegions(nyPort);

    // get one primary sender on vm4 and another primary on vm5
    // the startup order matters here
    startSerialSenders();

    // exercise region and gateway operations with different messaging
    exerciseWANOperations();
    AsyncInvocation invVM4transaction =
        vm4.invokeAsync("doTxPutsAsync", () -> WANTestBase.doTxPuts(getTestMethodName() + "_RR"));
    AsyncInvocation invVM5transaction =
        vm5.invokeAsync("doTxPutsAsync", () -> WANTestBase.doTxPuts(getTestMethodName() + "_RR"));
    AsyncInvocation invVM4 =
        vm4.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    AsyncInvocation invVM5 =
        vm5.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    exerciseFunctions();

    try {
      invVM4transaction.join();
      invVM5transaction.join();
      invVM4.join();
      invVM5.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
  }

  // Uses partitioned regions and conserve-sockets=false
  @Test
  public void testPrimarySendersOnDifferentVMsPR() throws Exception {
    Integer lnPort =
        vm0.invoke("createFirstPeerLocator", () -> WANTestBase.createFirstPeerLocator(1));

    Integer nyPort = vm1.invoke("createFirstRemoteLocator",
        () -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCachesWith(Boolean.FALSE, nyPort, lnPort);

    createSerialSenders();

    createPartitionedRegions(nyPort);

    startSerialSenders();

    exerciseWANOperations();
    AsyncInvocation invVM4transaction =
        vm4.invokeAsync("doTxPutsPRAsync", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
            .doTxPutsPR(getTestMethodName() + "_RR", 100, 1000));
    AsyncInvocation invVM5transaction =
        vm5.invokeAsync("doTxPutsPRAsync", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
            .doTxPutsPR(getTestMethodName() + "_RR", 100, 1000));

    AsyncInvocation invVM4 =
        vm4.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    AsyncInvocation invVM5 =
        vm5.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    exerciseFunctions();

    invVM4transaction.join();
    invVM5transaction.join();
    invVM4.join();
    invVM5.join();
  }

  // Uses replicated regions and conserve-sockets=true
  @Test
  public void testPrimarySendersOnDifferentVMsReplicatedSocketPolicy() throws Exception {

    Integer lnPort =
        vm0.invoke("createFirstPeerLocator", () -> WANTestBase.createFirstPeerLocator(1));

    Integer nyPort = vm1.invoke("createFirstRemoteLocator",
        () -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCachesWith(Boolean.TRUE, nyPort, lnPort);

    createSerialSenders();

    createReplicatedRegions(nyPort);

    // get one primary sender on vm4 and another primary on vm5
    // the startup order matters here
    startSerialSenders();

    // exercise region and gateway operations with messaging
    exerciseWANOperations();
    AsyncInvocation invVM4transaction =
        vm4.invokeAsync("doTxPutsAsync", () -> WANTestBase.doTxPuts(getTestMethodName() + "_RR"));
    AsyncInvocation invVM5transaction =
        vm5.invokeAsync("doTxPutsAsync", () -> WANTestBase.doTxPuts(getTestMethodName() + "_RR"));

    AsyncInvocation invVM4 =
        vm4.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    AsyncInvocation invVM5 =
        vm5.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    exerciseFunctions();

    invVM4transaction.join();
    invVM5transaction.join();
    invVM4.join();
    invVM5.join();
  }

  // Uses partitioned regions and conserve-sockets=true
  // this always causes a distributed deadlock
  @Test
  public void testPrimarySendersOnDifferentVMsPRSocketPolicy() throws Exception {
    Integer lnPort =
        vm0.invoke("createFirstPeerLocator", () -> WANTestBase.createFirstPeerLocator(1));

    Integer nyPort = vm1.invoke("createFirstRemoteLocator",
        () -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCachesWith(Boolean.TRUE, nyPort, lnPort);

    createSerialSenders();

    createPartitionedRegions(nyPort);

    startSerialSenders();

    exerciseWANOperations();
    AsyncInvocation invVM4transaction =
        vm4.invokeAsync("doTxPutsPRAsync", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
            .doTxPutsPR(getTestMethodName() + "_RR", 100, 1000));
    AsyncInvocation invVM5transaction =
        vm5.invokeAsync("doTxPutsPRAsync", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
            .doTxPutsPR(getTestMethodName() + "_RR", 100, 1000));

    AsyncInvocation invVM4 =
        vm4.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    AsyncInvocation invVM5 =
        vm5.invokeAsync("doPutsAsync", () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    exerciseFunctions();

    invVM4transaction.join();
    invVM5transaction.join();
    invVM4.join();
    invVM5.join();
  }

  // **************************************************************************
  // Utility methods used by tests
  // **************************************************************************
  private void createReplicatedRegions(Integer nyPort) throws Exception {
    // create receiver
    vm2.invoke("createReplicatedRegion",
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, false));
    vm2.invoke("createReceiver", () -> WANTestBase.createReceiver());

    // create senders
    vm4.invoke("createReplicatedRegion",
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1,ln2", false));

    vm5.invoke("createReplicatedRegion",
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1,ln2", false));
  }

  private void createCachesWith(Boolean socketPolicy, Integer nyPort, Integer lnPort) {
    vm2.invoke("createCacheConserveSockets",
        () -> WANTestBase.createCacheConserveSockets(socketPolicy, nyPort));

    vm4.invoke("createCacheConserveSockets",
        () -> WANTestBase.createCacheConserveSockets(socketPolicy, lnPort));

    vm5.invoke("createCacheConserveSockets",
        () -> WANTestBase.createCacheConserveSockets(socketPolicy, lnPort));
  }

  private void exerciseFunctions() throws Exception {
    // do function calls that use a shared connection
    for (int x = 0; x < 1000; x++) {
      // setting it to Boolean.TRUE it should pass the test
      vm4.invoke("doFunctionPuts", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
          .doFunctionPuts(getTestMethodName() + "_RR", 1, Boolean.TRUE));
      vm5.invoke("doFunctionPuts", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
          .doFunctionPuts(getTestMethodName() + "_RR", 1, Boolean.TRUE));
    }
    for (int x = 0; x < 1000; x++) {
      // setting the Boolean.FALSE below will cause a deadlock in some GFE versions
      // setting it to Boolean.TRUE as above it should pass the test
      // this is similar to the customer found distributed deadlock
      vm4.invoke("doFunctionPuts2", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
          .doFunctionPuts(getTestMethodName() + "_RR", 1, Boolean.FALSE));
      vm5.invoke("doFunctionPuts2", () -> SerialGatewaySenderDistributedDeadlockDUnitTest
          .doFunctionPuts(getTestMethodName() + "_RR", 1, Boolean.FALSE));
    }
  }

  private void createPartitionedRegions(Integer nyPort) throws Exception {
    // create remote receiver
    vm2.invoke("createPartitionedRegion",
        () -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_RR", "", 0, 113, false));

    vm2.invoke("createReceiver", () -> WANTestBase.createReceiver());

    // create sender vms
    vm4.invoke("createPartitionedRegion",
        () -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_RR", "ln1,ln2", 1,
            113, false));

    vm5.invoke("createPartitionedRegion",
        () -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_RR", "ln1,ln2", 1,
            113, false));
  }

  private void exerciseWANOperations() throws Exception {
    // note - some of these should be made async to truly exercise the
    // messaging between the WAN gateways and members

    // exercise region and gateway operations
    vm4.invoke("exerciseWANOperations.doPuts",
        () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 100));
    vm5.invoke("exerciseWANOperations.doPuts",
        () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 100));
    vm4.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 100));
    vm2.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 100));

    vm5.invoke("exerciseWANOperations.doDestroys",
        () -> WANTestBase.doDestroys(getTestMethodName() + "_RR", 100));
    vm5.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));
    vm2.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));

    vm4.invoke("exerciseWANOperations.doPuts2",
        () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 100));
    vm5.invoke("exerciseWANOperations.doPuts2",
        () -> WANTestBase.doPuts(getTestMethodName() + "_RR", 100));
    vm4.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 100));
    vm2.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 100));

    vm4.invoke("exerciseWANOperations.doInvalidates",
        () -> SerialGatewaySenderDistributedDeadlockDUnitTest
            .doInvalidates(getTestMethodName() + "_RR", 100, 100));
    vm4.invoke("exerciseWANOperations.doPutAll",
        () -> WANTestBase.doPutAll(getTestMethodName() + "_RR", 100, 10));
    vm5.invoke("exerciseWANOperations.doPutAll",
        () -> WANTestBase.doPutAll(getTestMethodName() + "_RR", 100, 10));
    vm4.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm2.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

    vm4.invoke("exerciseWANOperations.doDestroys",
        () -> WANTestBase.doDestroys(getTestMethodName() + "_RR", 1000));
    vm5.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));
    vm2.invoke("exerciseWANOperations.validateRegionSize",
        () -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 0));

    vm4.invoke("exerciseWANOperations.doPutsPDXSerializable",
        () -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 100));
    vm5.invoke("exerciseWANOperations.validateRegionSize_PDX",
        () -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 100));
    vm2.invoke("exerciseWANOperations.validateRegionSize_PDX",
        () -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 100));
  }

  private void startSerialSenders() throws Exception {
    // get one primary sender on vm4 and another primary on vm5
    // the startup order matters here so that primaries are
    // on different JVM's
    vm4.invoke("start primary sender", () -> WANTestBase.startSender("ln1"));

    vm5.invoke("start primary sender", () -> WANTestBase.startSender("ln2"));

    // start secondaries
    vm5.invoke("start secondary sender", () -> WANTestBase.startSender("ln1"));

    vm4.invoke("start secondary sender", () -> WANTestBase.startSender("ln2"));
  }

  private void createSerialSenders() throws Exception {

    vm4.invoke("create primary sender",
        () -> WANTestBase.createSender("ln1", 2, false, 100, 10, false, false, null, true));

    vm5.invoke("create secondary sender",
        () -> WANTestBase.createSender("ln1", 2, false, 100, 10, false, false, null, true));

    vm4.invoke("create primary sender",
        () -> WANTestBase.createSender("ln2", 2, false, 100, 10, false, false, null, true));

    vm5.invoke("create secondary sender",
        () -> WANTestBase.createSender("ln2", 2, false, 100, 10, false, false, null, true));
  }

  public static void doFunctionPuts(String name, int num, Boolean useThreadOwnedSocket)
      throws Exception {
    Region region = CacheFactory.getAnyInstance().getRegion(name);
    FunctionService.registerFunction(new TestFunction());
    Execution exe = FunctionService.onRegion(region);
    for (int x = 0; x < num; x++) {
      exe.setArguments(useThreadOwnedSocket)
          .execute("org.apache.geode.internal.cache.wan.serial.TestFunction");
    }
  }

  public static void doTxPutsPR(String regionName, int numPuts, int size) throws Exception {
    Region r = cache.getRegion(SEPARATOR + regionName);
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    for (int x = 0; x < numPuts; x++) {
      int temp = (int) (Math.floor(Math.random() * size));
      try {
        mgr.begin();
        r.put(temp, temp);
        mgr.commit();
      } catch (org.apache.geode.cache.TransactionDataNotColocatedException txe) {
        // ignore colocation issues or primary bucket issues
      } catch (org.apache.geode.cache.CommitConflictException cce) {
        // ignore - conflicts are ok and expected
      }
    }
  }

  public static void doInvalidates(String regionName, int numInvalidates, int size)
      throws Exception {
    Region r = cache.getRegion(SEPARATOR + regionName);
    for (int x = 0; x < numInvalidates; x++) {
      int temp = (int) (Math.floor(Math.random() * size));
      try {
        if (r.containsValueForKey(temp)) {
          r.invalidate(temp);
        }
      } catch (org.apache.geode.cache.EntryNotFoundException entryNotFoundException) {
        // ignore as an entry may not exist
      }
    }
  }

}


class TestFunction implements Function {

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public void execute(FunctionContext fc) {
    boolean option = (Boolean) fc.getArguments();
    if (option) {
      DistributedSystem.setThreadsSocketPolicy(false);
    }
    RegionFunctionContext context = (RegionFunctionContext) fc;
    Region local = context.getDataSet();
    local.put(randKeyValue(10), randKeyValue(10000));
  }

  @Override
  public String getId() {
    return getClass().getName();
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  private int randKeyValue(int size) {
    double temp = Math.floor(Math.random() * size);
    return (int) temp;
  }
}
