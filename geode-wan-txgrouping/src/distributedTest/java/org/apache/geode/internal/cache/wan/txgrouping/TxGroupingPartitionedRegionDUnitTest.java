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
package org.apache.geode.internal.cache.wan.txgrouping;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class TxGroupingPartitionedRegionDUnitTest extends TxGroupingBaseDUnitTest {
  @Test
  @Parameters({"true", "false"})
  public void testPartitionedRegionPropagationWithGroupTransactionEventsAndMixOfEventsInAndNotInTransactions(
      boolean isParallel)
      throws Exception {
    newYorkServerVM.invoke("create New York server", () -> {
      startServerWithReceiver(newYorkLocatorPort, newYorkReceiverPort, true);
      createCustomerOrderShipmentPartitionedRegion(null);
    });

    int batchSize = 10;
    for (VM server : londonServersVM) {
      server.invoke("create London server " + server.getId(), () -> {
        startServerWithSender(server.getId(), londonLocatorPort, newYorkId, newYorkName, isParallel,
            true,
            batchSize, isParallel ? 2 : 1);
        createCustomerOrderShipmentPartitionedRegion(newYorkName);
        GatewaySender sender = cacheRule.getCache().getGatewaySender(newYorkName);
        await().untilAsserted(() -> assertThat(isRunning(sender)).isTrue());
      });
    }

    int customers = 4;
    int transactionsPerCustomer = 1000;
    int ordersPerCustomerNotInTransactions = 1000;

    final Map<Object, Object> keyValuesNotInTransactions = new HashMap<>();
    for (int custId = 0; custId < customers; custId++) {
      for (int i = 0; i < ordersPerCustomerNotInTransactions; i++) {
        CustId custIdObject = new CustId(custId);
        OrderId orderId =
            new OrderId(i + ordersPerCustomerNotInTransactions * customers, custIdObject);
        keyValuesNotInTransactions.put(orderId, new Order());
      }
    }

    // eventsPerTransaction is 1 (orders) + 3 (shipments)
    int eventsPerTransaction = 4;
    List<AsyncInvocation<Void>> putsInTransactionsInvocationList = new ArrayList<>(customers);
    for (int i = 0; i < customers; i++) {
      final int customerId = i;
      putsInTransactionsInvocationList.add(
          londonServer1VM.invokeAsync(
              () -> doOrderAndShipmentPutsInsideTransactions(customerId, eventsPerTransaction,
                  transactionsPerCustomer)));
    }

    AsyncInvocation<Void> putsNotInTransactionsInvocation =
        londonServer2VM.invokeAsync(
            () -> putGivenKeyValues(orderRegionName, keyValuesNotInTransactions));

    for (AsyncInvocation<Void> putsInTransactionInvocation : putsInTransactionsInvocationList) {
      putsInTransactionInvocation.await();
    }
    putsNotInTransactionsInvocation.await();

    int entries =
        ordersPerCustomerNotInTransactions * customers + transactionsPerCustomer * customers;

    for (VM londonServer : londonServersVM) {
      londonServer.invoke(() -> validateRegionSize(orderRegionName, entries));
    }

    newYorkServerVM.invoke(() -> validateRegionSize(orderRegionName, entries));

    for (VM londonServer : londonServersVM) {
      londonServer.invoke(() -> checkConflatedStats(newYorkName, 0));
    }

    for (VM londonServer : londonServersVM) {
      londonServer.invoke(() -> validateGatewaySenderQueueAllBucketsDrained(newYorkName));
    }
  }
}
