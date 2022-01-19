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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.Serializable;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({OQLIndexTest.class})
public class InequalityQueryWithRebalanceDistributedTest implements Serializable {

  private String regionName;

  private static MemberVM locator;

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0);
    gfsh.connectAndVerify(locator);
  }

  @Before
  public void before() throws Exception {
    regionName = getClass().getSimpleName() + "_" + testName.getMethodName() + "_region";
  }

  @Test
  public void testArbitraryBucketIndexUpdatedAfterBucketMoved() {
    // Start server1
    MemberVM server1 = cluster.startServerVM(1, locator.getPort());

    // Create the region
    createRegion();

    // Create the index
    createIndex();

    // Load entries
    server1.invoke(this::loadRegion);

    // Start server2
    cluster.startServerVM(2, locator.getPort());

    // Wait for server2 to create the region MBean
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 2);

    // Invoke rebalance
    rebalance();

    // Execute query
    executeQuery();
  }

  private void createRegion() {
    gfsh.executeAndAssertThat("create region --name=" + regionName
        + " --type=PARTITION  --total-num-buckets=2 --partition-resolver="
        + IsolatingPartitionResolver.class.getName()).statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 1);
  }

  private void createIndex() {
    gfsh.executeAndAssertThat("create index --region=" + regionName
        + " --name=tradeStatus --expression=tradeStatus.toString()").statusIsSuccess();
  }

  private void loadRegion() {
    Region<Integer, Trade> region = ClusterStartupRule.getCache().getRegion(regionName);
    IntStream.range(0, 10).forEach(i -> region.put(i,
        new Trade(String.valueOf(i), "aId1", i % 2 == 0 ? TradeStatus.OPEN : TradeStatus.CLOSED)));
  }

  private void rebalance() {
    gfsh.executeAndAssertThat("rebalance").statusIsSuccess();
  }

  private void executeQuery() {
    gfsh.executeAndAssertThat("query --query=\"SELECT * FROM " + SEPARATOR + regionName
        + " WHERE arrangementId = 'aId1' AND tradeStatus.toString() != 'CLOSED'\"")
        .statusIsSuccess();
  }

  public enum TradeStatus {
    OPEN,
    CLOSED
  }

  public static class Trade implements PdxSerializable {

    public String id;

    public String arrangementId;

    public TradeStatus tradeStatus;

    public Trade() {}

    public Trade(String id, String arrangementId, TradeStatus tradeStatus) {
      this.id = id;
      this.arrangementId = arrangementId;
      this.tradeStatus = tradeStatus;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("id", id);
      writer.writeString("arrangementId", arrangementId);
      writer.writeObject("tradeStatus", tradeStatus);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      arrangementId = reader.readString("arrangementId");
      tradeStatus = (TradeStatus) reader.readObject("tradeStatus");
    }
  }

  public static class IsolatingPartitionResolver implements PartitionResolver<Integer, Trade> {

    public IsolatingPartitionResolver() {}

    public Object getRoutingObject(EntryOperation<Integer, Trade> operation) {
      // Route key=0 to bucket 0 and all other keys to bucket 1
      return operation.getKey() == 0 ? 0 : 1;
    }

    public String getName() {
      return getClass().getSimpleName();
    }
  }
}
