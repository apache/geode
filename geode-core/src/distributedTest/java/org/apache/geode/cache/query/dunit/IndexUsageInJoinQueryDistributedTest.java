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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({OQLIndexTest.class})
public class IndexUsageInJoinQueryDistributedTest implements Serializable {

  private MemberVM locator;

  private MemberVM server;

  private ClientVM client;

  private static final String PRODUCT_REGION_NAME = "product";

  private static final String INSTRUMENT_REGION_NAME = "instrument";

  @Rule
  public ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Test
  public void testSecondIndexUsedWhenExecutingJoinQuery() throws Exception {
    // Start Locator
    locator = clusterRule.startLocatorVM(0);

    // Start server
    server = clusterRule.startServerVM(1, locator.getPort());

    // Create server regions
    server.invoke(this::createServerRegionsAndIndexes);

    // Start client
    client = clusterRule.startClientVM(2, c -> c.withLocatorConnection(locator.getPort()));

    // Create client regions
    client.invoke(this::createClientRegions);

    // Load regions
    int numProducts = 1000;
    int numInstruments = 100;
    client.invoke(() -> loadRegions(numProducts, numInstruments));

    // Get number of deserializations before query
    long deserializationsBeforeQuery = server.invoke(this::getDeserializations);

    // Execute query
    client.invoke(() -> executeQuery(numInstruments));

    // Get number of deserializations after query
    long deserializationsAfterQuery = server.invoke(this::getDeserializations);

    // Verify number of deserializations during query
    // 1 for each instrument in the instrumentTypesIndex (100)
    // For each instrument, 1 product in the productIdKeyIndex (100)
    // 1 for the bind parameter
    assertThat(deserializationsAfterQuery - deserializationsBeforeQuery)
        .isEqualTo((numInstruments * 2) + 1);
  }

  private void createServerRegionsAndIndexes()
      throws RegionNotFoundException, IndexNameConflictException, IndexExistsException {
    Cache cache = ClusterStartupRule.getCache();
    QueryService queryService = cache.getQueryService();
    Region productRegion = cache.createRegionFactory(REPLICATE).create(PRODUCT_REGION_NAME);
    queryService.createKeyIndex("productIdKeyIndex", "productId", productRegion.getFullPath());
    Region instrumentRegion = cache.createRegionFactory(REPLICATE).create(INSTRUMENT_REGION_NAME);
    queryService.createIndex("instrumentTypesIndex", "types['TYPE']",
        instrumentRegion.getFullPath());
  }

  private void createClientRegions() {
    ClientCache cache = ClusterStartupRule.getClientCache();
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(PRODUCT_REGION_NAME);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(INSTRUMENT_REGION_NAME);
  }

  private void loadRegions(int numProducts, int numInstruments) {
    ClientCache cache = ClusterStartupRule.getClientCache();
    Region productRegion = cache.getRegion(PRODUCT_REGION_NAME);
    Region instrumentRegion = cache.getRegion(INSTRUMENT_REGION_NAME);

    // Load product region
    for (int i = 0; i < numProducts; i++) {
      String productId = String.valueOf(i);
      Product product = new Product(productId);
      productRegion.put(productId, product);
    }

    // Load instrument region
    for (int i = 0; i < numInstruments; i++) {
      String instrumentId = String.valueOf(i);
      Map<String, String> types = new HashMap<>();
      types.put("TYPE", "TYPE1");
      Instrument instrument = new Instrument(instrumentId, "0", types);
      instrumentRegion.put(instrumentId, instrument);
    }
  }

  private void executeQuery(int numInstruments) throws Exception {
    ClientCache cache = ClusterStartupRule.getClientCache();
    Region productRegion = cache.getRegion(PRODUCT_REGION_NAME);
    Region instrumentRegion = cache.getRegion(INSTRUMENT_REGION_NAME);
    String queryString =
        "<trace> select i,p from " + instrumentRegion.getFullPath() + " i, "
            + productRegion.getFullPath()
            + " p where i.productId = p.productId and i.types['TYPE']=$1";
    Query query = ClusterStartupRule.getClientCache().getQueryService().newQuery(queryString);
    Collection results = (Collection) query.execute(new Object[] {"TYPE1"});
    assertThat(results.size()).isEqualTo(numInstruments);
  }

  private long getDeserializations() {
    DistributionStats distributionStats =
        (DistributionStats) ((InternalDistributedSystem) ClusterStartupRule.getCache()
            .getDistributedSystem()).getDistributionManager().getStats();
    return distributionStats.getStats().getLong("deserializations");
  }

  public static class Product implements PdxSerializable {

    public String productId;

    public Product() {}

    public Product(String productId) {
      this.productId = productId;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("productId", productId);
    }

    @Override
    public void fromData(PdxReader reader) {
      productId = reader.readString("productId");
    }
  }

  public static class Instrument implements PdxSerializable {

    private Map<String, String> types;
    private String instrumentId;
    private String productId;

    public Instrument() {}

    public Instrument(String instrumentId, String productId, Map<String, String> types) {
      this.instrumentId = instrumentId;
      this.productId = productId;
      this.types = types;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("instrumentId", instrumentId);
      writer.writeString("productId", productId);
      writer.writeObject("types", types);
    }

    @Override
    public void fromData(PdxReader reader) {
      instrumentId = reader.readString("instrumentId");
      productId = reader.readString("productId");
      types = (Map<String, String>) reader.readObject("types");
    }
  }
}
