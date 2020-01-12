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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * Extracted from {@link PRQueryDistributedTest}.
 */
@Category(OQLIndexTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PRQueryWithIndexAndPdxDistributedTest implements Serializable {

  private static final String REGION_NAME =
      PRQueryWithIndexAndPdxDistributedTest.class.getSimpleName();

  private VM vm0;
  private VM vm1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);

    vm0.invoke(() -> cacheRule.createCache());
    vm1.invoke(() -> cacheRule.createCache());
  }

  @Test
  @Parameters({"HASH_INDEX", "RANGE_INDEX", "RANGE_INDEX_WITH_PDX"})
  @TestCaseName("{method}({params})")
  public void createIndexDoesNotDeserializePdxObjects(IndexCreation indexCreation) {
    // the 3 parameters from IndexCreation enum:
    SerializableRunnableIF createIndex = () -> {
      indexCreation.createIndex(cacheRule.getCache());
    };
    PdxAssetFactory valueSupplier = indexCreation.valueSupplier();
    String queryString = indexCreation.queryString();

    // the test:
    vm0.invoke(() -> {
      PartitionAttributesFactory paf = new PartitionAttributesFactory().setTotalNumBuckets(10);
      cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .setPartitionAttributes(paf.create()).create(REGION_NAME);
    });
    vm1.invoke(() -> {
      PartitionAttributesFactory paf = new PartitionAttributesFactory().setTotalNumBuckets(10);
      cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
          .setPartitionAttributes(paf.create()).create(REGION_NAME);
    });

    // Do Puts. These objects cannot be deserialized because they throw an exception
    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(REGION_NAME);
      region.put(0, new PdxNotDeserializableAsset(0, "B"));
      region.put(10, new PdxNotDeserializableAsset(1, "B"));
      region.put(1, new PdxNotDeserializableAsset(1, "B"));
      IntStream.range(11, 100).forEach(i -> region.put(i, valueSupplier.getAsset(i)));
    });

    // If this tries to deserialize the assets, it will fail
    vm0.invoke(createIndex);

    vm0.invoke(() -> {
      QueryService queryService = cacheRule.getCache().getQueryService();
      Query query = queryService.newQuery(queryString); // enum
      SelectResults<Struct> results = (SelectResults) query.execute();

      assertThat(results).hasSize(3);

      Index index = queryService.getIndex(cacheRule.getCache().getRegion(REGION_NAME),
          "ContractDocumentIndex");

      assertThat(index.getStatistics().getTotalUses()).isEqualTo(1);
    });
  }

  private enum IndexCreation {
    HASH_INDEX((cache) -> {
      try {
        cache.getQueryService().createHashIndex("ContractDocumentIndex", "document",
            Region.SEPARATOR + REGION_NAME);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "select assetId, document from " + Region.SEPARATOR + REGION_NAME
        + " where document='B' limit 1000",
        i -> new PdxNotDeserializableAsset(i, Integer.toString(i))),

    RANGE_INDEX((cache) -> {
      try {
        cache.getQueryService().createIndex("ContractDocumentIndex", "ref",
            Region.SEPARATOR + REGION_NAME + " r, r.references ref");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "select r.assetId, r.document from " + Region.SEPARATOR + REGION_NAME
        + " r, r.references ref where ref='B_2' limit 1000",
        i -> new PdxNotDeserializableAsset(i, Integer.toString(i))),

    RANGE_INDEX_WITH_PDX((cache) -> {
      try {
        cache.getQueryService().createIndex("ContractDocumentIndex", "ref",
            Region.SEPARATOR + REGION_NAME + " r, r.references ref");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "select r from " + Region.SEPARATOR + REGION_NAME
        + " r, r.references ref where ref='B_2' limit 1000",
        i -> new PdxAsset(i, Integer.toString(i)));

    private final Consumer<Cache> strategy;
    private final String queryString;
    private final PdxAssetFactory valueSupplier;

    IndexCreation(Consumer<Cache> strategy, String queryString, PdxAssetFactory valueSupplier) {
      this.strategy = strategy;
      this.queryString = queryString;
      this.valueSupplier = valueSupplier;
    }

    void createIndex(Cache cache) {
      strategy.accept(cache);
    }

    String queryString() {
      return queryString;
    }

    PdxAssetFactory valueSupplier() {
      return valueSupplier;
    }
  }

  private interface PdxAssetFactory extends Serializable {
    PdxAsset getAsset(int i);
  }

  private static class PdxNotDeserializableAsset extends PdxAsset {

    public PdxNotDeserializableAsset() {
      throw new RuntimeException("Preventing Deserialization of Asset");
    }

    PdxNotDeserializableAsset(final int assetId, final String document) {
      super(assetId, document);
    }

    @Override
    public void fromData(final PdxReader reader) {
      throw new RuntimeException("Not allowing us to deserialize one of these");
    }
  }

  private static class PdxAsset implements PdxSerializable {

    private int assetId;
    private String document;
    private Collection<String> references = new ArrayList<>();

    public PdxAsset() {
      // nothing
    }

    PdxAsset(final int assetId, final String document) {
      this.assetId = assetId;
      this.document = document;
      references.add(document + "_1");
      references.add(document + "_2");
      references.add(document + "_3");
    }

    @Override
    public void toData(final PdxWriter writer) {
      writer.writeString("document", document);
      writer.writeInt("assetId", assetId);
      writer.writeObject("references", references);
    }

    @Override
    public void fromData(final PdxReader reader) {
      document = reader.readString("document");
      assetId = reader.readInt("assetId");
      references = (Collection<String>) reader.readObject("references");
    }
  }
}
