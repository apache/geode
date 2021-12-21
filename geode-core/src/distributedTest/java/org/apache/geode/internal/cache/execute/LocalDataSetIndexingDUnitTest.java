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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.Region.SEPARATOR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
public class LocalDataSetIndexingDUnitTest extends JUnit4CacheTestCase {

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  public LocalDataSetIndexingDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.**;org.apache.geode.test.dunit.**");
    return result;
  }


  @Test
  public void testLocalDataSetIndexing() {
    final CacheSerializableRunnable createPRs = new CacheSerializableRunnable("create prs ") {
      @Override
      public void run2() {
        AttributesFactory<Integer, RegionValue> factory =
            new AttributesFactory<>();
        factory.setPartitionAttributes(new PartitionAttributesFactory<>()
            .setRedundantCopies(1).setTotalNumBuckets(8).create());
        final PartitionedRegion pr1 = (PartitionedRegion) createRootRegion("pr1", factory.create());
        factory = new AttributesFactory<>();
        factory.setPartitionAttributes(new PartitionAttributesFactory<>()
            .setRedundantCopies(1).setTotalNumBuckets(8).setColocatedWith(pr1.getName()).create());
        final PartitionedRegion pr2 = (PartitionedRegion) createRootRegion("pr2", factory.create());
      }
    };

    final CacheSerializableRunnable createIndexesOnPRs =
        new CacheSerializableRunnable("create prs ") {
          @Override
          public void run2() {
            try {
              QueryService qs = getCache().getQueryService();
              qs.createIndex("valueIndex1", IndexType.FUNCTIONAL, "e1.value", SEPARATOR + "pr1 e1");
              qs.createIndex("valueIndex2", IndexType.FUNCTIONAL, "e2.value", SEPARATOR + "pr2 e2");
            } catch (Exception e) {
              org.apache.geode.test.dunit.Assert
                  .fail("Test failed due to Exception in index creation ", e);
            }
          }
        };
    final CacheSerializableRunnable execute = new CacheSerializableRunnable("execute function") {
      @Override
      public void run2() {

        final PartitionedRegion pr1 = (PartitionedRegion) getRootRegion("pr1");
        final PartitionedRegion pr2 = (PartitionedRegion) getRootRegion("pr2");

        final Set<Integer> filter = new HashSet<>();
        for (int i = 1; i <= 80; i++) {
          pr1.put(i, new RegionValue(i));
          if (i <= 20) {
            pr2.put(i, new RegionValue(i));
            if ((i % 5) == 0) {
              filter.add(i);
            }
          }
        }

        ArrayList<List> result = (ArrayList<List>) FunctionService.onRegion(pr1).withFilter(filter)
            .execute(new FunctionAdapter() {
              @Override
              public void execute(FunctionContext context) {
                try {
                  RegionFunctionContext rContext = (RegionFunctionContext) context;
                  Region pr1 = rContext.getDataSet();
                  LocalDataSet localCust =
                      (LocalDataSet) PartitionRegionHelper.getLocalDataForContext(rContext);
                  Map<String, Region<?, ?>> colocatedRegions =
                      PartitionRegionHelper.getColocatedRegions(pr1);
                  Map<String, Region<?, ?>> localColocatedRegions =
                      PartitionRegionHelper.getLocalColocatedRegions(rContext);
                  Region pr2 = colocatedRegions.get(SEPARATOR + "pr2");
                  LocalDataSet localOrd =
                      (LocalDataSet) localColocatedRegions.get(SEPARATOR + "pr2");
                  QueryObserverImpl observer = new QueryObserverImpl();
                  QueryObserverHolder.setInstance(observer);
                  QueryService qs = pr1.getCache().getQueryService();
                  DefaultQuery query = (DefaultQuery) qs.newQuery(
                      "select distinct e1.value from " + SEPARATOR + "pr1 e1, " + SEPARATOR
                          + "pr2  e2 where e1.value=e2.value");
                  final ExecutionContext executionContext =
                      new QueryExecutionContext(null, cache, query);

                  GemFireCacheImpl.getInstance().getLogger()
                      .fine(" Num BUCKET SET: " + localCust.getBucketSet());
                  GemFireCacheImpl.getInstance().getLogger().fine("VALUES FROM PR1 bucket:");
                  for (Integer bId : localCust.getBucketSet()) {
                    BucketRegion br =
                        ((PartitionedRegion) pr1).getDataStore().getLocalBucketById(bId);
                    String val = "";
                    for (Object e : br.values()) {
                      val += (e + ",");
                    }
                    GemFireCacheImpl.getInstance().getLogger().fine(": " + val);
                  }

                  GemFireCacheImpl.getInstance().getLogger().fine("VALUES FROM PR2 bucket:");
                  for (Integer bId : localCust.getBucketSet()) {
                    BucketRegion br =
                        ((PartitionedRegion) pr2).getDataStore().getLocalBucketById(bId);
                    String val = "";
                    for (Object e : br.values()) {
                      val += (e + ",");
                    }
                    GemFireCacheImpl.getInstance().getLogger().fine(": " + val);
                  }

                  SelectResults r =
                      (SelectResults) localCust.executeQuery(query, executionContext, null,
                          localCust.getBucketSet());

                  GemFireCacheImpl.getInstance().getLogger().fine("Result :" + r.asList());

                  Assert.assertTrue(observer.isIndexesUsed);
                  pr1.getCache().getLogger().fine("Index Used: " + observer.numIndexesUsed());
                  Assert.assertTrue(2 == observer.numIndexesUsed());
                  context.getResultSender().lastResult(r.asList());
                } catch (Exception e) {
                  context.getResultSender().lastResult(Boolean.TRUE);
                }
              }

              @Override
              public String getId() {
                return "ok";
              }

              @Override
              public boolean optimizeForWrite() {
                return false;
              }

            }).getResult();
        int numResults = 0;
        for (List oneNodeResult : result) {
          GemFireCacheImpl.getInstance().getLogger()
              .fine("Result :" + numResults + " oneNodeResult.size(): " + oneNodeResult.size()
                  + " oneNodeResult :" + oneNodeResult);
          numResults = +oneNodeResult.size();
        }
        Assert.assertTrue(10 == numResults);
      }
    };
    dataStore1.invoke(createPRs);
    dataStore2.invoke(createPRs);
    dataStore1.invoke(createIndexesOnPRs);
    dataStore1.invoke(execute);
  }

}


class QueryObserverImpl extends QueryObserverAdapter {

  boolean isIndexesUsed = false;

  ArrayList<String> indexesUsed = new ArrayList<>();

  String indexName;

  @Override
  public void beforeIndexLookup(Index index, int oper, Object key) {
    indexName = index.getName();
    indexesUsed.add(index.getName());

  }

  @Override
  public void afterIndexLookup(Collection results) {
    if (results != null) {
      isIndexesUsed = true;
    }
  }

  public int numIndexesUsed() {
    return indexesUsed.size();
  }
}


class RegionValue implements Serializable, Comparable<RegionValue> {

  private static final long serialVersionUID = 1L;

  public int value = 0;

  public int value2 = 0;

  public RegionValue(int value) {
    this.value = value;
    value2 = value;
  }

  @Override
  public int compareTo(RegionValue o) {
    if (value > o.value) {
      return 1;
    } else if (value < o.value) {
      return -1;
    } else {
      return 0;
    }
  }

  public String toString() {
    return "" + value;
  }
}
/*
 * 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35
 * 36 37 38 39
 *
 * 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
 *
 * 5 10 15 20 - 4 buckets
 *
 * 5, 2, 7, 4
 *
 * Result : 5, 13, 2, 10 , 18 , 7, 15, 4, 12, 20
 */
