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
package org.apache.geode.cache.lucene.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.management.LuceneIndexMetrics;
import org.apache.geode.cache.lucene.management.LuceneServiceMXBean;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({LuceneTest.class})
public class LuceneVectorClusterConfigurationDUnitTest {

  private static final String INDEX_NAME = "vecIdx";
  private static final String REGION_NAME = "vecRegion";

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator;

  private static PdxInstance createVectorDoc(Cache cache, String category, float[] embedding) {
    return cache.createPdxInstanceFactory("VectorDoc")
        .writeString("category", category)
        .writeFloatArray("embedding", embedding)
        .create();
  }

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0);
  }

  @Test
  public void vectorIndexPersistsAcrossClusterConfigJoinAndSupportsGfshDescribeAndSearch()
      throws Exception {
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(
        "create lucene index --name=" + INDEX_NAME + " --region=" + REGION_NAME
            + " --field=category --vector-field=embedding --vector-dim=4 "
            + "--vector-similarity=COSINE --vector-sq8=true")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=" + REGION_NAME + " --type=PARTITION")
        .statusIsSuccess();

    // Verify Cluster Configuration XML persisted <lucene:vector-field .../>
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      InternalConfigurationPersistenceService cps =
          internalLocator.getConfigurationPersistenceService();
      Configuration cfg = cps.getConfiguration(ConfigurationPersistenceService.CLUSTER_CONFIG);
      String xml = cfg.getCacheXmlContent();
      assertThat(xml)
          .contains("vector-field")
          .contains("name=\"embedding\"")
          .contains("dimension=\"4\"")
          .contains("similarity=\"COSINE\"")
          .contains("quantize-sq8=\"true\"");
    });

    // Start a fresh server-2 joining from Cluster Configuration and verify vector metadata restored
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    server2.invoke(() -> {
      LuceneService service = LuceneServiceProvider.get(ClusterStartupRule.getCache());
      LuceneIndex idx = service.getIndex(INDEX_NAME, REGION_NAME);
      assertThat(idx).isNotNull();
      assertThat(idx.getFieldNames()).containsExactly("category");
      assertThat(idx.getVectorFields()).containsOnlyKeys("embedding");
      assertThat(idx.getVectorFields().get("embedding")).isEqualTo(
          new VectorIndexMetadata("embedding", 4, VectorSimilarityFunction.COSINE, true));
    });

    // Put PDX vector documents on server-1 (zero-copy PdxInstance path) and wait until flushed
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<String, PdxInstance> region = cache.getRegion(REGION_NAME);
      region.put("doc-keep-1",
          createVectorDoc(cache, "keep", new float[] {1.0f, 0.0f, 0.0f, 0.0f}));
      region.put("doc-skip-1",
          createVectorDoc(cache, "skip", new float[] {0.99f, 0.01f, 0.0f, 0.0f}));
      region.put("doc-keep-2",
          createVectorDoc(cache, "keep", new float[] {0.8f, 0.6f, 0.0f, 0.0f}));
      region.put("doc-keep-3",
          createVectorDoc(cache, "keep", new float[] {0.0f, 0.0f, 1.0f, 0.0f}));

      LuceneService service = LuceneServiceProvider.get(cache);
      assertThat(service.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60, TimeUnit.SECONDS)).isTrue();
    });

    // Verify gfsh describe lucene index displays Vector Fields and Segment Bytes
    gfsh.executeAndAssertThat(
        "describe lucene index --name=" + INDEX_NAME + " --region=/" + REGION_NAME)
        .statusIsSuccess()
        .containsOutput("embedding")
        .containsOutput("dim=4")
        .containsOutput("COSINE")
        .containsOutput("sq8=true")
        .containsOutput("Segment Bytes");

    // Verify gfsh search lucene executes pre-filtered HNSW k-NN query across the cluster
    gfsh.executeAndAssertThat(
        "search lucene --name=" + INDEX_NAME + " --region=/" + REGION_NAME
            + " --vector-field=embedding --query-vector=1.0,0.0,0.0,0.0 --k=2 "
            + "--filter-query=category:keep --keys-only")
        .statusIsSuccess()
        .containsOutput("doc-keep-1")
        .containsOutput("doc-keep-2")
        .doesNotContainOutput("doc-skip-1");

    // Restart server-1 and confirm cluster configuration restores vector index cleanly after
    // restart
    server1.stop();
    MemberVM restartedServer1 = cluster.startServerVM(1, locatorPort);
    restartedServer1.invoke(() -> {
      LuceneService service = LuceneServiceProvider.get(ClusterStartupRule.getCache());
      LuceneIndex idx = service.getIndex(INDEX_NAME, REGION_NAME);
      assertThat(idx).isNotNull();
      assertThat(idx.getVectorFields().get("embedding")).isEqualTo(
          new VectorIndexMetadata("embedding", 4, VectorSimilarityFunction.COSINE, true));
    });
  }

  @Test
  public void segmentMemoryBytesReportsLiveBytesEvenWhenStatisticSamplingDisabled()
      throws Exception {
    int locatorPort = locator.getPort();
    Properties serverProps = new Properties();
    serverProps.setProperty(STATISTIC_SAMPLING_ENABLED, "false");
    MemberVM server1 = cluster.startServerVM(1, serverProps, locatorPort);
    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(
        "create lucene index --name=liveBytesIdx --region=liveBytesRegion "
            + "--vector-field=embedding --vector-dim=8 --vector-similarity=COSINE --vector-sq8=true")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=liveBytesRegion --type=PARTITION")
        .statusIsSuccess();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<String, PdxInstance> region = cache.getRegion("liveBytesRegion");
      for (int i = 0; i < 20; i++) {
        region.put("k-" + i, createVectorDoc(cache, "c",
            new float[] {1.0f, i * 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f}));
      }
      LuceneService service = LuceneServiceProvider.get(cache);
      assertThat(service.waitUntilFlushed("liveBytesIdx", "liveBytesRegion", 60, TimeUnit.SECONDS))
          .isTrue();

      LuceneIndexImpl index =
          (LuceneIndexImpl) service.getIndex("liveBytesIdx", "liveBytesRegion");
      LuceneIndexDetails details = new LuceneIndexDetails(index, "server1");
      assertThat(details.getSegmentMemoryBytes()).isGreaterThan(0L);

      ManagementService mgmt = ManagementService.getManagementService(cache);
      ObjectName objectName = MBeanJMXAdapter.getCacheServiceMBeanName(
          cache.getDistributedSystem().getDistributedMember(), "LuceneService");
      LuceneServiceMXBean mxBean =
          mgmt.getMBeanInstance(objectName, LuceneServiceMXBean.class);
      assertThat(mxBean).isNotNull();
      LuceneIndexMetrics metrics = mxBean.listIndexMetrics("/liveBytesRegion", "liveBytesIdx");
      assertThat(metrics).isNotNull();
      assertThat(metrics.getSegmentMemoryBytes()).isGreaterThan(0L);
      assertThat(metrics.getVectorFields()).hasSize(1);
    });
  }
}
