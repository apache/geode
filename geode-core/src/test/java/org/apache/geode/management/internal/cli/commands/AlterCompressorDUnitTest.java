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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.CompressionTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({DistributedTest.class, CompressionTest.class})
public class AlterCompressorDUnitTest {
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setupCluster() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, "dataStore", locator.getPort());
    server2 = cluster.startServerVM(2, "dataStore", locator.getPort());
    server3 = cluster.startServerVM(3, "accessor", locator.getPort());

    gfsh.connect(locator);

    // create diskstores
    gfsh.executeAndAssertThat(
        "create disk-store --name=diskStore --groups=dataStore --dir=diskStore").statusIsSuccess();

    locator.waitTillDiskstoreIsReady("diskStore", 2);
    // create regions
    gfsh.executeAndAssertThat(
        "create region --name=testRegion --type=REPLICATE_PERSISTENT --group=dataStore --disk-store=diskStore")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=testRegion --type=REPLICATE_PROXY --group=accessor")
        .statusIsSuccess();

    // Load Data only from server3 since region is replicated
    server3.invoke(() -> {
      Region testRegion = ClusterStartupRule.getCache().getRegion("testRegion");
      IntStream.range(0, 100).forEach(i -> testRegion.put("key" + i, "value" + i));
    });
  }

  // This test is validating whether a persistent region can be altered to have a compressor.
  // 1) Start the cluster, create a region with out compressor and load some data.
  // 2) Being offline add a compressor by altering disk-store and cluster-config.
  // 3) Bounce servers and validate that the region is recovered and data is compressed.
  // 4) Again in offline remove the compressor and assert that region is recovered.
  @Test
  public void alterDiskCompressor() throws Exception {
    // verify no compressor to start with
    gfsh.executeAndAssertThat("describe region --name=testRegion").statusIsSuccess()
        .doesNotContainOutput("compressor");
    gfsh.executeAndAssertThat("describe disk-store --member=server-1 --name=diskStore")
        .statusIsSuccess().doesNotContainOutput("compressor");

    // verify that server's region has no compressor and the data is not compressed
    VMProvider.invokeInEveryMember(AlterCompressorDUnitTest::verifyRegionIsNotCompressed, server1,
        server2);

    // stop the VM
    server1.stopMember(false);
    server2.stopMember(false);
    server3.stopMember(false);

    // Alter disk-store & region to add compressor
    Arrays.asList(server1, server2).stream().forEach(server -> {
      String diskDir = server.getWorkingDir() + "/diskStore";
      // make sure offline diskstore has no compressor
      gfsh.executeAndAssertThat(
          "describe offline-disk-store --name=diskStore --disk-dirs=" + diskDir).statusIsSuccess()
          .containsOutput("-compressor=none");

      // alter diskstore to have compressor
      gfsh.executeAndAssertThat("alter disk-store --name=diskStore --region=testRegion --disk-dirs="
          + diskDir + " --compressor=" + TestCompressor1.class.getName()).statusIsSuccess();

      // describe again
      gfsh.executeAndAssertThat(
          "describe offline-disk-store --name=diskStore --disk-dirs=" + diskDir).statusIsSuccess()
          .containsOutput("-compressor=" + TestCompressor1.class.getName());
    });

    locator.invoke(() -> {
      // add the compressor to the region attributes and put it back in cluster config
      // this is just a hack to change the cache.xml so that when server restarts it restarts
      // with new region attributes
      InternalConfigurationPersistenceService ccService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration configuration = ccService.getConfiguration("dataStore");
      String modifiedXml =
          addCompressor(configuration.getCacheXmlContent(), TestCompressor1.class.getName());
      configuration.setCacheXmlContent(modifiedXml);
      ccService.getConfigurationRegion().put("dataStore", configuration);
    });

    // now start the VMs
    startServers();

    // verify that server's region has compressor added and the data is compressed
    VMProvider.invokeInEveryMember(AlterCompressorDUnitTest::verifyRegionIsCompressed, server1);

    // stop the VM
    server1.stopMember(false);
    server2.stopMember(false);
    server3.stopMember(false);

    Arrays.asList(server1, server2).stream().forEach(server -> {
      String diskDir = server.getWorkingDir() + "/diskStore";
      // make sure offline diskstore has no compressor
      gfsh.executeAndAssertThat(
          "describe offline-disk-store --name=diskStore --disk-dirs=" + diskDir).statusIsSuccess()
          .containsOutput("-compressor=" + TestCompressor1.class.getName());

      // alter diskstore to have compressor
      gfsh.executeAndAssertThat("alter disk-store --name=diskStore --region=testRegion --disk-dirs="
          + diskDir + " --compressor=none").statusIsSuccess();

      // describe again
      gfsh.executeAndAssertThat(
          "describe offline-disk-store --name=diskStore --disk-dirs=" + diskDir).statusIsSuccess()
          .containsOutput("-compressor=none");
    });

    locator.invoke(() -> {
      // remove the compressor to the region attributes and put it back in cluster config
      // this is just a hack to change the cache.xml so that when server restarts it restarts
      // with new region attributes
      InternalConfigurationPersistenceService ccService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration configuration = ccService.getConfiguration("dataStore");
      String modifiedXml = removeCompressor(configuration.getCacheXmlContent());
      configuration.setCacheXmlContent(modifiedXml);
      ccService.getConfigurationRegion().put("dataStore", configuration);
    });

    startServers();

    // verify that server's region has no compressor and the data is not compressed
    VMProvider.invokeInEveryMember(AlterCompressorDUnitTest::verifyRegionIsNotCompressed, server2);
  }

  private void startServers() throws Exception {
    // start server1 and server2 in parallel because they each has a replicate data store on disk
    Arrays.asList(1, 2).parallelStream().forEach(id -> {
      cluster.startServerVM(id, "dataStore", locator.getPort());
    });

    cluster.startServerVM(3, "accessor", locator.getPort());
  }

  private static String addCompressor(String cacheXmlContent, String name) throws Exception {
    Document document = XmlUtils.createDocumentFromXml(cacheXmlContent);
    NodeList nodeList = document.getElementsByTagName("region-attributes");
    Node compressor = document.createElement("compressor");
    Node classname = document.createElement("class-name");
    classname.setTextContent(name);
    compressor.appendChild(classname);
    nodeList.item(0).appendChild(compressor);
    return XmlUtils.prettyXml(document.getFirstChild());
  }

  private static String removeCompressor(String cacheXmlContent) {
    String beginTag = "<compressor>";
    String endTag = "</compressor>";
    int beginIndex = cacheXmlContent.indexOf(beginTag);
    int endIndex = cacheXmlContent.indexOf(endTag);
    return cacheXmlContent.substring(0, beginIndex)
        + cacheXmlContent.substring(endIndex + endTag.length());
  }

  private static void verifyRegionIsCompressed() {
    // assert the region attributes
    Region testRegion = ClusterStartupRule.getCache().getRegion("testRegion");
    assertThat(testRegion.getAttributes().getCompressor()).isNotNull();
    assertThat(testRegion.getAttributes().getCompressor()).isInstanceOf(TestCompressor1.class);

    // assert that entry value is actually compressed
    TestCompressor1 compressor = new TestCompressor1();
    LocalRegion localReg = ((LocalRegion) testRegion);
    RegionEntry entry = localReg.entries.getEntry("key19");
    Object value = entry.getValue();

    assertThat(value).isInstanceOf(byte[].class);
    byte[] actual = (byte[]) value;
    byte[] expected = EntryEventImpl
        .serialize(CachedDeserializableFactory.create(EntryEventImpl.serialize("value19"), null));
    assertThat(actual).isEqualTo(compressor.compress(expected));
  }

  private static void verifyRegionIsNotCompressed() {
    // assert the region attributes
    Region testRegion = ClusterStartupRule.getCache().getRegion("testRegion");
    assertThat(testRegion.getAttributes().getCompressor()).isNull();

    // assert that values are not compressed
    LocalRegion localReg = ((LocalRegion) testRegion);
    RegionEntry entry = localReg.entries.getEntry("key19");
    Object value = entry.getValue();
    assertThat(value).isInstanceOf(CachedDeserializable.class);
    CachedDeserializable expected =
        CachedDeserializableFactory.create(EntryEventImpl.serialize("value19"), null);
    assertThat(((CachedDeserializable) value).getSerializedValue())
        .isEqualTo(expected.getSerializedValue());
  }
}
