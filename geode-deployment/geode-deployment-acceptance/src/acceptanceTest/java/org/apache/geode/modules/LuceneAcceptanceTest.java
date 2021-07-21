/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.modules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collection;

import example.test.pojo.model.InnerPojo;
import example.test.pojo.model.TestPojo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class LuceneAcceptanceTest extends AbstractDockerizedAcceptanceTest {
  private static ClientCache clientCache;

  public LuceneAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Before
  public void setup() {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "create lucene index --name=namesIndex --region=/TestRegion --field=firstName,lastName",
        "create region --name=TestRegion --type=PARTITION")
        .execute(gfshRule);

    clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .setPdxSerializer(
                new ReflectionBasedAutoSerializer("example.test.pojo.model.*"))
            .create();

    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("TestRegion");
    partitionedRegion.put(1L, new TestPojo("John", "Smith", new InnerPojo()));
    partitionedRegion.put(2L, new TestPojo("Robert", "Smith", new InnerPojo()));
    partitionedRegion.put(3L, new TestPojo("Jack", "Black", new InnerPojo()));
  }

  @After
  public void shutdown() {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "destroy lucene index --region=TestRegion",
        "destroy region --name=TestRegion")
        .execute(gfshRule);
    clientCache.close();
  }

  @Test
  public void testLuceneIndexWasCreated() {
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list lucene indexes")
        .execute(gfshRule).getOutputText()).contains("namesIndex")
            .doesNotContain("No lucene indexes found");
  }

  @Test
  public void testLuceneSearchUsingJavaApi() throws LuceneQueryException {
    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .create();

    LuceneService luceneService = LuceneServiceProvider.get(clientCache);
    LuceneQuery<Long, TestPojo> luceneQuery = luceneService.createLuceneQueryFactory()
        .create("namesIndex", "/TestRegion", "Smith", "lastName");

    Collection<TestPojo> results = luceneQuery.findValues();

    assertThat(results.size()).isEqualTo(2);
    assertThat(results).contains(new TestPojo("John", "Smith", new InnerPojo()),
        new TestPojo("Robert", "Smith", new InnerPojo()));

    clientCache.close();
  }

  @Test
  public void testLuceneSearchUsingGfsh() {
    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(),
            "search lucene --name=namesIndex --region=/TestRegion --queryString=Smith --defaultField=lastName")
        .execute(gfshRule).getOutputText()).doesNotContain("Jack");
    assertThat(GfshScript
        .of(getLocatorGFSHConnectionString(),
            "search lucene --name=namesIndex --region=/TestRegion --queryString=J* --defaultField=firstName")
        .execute(gfshRule).getOutputText()).contains("Jack", "John").doesNotContain("Robert");
  }
}
