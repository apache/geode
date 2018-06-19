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

package org.apache.geode.cache.lucene;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Properties;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class LuceneIntegerRangeDUnitTest {

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfshCommandRule = new GfshCommandRule();

  private static MemberVM locator;
  private static MemberVM server1;
  private static ClientVM client1;


  @BeforeClass
  public static void beforeAllTests() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    final int locatorPort = locator.getPort();
    server1 = clusterStartupRule
        .startServerVM(1,
            r -> r.withPDXPersistent()
                .withPDXReadSerialized()
                .withConnectionToLocator(locatorPort)
                .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.lucene.**"));

    gfshCommandRule.connect(locator);

    client1 =
        clusterStartupRule.startClientVM(2, new Properties(),
            (Consumer<ClientCacheFactory> & Serializable) (x -> x
                .set(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.lucene.**")
                .addPoolLocator("localhost", locatorPort)));
  }


  @Test
  public void testByIntegerRange() {
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      LuceneService luceneService = LuceneServiceProvider.get(cache);
      luceneService.createIndexFactory().setFields("name", "height", "dob").create("idx1",
          "/sampleregion");

      Region<String, Person> region =
          cache.<String, Person>createRegionFactory(RegionShortcut.PARTITION)
              .create("sampleregion");
    });
    
    client1.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Person person1 =
          new Person("person1", 110,
              Date.from(LocalDateTime.parse("2001-01-01T00:00:00").toInstant(ZoneOffset.UTC)));

      Person person2 =
          new Person("person2", 120,
              Date.from(LocalDateTime.parse("2002-01-01T00:00:00").toInstant(ZoneOffset.UTC)));
      Person person3 =
          new Person("person3", 130,
              Date.from(LocalDateTime.parse("2003-01-01T00:00:00").toInstant(ZoneOffset.UTC)));

      Region<String, Person> region = clientCache.<String, Person>createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create("sampleregion");

      region.put(person1.name, person1);
      region.put(person2.name, person2);
      region.put(person3.name, person3);

      QueryService queryService = clientCache.getQueryService();

      Query query = queryService.newQuery("select * from /sampleregion");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results).hasSize(3);      

      LuceneService luceneService = LuceneServiceProvider.get(clientCache);

      LuceneQuery luceneQuery1 = luceneService.createLuceneQueryFactory()
          .create("idx1", "/sampleregion", "+name=person* +height:[120 TO 130]", "name");

      assertThat(luceneQuery1.findKeys())
          .containsExactlyInAnyOrder("person2", "person3");
    });



  }

  public static class Person implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private int height;
    private Date dob;

    public Person() {
      // for marshalling.
    }

    public Person(String name, int height, Date dob) {
      this.name = name;
      this.height = height;
      this.dob = dob;
    }

    public String getName() {
      return name;
    }

    public int getHeight() {
      return height;
    }

    public Date getDob() {
      return dob;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setHeight(int height) {
      this.height = height;
    }

    public void setDob(Date dob) {
      this.dob = dob;
    }

    @Override
    public String toString() {
      final StringBuffer sb = new StringBuffer("Person{");
      sb.append("name='").append(name).append('\'');
      sb.append(", height=").append(height);
      sb.append(", dob=").append(dob);
      sb.append('}');
      return sb.toString();
    }
  }

}
