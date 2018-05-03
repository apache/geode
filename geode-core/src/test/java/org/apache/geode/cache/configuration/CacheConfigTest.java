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

package org.apache.geode.cache.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class CacheConfigTest {

  private CacheConfig cacheConfig;
  private JAXBService service;
  private RegionConfig regionConfig;

  @Before
  public void setUp() throws Exception {
    cacheConfig = new CacheConfig("1.0");
    service = new JAXBService(CacheConfig.class);
    service.validateWithLocalCacheXSD();
    regionConfig = new RegionConfig();
    regionConfig.setName("regionA");
    regionConfig.setRefid("REPLICATE");
  }

  @After
  public void tearDown() throws Exception {
    System.out.println(service.marshall(cacheConfig));
  }

  @Test
  public void validateIndexType() {
    RegionConfig.Index index = new RegionConfig.Index();
    cacheConfig.getRegion().add(regionConfig);
    index.setName("indexName");
    index.setKeyIndex(true);
    index.setExpression("expression");
    regionConfig.getIndex().add(index);
  }


  @Test
  public void stringType() {
    String xml =
        "<cache version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\" xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <region name=\"regionA\" refid=\"REPLICATE\">\n" + "        <entry>\n"
            + "            <key>\n" + "                <string>myKey</string>\n"
            + "            </key>\n" + "            <value>\n" + "                <declarable>\n"
            + "                    <class-name>org.apache.geode.management.internal.cli.domain.MyCacheListener</class-name>\n"
            + "                    <parameter name=\"name\">\n"
            + "                        <string>value</string>\n"
            + "                    </parameter>\n" + "                </declarable>\n"
            + "            </value>\n" + "        </entry>\n" + "    </region>\n" + "</cache>";
    cacheConfig.getRegion().add(regionConfig);
    RegionConfig.Entry entry = new RegionConfig.Entry();
    RegionConfig.Entry.Type key = new RegionConfig.Entry.Type("myKey");
    RegionConfig.Entry.Type value = new RegionConfig.Entry.Type(new DeclarableType(
        "org.apache.geode.management.internal.cli.domain.MyCacheListener", "{'name':'value'}"));

    entry.setKey(key);
    entry.setValue(value);
    regionConfig.getEntry().add(entry);

    // make sure the POJO can marshall to the expected xml
    assertThat(service.marshall(cacheConfig)).contains(xml);
    // make sure the xml can be unmarshalled.
    service.unMarshall(xml);
  }

  @Test
  public void entry() {
    String xml =
        "<cache version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\" xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <region name=\"regionA\" refid=\"REPLICATE\">\n" + "        <entry>\n"
            + "            <key>\n" + "                <string>key</string>\n"
            + "            </key>\n" + "            <value>\n"
            + "                <string>value</string>\n" + "            </value>\n"
            + "        </entry>\n" + "    </region>\n" + "</cache>";
    cacheConfig.getRegion().add(regionConfig);
    regionConfig.getEntry().add(new RegionConfig.Entry("key", "value"));
    assertThat(service.marshall(cacheConfig)).contains(xml);
    service.unMarshall(xml);
  }
}
