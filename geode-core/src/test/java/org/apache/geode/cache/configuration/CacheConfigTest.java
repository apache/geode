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

import org.apache.geode.internal.config.JAXBService;


public class CacheConfigTest {

  private CacheConfig cacheConfig;
  private JAXBService service;
  private RegionConfig regionConfig;
  private String regionXml;
  private DeclarableType declarableWithString;
  private String declarableWithStringXml;
  private String classNameTypeXml;
  private DeclarableType declarableWithParam;
  private String declarableWithParamXml;
  private String cacheXml;

  @Before
  public void setUp() throws Exception {
    cacheConfig = new CacheConfig("1.0");
    cacheXml =
        "<cache version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\" xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">";
    service = new JAXBService(CacheConfig.class);
    service.validateWithLocalCacheXSD();
    regionConfig = new RegionConfig();
    regionConfig.setName("regionA");
    regionConfig.setType("REPLICATE");
    regionXml = "<region name=\"regionA\" refid=\"REPLICATE\">";

    classNameTypeXml = "<class-name>my.className</class-name>";
    declarableWithString = new DeclarableType("my.className", "{'key':'value'}");
    declarableWithStringXml =
        classNameTypeXml + "<parameter name=\"key\"><string>value</string></parameter>";

    declarableWithParam = new DeclarableType("my.className");
    ParameterType param = new ParameterType("key");
    param.setDeclarable(declarableWithString);
    declarableWithParam.getParameters().add(param);
    declarableWithParamXml = classNameTypeXml + "<parameter name=\"key\"><declarable>"
        + declarableWithStringXml + "</declarable></parameter>";
  }

  @After
  public void tearDown() throws Exception {
    // make sure the marshalled xml passed validation
    System.out.println(service.marshall(cacheConfig));
  }

  @Test
  public void indexType() {
    String xml = cacheXml + regionXml
        + "<index name=\"indexName\" expression=\"expression\" key-index=\"true\"/>"
        + "</region></cache>";

    cacheConfig = service.unMarshall(xml);
    RegionConfig.Index index = cacheConfig.getRegions().get(0).getIndexes().get(0);
    assertThat(index.isKeyIndex()).isTrue();
    assertThat(index.getName()).isEqualTo("indexName");
    assertThat(index.getExpression()).isEqualTo("expression");
    assertThat(index.getType()).isEqualTo("range");
  }


  @Test
  public void regionEntry() {
    String xml = cacheXml + regionXml + "<entry>" + "<key><string>key1</string></key>"
        + "<value><declarable>" + declarableWithStringXml + "</declarable></value>" + "</entry>"
        + "<entry>" + "<key><string>key2</string></key>" + "<value><declarable>"
        + declarableWithParamXml + "</declarable></value>" + "</entry>" + "</region></cache>";

    cacheConfig = service.unMarshall(xml);
    RegionConfig.Entry entry = cacheConfig.getRegions().get(0).getEntries().get(0);
    assertThat(entry.getKey().toString()).isEqualTo("key1");
    assertThat(entry.getValue().getDeclarable()).isEqualTo(declarableWithString);

    entry = cacheConfig.getRegions().get(0).getEntries().get(1);
    assertThat(entry.getKey().toString()).isEqualTo("key2");
    assertThat(entry.getValue().getDeclarable()).isEqualTo(declarableWithParam);
  }

  @Test
  public void cacheTransactionManager() {
    String xml = cacheXml + "<cache-transaction-manager>" + "<transaction-listener>"
        + declarableWithStringXml + "</transaction-listener>" + "<transaction-writer>"
        + declarableWithStringXml + "</transaction-writer>"
        + "</cache-transaction-manager></cache>";

    cacheConfig = service.unMarshall(xml);
    assertThat(cacheConfig.getCacheTransactionManager().getTransactionWriter())
        .isEqualTo(declarableWithString);
    assertThat(cacheConfig.getCacheTransactionManager().getTransactionListeners().get(0))
        .isEqualTo(declarableWithString);
  }

  @Test
  public void declarables() {
    String xml = cacheXml + "<region-attributes>" + "<cache-loader>" + declarableWithStringXml
        + "</cache-loader>" + "<cache-listener>" + declarableWithStringXml + "</cache-listener>"
        + "<cache-writer>" + declarableWithStringXml + "</cache-writer>" + "<compressor>"
        + classNameTypeXml + "</compressor>"
        + "<region-time-to-live><expiration-attributes timeout=\"0\"><custom-expiry>"
        + declarableWithStringXml + "</custom-expiry></expiration-attributes></region-time-to-live>"
        + "</region-attributes>" + "<function-service><function>" + declarableWithStringXml
        + "</function></function-service>" + "<initializer>" + declarableWithStringXml
        + "</initializer>" + "<pdx><pdx-serializer>" + declarableWithStringXml
        + "</pdx-serializer></pdx>" + "<cache-server><custom-load-probe>" + declarableWithStringXml
        + "</custom-load-probe></cache-server>" + "<gateway-conflict-resolver>"
        + declarableWithStringXml + "</gateway-conflict-resolver>"
        + "<gateway-receiver><gateway-transport-filter>" + declarableWithStringXml
        + "</gateway-transport-filter></gateway-receiver>" + "<async-event-queue id=\"queue\">"
        + "<gateway-event-substitution-filter>" + declarableWithStringXml
        + "</gateway-event-substitution-filter>" + "<gateway-event-filter>"
        + declarableWithStringXml + "</gateway-event-filter>" + "<async-event-listener>"
        + declarableWithStringXml + "</async-event-listener>" + "</async-event-queue>"
        + "<gateway-hub id=\"hub\"><gateway id=\"gateway\"><gateway-listener>"
        + declarableWithStringXml + "</gateway-listener></gateway></gateway-hub>" + "</cache>";

    cacheConfig = service.unMarshall(xml);

    assertThat(cacheConfig.getInitializer()).isEqualTo(declarableWithString);
    assertThat(cacheConfig.getFunctionService().getFunctions().get(0))
        .isEqualTo(declarableWithString);
    assertThat(cacheConfig.getPdx().getPdxSerializer()).isEqualTo(declarableWithString);
    assertThat(cacheConfig.getCacheServers().get(0).getCustomLoadProbe())
        .isEqualTo(declarableWithString);
    assertThat(cacheConfig.getGatewayConflictResolver()).isEqualTo(declarableWithString);
    assertThat(cacheConfig.getGatewayReceiver().getGatewayTransportFilters().get(0))
        .isEqualTo(declarableWithString);
    assertThat(cacheConfig.getGatewayHubs().get(0).getGateway().get(0).getGatewayListeners().get(0))
        .isEqualTo(declarableWithString);

    CacheConfig.AsyncEventQueue asyncEventQueue = cacheConfig.getAsyncEventQueues().get(0);
    assertThat(asyncEventQueue.getAsyncEventListener()).isEqualTo(declarableWithString);
    assertThat(asyncEventQueue.getGatewayEventFilters().get(0)).isEqualTo(declarableWithString);
    assertThat(asyncEventQueue.getGatewayEventSubstitutionFilter()).isEqualTo(declarableWithString);

    RegionAttributesType regionAttributes = cacheConfig.getRegionAttributes().get(0);
    assertThat(regionAttributes.getCacheListeners().get(0)).isEqualTo(declarableWithString);
    assertThat(regionAttributes.getCompressor().toString()).isEqualTo("my.className");
    assertThat(regionAttributes.getCacheLoader()).isEqualTo(declarableWithString);
    assertThat(regionAttributes.getCacheWriter()).isEqualTo(declarableWithString);
    assertThat(regionAttributes.getRegionTimeToLive().getCustomExpiry())
        .isEqualTo(declarableWithString);
  }

  @Test
  public void regionConfig() {
    cacheConfig = new CacheConfig("1.0");
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("test");
    regionConfig.setType("REPLICATE");
    RegionAttributesType attributes = new RegionAttributesType();
    attributes.setCacheLoader(new DeclarableType("abc.Foo"));
    regionConfig.setRegionAttributes(attributes);
    cacheConfig.getRegions().add(regionConfig);

    // make sure the xml marshed by this config can be validated with xsd
    String xml = service.marshall(cacheConfig);

    CacheConfig newCache = service.unMarshall(xml);
    assertThat(cacheConfig).isEqualToComparingFieldByFieldRecursively(newCache);
  }

  @Test
  public void regionAttributeType() throws Exception {
    String xml = "<region name=\"test\">\n"
        + "        <region-attributes>\n"
        + "            <region-time-to-live>\n"
        + "                <expiration-attributes action=\"invalidate\" timeout=\"20\">\n"
        + "                    <custom-expiry>\n"
        + "                        <class-name>bar</class-name>\n"
        + "                    </custom-expiry>\n"
        + "                </expiration-attributes>\n"
        + "            </region-time-to-live>\n"
        + "            <entry-time-to-live>\n"
        + "                <expiration-attributes action=\"destroy\" timeout=\"10\">\n"
        + "                    <custom-expiry>\n"
        + "                        <class-name>foo</class-name>\n"
        + "                    </custom-expiry>\n"
        + "                </expiration-attributes>\n"
        + "            </entry-time-to-live>\n"
        + "        </region-attributes>\n"
        + "    </region>";

    RegionConfig regionConfig = service.unMarshall(xml, RegionConfig.class);
    RegionAttributesType.ExpirationAttributesType entryTimeToLive =
        regionConfig.getRegionAttributes().getEntryTimeToLive();
    assertThat(entryTimeToLive.getTimeout()).isEqualTo("10");
    assertThat(entryTimeToLive.getAction()).isEqualTo("destroy");
    assertThat(entryTimeToLive.getCustomExpiry().getClassName()).isEqualTo("foo");
    RegionAttributesType.ExpirationAttributesType regionTimeToLive =
        regionConfig.getRegionAttributes().getRegionTimeToLive();
    assertThat(regionTimeToLive.getTimeout()).isEqualTo("20");
    assertThat(regionTimeToLive.getAction()).isEqualTo("invalidate");
    assertThat(regionTimeToLive.getCustomExpiry().getClassName()).isEqualTo("bar");

    cacheConfig.getRegions().add(regionConfig);
  }
}
