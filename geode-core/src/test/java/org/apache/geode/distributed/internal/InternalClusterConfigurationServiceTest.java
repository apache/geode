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

package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.configuration.CacheConfig;
import org.apache.geode.internal.cache.configuration.CacheElement;
import org.apache.geode.internal.cache.configuration.JndiBindingsType;
import org.apache.geode.internal.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class InternalClusterConfigurationServiceTest {
  private String xml;
  private CacheConfig unmarshalled;
  private ClusterConfigurationService service;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    service = spy(ClusterConfigurationService.class);
    configuration = new Configuration("cluster");
    doReturn(configuration).when(service).getConfiguration(any());
    doReturn(mock(Region.class)).when(service).getConfigurationRegion();
    doReturn(true).when(service).lockSharedConfiguration();
    doNothing().when(service).unlockSharedConfiguration();
  }

  @Test
  public void testCacheMarshall() {
    CacheConfig cacheConfig = new CacheConfig();
    setBasicValues(cacheConfig);

    xml = service.marshall(cacheConfig);
    System.out.println(xml);
    assertThat(xml).contains("</cache>");

    assertThat(xml).contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
    assertThat(xml).contains(
        "xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\"");

    unmarshalled = service.unMarshall(xml);
  }

  @Test
  public void testCacheOneMarshall() throws Exception {
    CacheConfig cache = new CacheConfig();
    setBasicValues(cache);
    cache.getCustomCacheElements().add(new ElementOne("test"));

    xml = service.marshall(cache, ElementOne.class);
    System.out.println(xml);
    assertThat(xml).contains("custom-one>");
  }

  @Test
  public void testMixMarshall() throws Exception {
    CacheConfig cache = new CacheConfig();
    setBasicValues(cache);
    cache.getCustomCacheElements().add(new ElementOne("testOne"));

    xml = service.marshall(cache, ElementOne.class);
    System.out.println(xml);
    assertThat(xml).contains("custom-one>");

    // xml generated with CacheConfigOne marshaller can be unmarshalled by CacheConfigTwo
    unmarshalled = service.unMarshall(xml);
    unmarshalled.getCustomCacheElements().add(new ElementTwo("testTwo"));

    // xml generated wtih CacheConfigTwo has both elements in there.
    xml = service.marshall(unmarshalled, ElementTwo.class);
    System.out.println(xml);
    assertThat(xml).contains("custom-one>");
    assertThat(xml).contains("custom-two>");
    assertThat(xml).containsPattern("xmlns:ns\\d=\"http://geode.apache.org/schema/cache\"");
    assertThat(xml).containsPattern("xmlns:ns\\d=\"http://geode.apache.org/schema/CustomOne\"");
    assertThat(xml).containsPattern("xmlns:ns\\d=\"http://geode.apache.org/schema/CustomTwo\"");
  }

  @Test
  public void updateRegionConfig() {
    service.updateCacheConfig("cluster", cacheConfig -> {
      RegionConfig regionConfig = new RegionConfig();
      regionConfig.setName("regionA");
      regionConfig.setRefid("REPLICATE");
      cacheConfig.getRegion().add(regionConfig);
      return cacheConfig;
    });

    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent())
        .contains("<region name=\"regionA\" refid=\"REPLICATE\"/>");
  }

  private void setBasicValues(CacheConfig cache) {
    cache.setCopyOnRead(true);
    CacheConfig.GatewayReceiver receiver = new CacheConfig.GatewayReceiver();
    receiver.setBindAddress("localhost");
    receiver.setEndPort("8080");
    receiver.setManualStart(false);
    receiver.setStartPort("6000");
    cache.setGatewayReceiver(receiver);
    cache.setVersion("1.0");

    RegionConfig region = new RegionConfig();
    region.setName("testRegion");
    region.setRefid("REPLICATE");
    cache.getRegion().add(region);
  }

  @Test
  public void jndiBindings() {
    service.updateCacheConfig("cluster", cacheConfig -> {
      JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
      jndiBinding.setJndiName("jndiOne");
      jndiBinding.setJdbcDriverClass("com.sun.ABC");
      jndiBinding.setType("SimpleDataSource");
      jndiBinding.getConfigProperty()
          .add(new JndiBindingsType.JndiBinding.ConfigProperty("test", "test", "test"));
      cacheConfig.getJndiBindings().add(jndiBinding);
      return cacheConfig;
    });

    assertThat(configuration.getCacheXmlContent()).containsOnlyOnce("</jndi-bindings>");
    assertThat(configuration.getCacheXmlContent()).contains(
        "<jndi-binding jdbc-driver-class=\"com.sun.ABC\" jndi-name=\"jndiOne\" type=\"SimpleDataSource\">");
    assertThat(configuration.getCacheXmlContent())
        .contains("config-property-name>test</config-property-name>");
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"id", "value"})
  @XmlRootElement(name = "custom-one", namespace = "http://geode.apache.org/schema/CustomOne")
  public static class ElementOne implements CacheElement {
    private String id;
    private String value;

    public ElementOne() {}

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public ElementOne(String value) {
      this.id = value;
    }

    public String getId() {
      return id;
    }

    public void setId(String value) {
      this.id = value;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"id", "value"})
  @XmlRootElement(name = "custom-two", namespace = "http://geode.apache.org/schema/CustomTwo")
  public static class ElementTwo implements CacheElement {
    private String id;
    private String value;

    public ElementTwo() {}

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public ElementTwo(String value) {
      this.id = value;
    }

    public String getId() {
      return id;
    }

    public void setId(String value) {
      this.id = value;
    }
  }
}
