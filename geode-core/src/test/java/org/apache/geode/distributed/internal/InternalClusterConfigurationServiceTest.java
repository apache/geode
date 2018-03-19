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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class InternalClusterConfigurationServiceTest {
  private String xml;
  private CacheConfig unmarshalled;
  private InternalClusterConfigurationService service, service2;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    service = spy(InternalClusterConfigurationService.class);
    service2 = spy(InternalClusterConfigurationService.class);
    configuration = new Configuration("cluster");
    doReturn(configuration).when(service).getConfiguration(any());
    doReturn(configuration).when(service2).getConfiguration(any());
    doReturn(mock(Region.class)).when(service).getConfigurationRegion();
    doReturn(mock(Region.class)).when(service2).getConfigurationRegion();
    doReturn(true).when(service).lockSharedConfiguration();
    doReturn(true).when(service2).lockSharedConfiguration();
    doNothing().when(service).unlockSharedConfiguration();
    doNothing().when(service2).unlockSharedConfiguration();
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
  public void xmlWithCustomElementsCanBeUnMarshalledByAnotherService() {
    service.saveCustomCacheElement("cluster", new ElementOne("one"));
    service.saveCustomCacheElement("cluster", new ElementTwo("two"));

    String prettyXml = configuration.getCacheXmlContent();
    System.out.println(prettyXml);

    // the xml is sent to another locator, and can interpreted ocrrectly
    service2.updateCacheConfig("cluster", cc -> {
      return cc;
    });

    ElementOne elementOne = service2.getCustomCacheElement("cluster", "one", ElementOne.class);
    assertThat(elementOne.getId()).isEqualTo("one");

    String uglyXml = configuration.getCacheXmlContent();
    System.out.println(uglyXml);
    assertThat(uglyXml).isNotEqualTo(prettyXml);

    // the xml can be unmarshalled correctly by the first locator
    CacheConfig cacheConfig = service.getCacheConfig("cluster");
    service.updateCacheConfig("cluster", cc -> {
      return cc;
    });
    assertThat(cacheConfig.getCustomCacheElements()).hasSize(2);
    assertThat(cacheConfig.getCustomCacheElements().get(0)).isInstanceOf(ElementOne.class);
    assertThat(cacheConfig.getCustomCacheElements().get(1)).isInstanceOf(ElementTwo.class);

    assertThat(configuration.getCacheXmlContent()).isEqualTo(prettyXml);
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

  @Test
  public void addCustomCacheElement() {
    ElementOne customOne = new ElementOne("testOne");
    service.saveCustomCacheElement("cluster", customOne);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");

    ElementTwo customTwo = new ElementTwo("testTwo");
    service.saveCustomCacheElement("cluster", customTwo);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");
    assertThat(configuration.getCacheXmlContent()).contains("custom-two>");
  }

  @Test
  public void updateCustomCacheElement() {
    ElementOne customOne = new ElementOne("testOne");
    service.saveCustomCacheElement("cluster", customOne);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");
    assertThat(configuration.getCacheXmlContent()).contains("<id>testOne</id>");
    assertThat(configuration.getCacheXmlContent()).doesNotContain("<value>");

    customOne = service.getCustomCacheElement("cluster", "testOne", ElementOne.class);
    customOne.setValue("valueOne");
    service.saveCustomCacheElement("cluster", customOne);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");
    assertThat(configuration.getCacheXmlContent()).contains("<id>testOne</id>");
    assertThat(configuration.getCacheXmlContent()).contains("<value>valueOne</value>");
  }

  @Test
  public void deleteCustomCacheElement() {
    ElementOne customOne = new ElementOne("testOne");
    service.saveCustomCacheElement("cluster", customOne);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");

    service.deleteCustomCacheElement("cluster", "testOne", ElementOne.class);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).doesNotContain("custom-one>");
  }

  @Test
  public void updateCustomRegionElement() {
    // start with a cache.xml that has region info
    service.updateCacheConfig("cluster", cacheConfig -> {
      setBasicValues(cacheConfig);
      return cacheConfig;
    });

    ElementOne one = new ElementOne("elementOne");
    one.setValue("valueOne");

    assertThatThrownBy(() -> service.saveCustomRegionElement("cluster", "noSuchRegion", one))
        .isInstanceOf(EntityNotFoundException.class)
        .hasMessageContaining("region noSuchRegion does not exist in group cluster");

    service.saveCustomRegionElement("cluster", "testRegion", one);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("region name=\"testRegion\"");
    assertThat(configuration.getCacheXmlContent())
        .containsPattern("\\w*:custom-one\\W*\\w*:region");

    ElementOne retrieved =
        service.getCustomRegionElement("cluster", "testRegion", "elementOne", ElementOne.class);
    assertThat(retrieved.getId()).isEqualTo("elementOne");
    assertThat(retrieved.getValue()).isEqualTo("valueOne");

    service.deleteCustomRegionElement("cluster", "testRegion", "elementOne", ElementOne.class);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).doesNotContain("custom-one>");
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

    public ElementOne(String id) {
      this.id = id;
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

    public ElementTwo(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void setId(String value) {
      this.id = value;
    }
  }
}
