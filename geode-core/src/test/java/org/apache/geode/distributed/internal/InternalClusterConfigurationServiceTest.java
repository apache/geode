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

import static org.apache.geode.internal.config.JAXBServiceTest.setBasicValues;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.config.JAXBServiceTest;
import org.apache.geode.internal.config.JAXBServiceTest.ElementOne;
import org.apache.geode.internal.config.JAXBServiceTest.ElementTwo;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class InternalClusterConfigurationServiceTest {
  private InternalClusterConfigurationService service, service2;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    service = spy(new InternalClusterConfigurationService(CacheConfig.class, ElementOne.class,
        ElementTwo.class));
    service2 = spy(new InternalClusterConfigurationService(CacheConfig.class));
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

    JAXBServiceTest.ElementTwo customTwo = new ElementTwo("testTwo");
    service.saveCustomCacheElement("cluster", customTwo);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");
    assertThat(configuration.getCacheXmlContent()).contains("custom-two>");
  }

  @Test
  // in case a locator in the cluster doesn't have the plugin installed
  public void xmlWithCustomElementsCanBeUnMarshalledByAnotherService() {
    service.saveCustomCacheElement("cluster", new ElementOne("one"));
    service.saveCustomCacheElement("cluster", new ElementTwo("two"));

    String prettyXml = configuration.getCacheXmlContent();
    System.out.println(prettyXml);

    // the xml is sent to another locator with no such plugin installed, it can be parsed
    // but the element couldn't be recognized by the locator without the plugin
    service2.updateCacheConfig("cluster", cc -> cc);
    ElementOne elementOne = service2.getCustomCacheElement("cluster", "one", ElementOne.class);
    assertThat(elementOne).isNull();

    String uglyXml = configuration.getCacheXmlContent();
    System.out.println(uglyXml);
    assertThat(uglyXml).isNotEqualTo(prettyXml);

    // the xml can be unmarshalled correctly by the first locator
    CacheConfig cacheConfig = service.getCacheConfig("cluster");
    service.updateCacheConfig("cluster", cc -> cc);
    assertThat(cacheConfig.getCustomCacheElements()).hasSize(2);
    assertThat(cacheConfig.getCustomCacheElements().get(0)).isInstanceOf(ElementOne.class);
    assertThat(cacheConfig.getCustomCacheElements().get(1)).isInstanceOf(ElementTwo.class);

    assertThat(configuration.getCacheXmlContent()).isEqualTo(prettyXml);
  }


  @Test
  public void updateCustomCacheElement() {
    ElementOne customOne = new ElementOne("testOne");
    service.saveCustomCacheElement("cluster", customOne);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");
    assertThat(configuration.getCacheXmlContent()).containsPattern("<ns\\d:id>testOne</ns\\d:id>");
    assertThat(configuration.getCacheXmlContent()).doesNotContain("<value>");

    customOne = service.getCustomCacheElement("cluster", "testOne", ElementOne.class);
    customOne.setValue("valueOne");
    service.saveCustomCacheElement("cluster", customOne);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).contains("custom-one>");
    assertThat(configuration.getCacheXmlContent()).containsPattern("<ns\\d:id>testOne</ns\\d:id>");
    assertThat(configuration.getCacheXmlContent())
        .containsPattern("<ns\\d:value>valueOne</ns\\d:value>");
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
        .containsPattern("</ns\\d:custom-one>\n" + "    </region>");

    ElementOne retrieved =
        service.getCustomRegionElement("cluster", "testRegion", "elementOne", ElementOne.class);
    assertThat(retrieved.getId()).isEqualTo("elementOne");
    assertThat(retrieved.getValue()).isEqualTo("valueOne");

    service.deleteCustomRegionElement("cluster", "testRegion", "elementOne", ElementOne.class);
    System.out.println(configuration.getCacheXmlContent());
    assertThat(configuration.getCacheXmlContent()).doesNotContain("custom-one>");
  }

  @Test
  public void getNonExistingGroupConfigShouldReturnNull() {
    assertThat(service.getCacheConfig("non-existing-group")).isNull();
  }

  @Test
  public void getExistingGroupConfigShouldReturnNullIfNoXml() {
    Configuration groupConfig = new Configuration("some-new-group");
    doReturn(groupConfig).when(service).getConfiguration("some-new-group");
    CacheConfig groupCacheConfig = service.getCacheConfig("some-new-group");
    assertThat(groupCacheConfig).isNull();
  }

  @Test
  public void updateShouldInsertIfNotExist() {
    doCallRealMethod().when(service).updateCacheConfig(any(), any());
    doCallRealMethod().when(service).getCacheConfig(any());
    Region region = mock(Region.class);
    doReturn(region).when(service).getConfigurationRegion();

    service.updateCacheConfig("non-existing-group", cc -> cc);

    verify(region).put(eq("non-existing-group"), any());
  }
}
