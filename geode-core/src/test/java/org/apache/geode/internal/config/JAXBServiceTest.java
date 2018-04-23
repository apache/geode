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

package org.apache.geode.internal.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class JAXBServiceTest {

  private String xml;
  private CacheConfig unmarshalled;
  private JAXBService service, service2;

  @Before
  public void setUp() throws Exception {
    service = spy(new JAXBService());
    service2 = spy(new JAXBService());
  }

  @Test
  public void testCacheMarshall() {
    CacheConfig cacheConfig = new CacheConfig();
    setBasicValues(cacheConfig);

    xml = service.marshall(cacheConfig);
    System.out.println(xml);
    // cache has the default namespace
    assertThat(xml).contains("xmlns=\"http://geode.apache.org/schema/cache\"");
    assertThat(xml).contains(
        "xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\"");
    assertThat(xml).contains("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
    assertThat(xml).contains("</cache>");

    unmarshalled = service.unMarshall(xml);
  }

  @Test
  public void invalidXmlShouldFail() throws Exception {
    CacheConfig cacheConfig = new CacheConfig();
    // missing version attribute
    assertThatThrownBy(() -> service.marshall(cacheConfig))
        .hasStackTraceContaining("Attribute 'version' must appear on element 'cache'");
  }

  @Test
  public void testCacheOneMarshall() throws Exception {
    CacheConfig cache = new CacheConfig();
    setBasicValues(cache);
    cache.getCustomCacheElements().add(new ElementOne("test"));

    service.registerBindClasses(ElementOne.class);
    xml = service.marshall(cache);
    System.out.println(xml);
    // cache has the default namespace
    assertThat(xml).contains("xmlns=\"http://geode.apache.org/schema/cache\"");
    assertThat(xml).contains("custom-one>");
  }

  @Test
  public void testMixMarshall() throws Exception {
    CacheConfig cache = new CacheConfig();
    setBasicValues(cache);
    cache.getCustomCacheElements().add(new ElementOne("testOne"));
    service.registerBindClasses(ElementOne.class);

    xml = service.marshall(cache);
    System.out.println(xml);
    assertThat(xml).contains("custom-one>");

    unmarshalled = service.unMarshall(xml);
    unmarshalled.getCustomCacheElements().add(new ElementTwo("testTwo"));
    service.registerBindClasses(ElementTwo.class);

    // xml generated wtih CacheConfigTwo has both elements in there.
    xml = service.marshall(unmarshalled);
    System.out.println(xml);
    assertThat(xml).contains("custom-one>");
    assertThat(xml).contains("custom-two>");
    assertThat(xml).containsPattern("xmlns=\"http://geode.apache.org/schema/cache\"");
    assertThat(xml).containsPattern("xmlns:ns\\d=\"http://geode.apache.org/schema/CustomOne\"");
    assertThat(xml).containsPattern("xmlns:ns\\d=\"http://geode.apache.org/schema/CustomTwo\"");
  }

  @Test
  public void xmlWithCustomElementsCanBeUnMarshalledByAnotherService() {
    CacheConfig cache = new CacheConfig();
    setBasicValues(cache);
    service.registerBindClasses(ElementOne.class);
    service.registerBindClasses(ElementTwo.class);
    cache.getCustomCacheElements().add(new ElementOne("test"));
    cache.getCustomCacheElements().add(new ElementTwo("test"));

    String prettyXml = service.marshall(cache);
    System.out.println(prettyXml);

    service2.registerBindClasses(ElementOne.class);
    CacheConfig cacheConfig = service2.unMarshall(prettyXml);
    List elements = cacheConfig.getCustomCacheElements();
    assertThat(elements.get(0)).isInstanceOf(ElementOne.class);
    assertThat(elements.get(1)).isNotInstanceOf(ElementTwo.class);

    String uglyXml = service2.marshall(cacheConfig);
    System.out.println(uglyXml);
    assertThat(uglyXml).isNotEqualTo(prettyXml);

    // the xml can be unmarshalled correctly by the first service
    String newXml = service.marshall(service.unMarshall(uglyXml));
    assertThat(newXml).isEqualTo(prettyXml);
  }

  public static void setBasicValues(CacheConfig cache) {
    cache.setCopyOnRead(true);
    CacheConfig.GatewayReceiver receiver = new CacheConfig.GatewayReceiver();
    receiver.setBindAddress("localhost");
    receiver.setEndPort(8080);
    receiver.setManualStart(false);
    receiver.setStartPort(6000);
    cache.setGatewayReceiver(receiver);

    RegionConfig region = new RegionConfig();
    region.setName("testRegion");
    region.setRefid("REPLICATE");
    cache.getRegions().add(region);
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"id", "value"})
  @XmlRootElement(name = "custom-one", namespace = "http://geode.apache.org/schema/CustomOne")
  public static class ElementOne implements CacheElement {
    private static final long serialVersionUID = 5385622917240503975L;
    @XmlElement(name = "id", namespace = "http://geode.apache.org/schema/CustomOne")
    private String id;
    @XmlElement(name = "value", namespace = "http://geode.apache.org/schema/CustomOne")
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
    private static final long serialVersionUID = 1623166601787822155L;
    @XmlElement(name = "id", namespace = "http://geode.apache.org/schema/CustomTwo")
    private String id;
    @XmlElement(name = "value", namespace = "http://geode.apache.org/schema/CustomTwo")
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
