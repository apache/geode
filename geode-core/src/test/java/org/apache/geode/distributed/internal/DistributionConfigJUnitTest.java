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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.UnmodifiableException;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.security.templates.SamplePostProcessor;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class DistributionConfigJUnitTest {

  private Map<Class<?>, Class<?>> classMap;

  private Map<String, ConfigAttribute> attributes;
  private Map<String, Method> setters;
  private Map<String, Method> getters;
  private Map<String, Method> checkers;
  private String[] attNames;

  private DistributionConfigImpl config;

  @Before
  public void before() {
    classMap = new HashMap<>();
    classMap.put(boolean.class, Boolean.class);
    classMap.put(byte.class, Byte.class);
    classMap.put(short.class, Short.class);
    classMap.put(char.class, Character.class);
    classMap.put(int.class, Integer.class);
    classMap.put(long.class, Long.class);
    classMap.put(float.class, Float.class);
    classMap.put(double.class, Double.class);

    attributes = DistributionConfig.attributes;
    setters = DistributionConfig.setters;
    getters = DistributionConfig.getters;
    attNames = DistributionConfig.dcValidAttributeNames;
    checkers = AbstractDistributionConfig.checkers;

    config = new DistributionConfigImpl(new Properties());
  }

  @Test
  public void testGetAttributeNames() {
    String[] attNames = AbstractDistributionConfig._getAttNames();
    assertEquals(attNames.length, 156);

    List boolList = new ArrayList();
    List intList = new ArrayList();
    List fileList = new ArrayList();
    List stringList = new ArrayList();
    List otherList = new ArrayList();
    for (String attName : attNames) {
      Class clazz = AbstractDistributionConfig._getAttributeType(attName);
      if (clazz.equals(Boolean.class)) {
        boolList.add(attName);
      } else if (clazz.equals(Integer.class)) {
        intList.add(attName);
      } else if (clazz.equals(String.class)) {
        stringList.add(attName);
      } else if (clazz.equals(File.class)) {
        fileList.add(attName);
      } else {
        otherList.add(attName);
      }
    }

    System.out.println("boolList: " + boolList);
    System.out.println();
    System.out.println("intList: " + intList);
    System.out.println();
    System.out.println("stringlList: " + stringList);
    System.out.println();
    System.out.println("filelList: " + fileList);
    System.out.println();
    System.out.println("otherList: " + otherList);

    // TODO - This makes no sense. One has no idea what the correct expected number of attributes
    // are.
    assertEquals(29, boolList.size());
    assertEquals(33, intList.size());
    assertEquals(85, stringList.size());
    assertEquals(5, fileList.size());
    assertEquals(4, otherList.size());
  }

  @Test
  public void testAttributeDesc() {
    String[] attNames = AbstractDistributionConfig._getAttNames();
    for (String attName : attNames) {
      assertTrue("Does not contain description for attribute " + attName,
          AbstractDistributionConfig.dcAttDescriptions.containsKey(attName));
    }
    List<String> attList = Arrays.asList(attNames);
    for (Object attName : AbstractDistributionConfig.dcAttDescriptions.keySet()) {
      if (!attList.contains(attName)) {
        System.out.println("Has unused description for " + attName.toString());
      }
    }
  }

  @Test
  public void sameCount() {
    assertEquals(attributes.size(), setters.size());
    assertEquals(setters.size(), getters.size());
  }

  @Test
  public void everyAttrHasValidSetter() {
    for (String attr : attributes.keySet()) {
      Method setter = setters.get(attr);
      assertNotNull(attr + " should have a setter", setter);
      assertTrue(setter.getName().startsWith("set"));
      assertEquals(setter.getParameterCount(), 1);

      if (!(attr.equalsIgnoreCase(LOG_LEVEL) || attr.equalsIgnoreCase(SECURITY_LOG_LEVEL))) {
        Class clazz = attributes.get(attr).type();
        try {
          setter.invoke(mock(DistributionConfig.class), any(clazz));
        } catch (Exception e) {
          throw new RuntimeException("Error calling setter " + setter.getName(), e);
        }
      }

    }
  }

  @Test
  public void everyAttrHasValidGetter() {
    for (String attr : attributes.keySet()) {
      Method getter = getters.get(attr);
      assertNotNull(attr + " should have a getter", getter);
      assertTrue(getter.getName().startsWith("get"));
      assertEquals(getter.getParameterCount(), 0);

      if (!(attr.equalsIgnoreCase(LOG_LEVEL) || attr.equalsIgnoreCase(SECURITY_LOG_LEVEL))) {
        Class clazz = attributes.get(attr).type();
        Class returnClass = getter.getReturnType();
        if (returnClass.isPrimitive()) {
          returnClass = classMap.get(returnClass);
        }
        assertEquals(returnClass, clazz);
      }
    }
  }

  @Test
  public void everyGetterSetterSameNameSameType() {
    for (String attr : getters.keySet()) {
      Method getter = getters.get(attr);
      Method setter = setters.get(attr);
      assertNotNull("every getter should have a corresponding setter " + attr, setter);
      String setterName = setter.getName();
      String getterName = getter.getName();
      assertEquals(setterName.substring(setterName.indexOf("set") + 3),
          getterName.substring(getterName.indexOf("get") + 3));
      assertEquals(setter.getParameterTypes()[0], getter.getReturnType());
    }

    for (String attr : setters.keySet()) {
      Method getter = getters.get(attr);
      assertNotNull("every setter should have a corresponding getter: " + attr, getter);
    }
  }

  @Test
  public void everySetterHasAttributeDefined() {
    for (String attr : setters.keySet()) {
      ConfigAttribute configAttribute = attributes.get(attr);
      assertNotNull(attr + " should be defined a ConfigAttribute", configAttribute);
    }
  }

  @Test
  public void everyGetterHasAttributeDefined() {
    for (String attr : getters.keySet()) {
      ConfigAttribute configAttribute = attributes.get(attr);
      assertNotNull(attr + " should be defined a ConfigAttribute", configAttribute);
    }
  }

  @Test
  public void testGetAttributeObject() {
    assertEquals(config.getAttributeObject(LOG_LEVEL), "config");
    assertEquals(config.getAttributeObject(SECURITY_LOG_LEVEL), "config");
    assertEquals(config.getAttributeObject(REDUNDANCY_ZONE), "");
    assertEquals(config.getAttributeObject(ENABLE_CLUSTER_CONFIGURATION).getClass(), Boolean.class);
  }

  @Test
  public void testCheckerChecksValidAttribute() {
    for (String att : checkers.keySet()) {
      System.out.println("att = " + att);
      assertTrue(attributes.containsKey(att));
      Method checker = checkers.get(att);
      assertEquals(checker.getParameterCount(), 1);
      assertEquals("invalid checker: " + checker.getName(), checker.getReturnType(),
          checker.getParameterTypes()[0]);

      // TODO assert checker and setter accepts this same type of parameter
    }
  }

  @Test
  public void testDistributionConfigImplModifiable() {
    // default DistributionConfigImpl contains only 2 modifiable attributes
    List modifiables = new ArrayList<>();
    for (String attName : attNames) {
      if (config.isAttributeModifiable(attName)) {
        modifiables.add(attName);
      }
    }
    assertEquals(modifiables.size(), 2);
    assertEquals(modifiables.get(0), HTTP_SERVICE_PORT);
    assertEquals(modifiables.get(1), JMX_MANAGER_HTTP_PORT);
  }

  @Test
  public void testRuntimeConfigModifiable() {
    InternalDistributedSystem ds = mock(InternalDistributedSystem.class);
    when(ds.getOriginalConfig()).thenReturn(config);
    RuntimeDistributionConfigImpl runtime = new RuntimeDistributionConfigImpl(ds);
    List modifiables = new ArrayList<>();
    for (String attName : attNames) {
      if (runtime.isAttributeModifiable(attName)) {
        modifiables.add(attName);
      }
    }

    assertEquals(modifiables.size(), 10);
    assertEquals(modifiables.get(0), ARCHIVE_DISK_SPACE_LIMIT);
    assertEquals(modifiables.get(1), ARCHIVE_FILE_SIZE_LIMIT);
    assertEquals(modifiables.get(2), HTTP_SERVICE_PORT);
    assertEquals(modifiables.get(3), JMX_MANAGER_HTTP_PORT);
    assertEquals(modifiables.get(4), LOG_DISK_SPACE_LIMIT);
    assertEquals(modifiables.get(5), LOG_FILE_SIZE_LIMIT);
    assertEquals(modifiables.get(6), LOG_LEVEL);
    assertEquals(modifiables.get(7), STATISTIC_ARCHIVE_FILE);
    assertEquals(modifiables.get(8), STATISTIC_SAMPLE_RATE);
    assertEquals(modifiables.get(9), STATISTIC_SAMPLING_ENABLED);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetInvalidAttributeObject() {
    config.setAttributeObject("fake attribute", "test", ConfigSource.api());
  }

  @Test(expected = UnmodifiableException.class)
  public void testSetUnmodifiableAttributeObject() {
    config.setAttributeObject(ARCHIVE_DISK_SPACE_LIMIT, 0, ConfigSource.api());
  }

  @Test
  public void testValidAttributeObject() {
    config.setAttributeObject(HTTP_SERVICE_PORT, 8080, ConfigSource.api());
    assertEquals(config.getHttpServicePort(), 8080);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOutOfRangeAttributeObject() {
    config.setAttributeObject(HTTP_SERVICE_PORT, -1, ConfigSource.api());
  }

  @Test
  public void testLogLevel() {
    config.modifiable = true;
    config.setAttribute(LOG_LEVEL, "config", ConfigSource.api());
    assertEquals(config.getLogLevel(), 700);

    config.setAttributeObject(SECURITY_LOG_LEVEL, "debug", ConfigSource.api());
    assertEquals(config.getSecurityLogLevel(), 500);
  }

  @Test
  public void testValidLocatorAddress() {
    String address = "81.240.0.1[7056]";
    config.modifiable = true;
    config.setAttributeObject(START_LOCATOR, address, ConfigSource.api());
    assertEquals(config.getStartLocator(), address);
  }

  @Test(expected = InternalGemFireException.class)
  public void testInvalidLocatorAddress() {
    String address = "bad.bad[7056]";
    config.modifiable = true;
    // config.setStartLocator(address);
    config.setAttributeObject(START_LOCATOR, address, ConfigSource.api());
  }

  @Test
  public void testAttributesAlwaysModifiable() {
    config.modifiable = false;
    assertTrue(config.isAttributeModifiable(HTTP_SERVICE_PORT));
    assertTrue(config.isAttributeModifiable(JMX_MANAGER_HTTP_PORT));

    config.modifiable = true;
    assertTrue(config.isAttributeModifiable(HTTP_SERVICE_PORT));
    assertTrue(config.isAttributeModifiable(JMX_MANAGER_HTTP_PORT));
  }

  @Test
  public void testSecurityProps() {
    Properties props = new Properties();
    props.put(SECURITY_MANAGER, SampleSecurityManager.class.getName());
    props.put(SECURITY_POST_PROCESSOR, SamplePostProcessor.class.getName());
    props.put(SECURITY_LOG_LEVEL, "config");
    // add another non-security property to verify it won't get put in the security properties
    props.put(ACK_WAIT_THRESHOLD, 2);

    DistributionConfig config = new DistributionConfigImpl(props);
    // SECURITY_ENABLED_COMPONENTS is automatically added to getSecurityProps
    assertEquals(config.getSecurityProps().size(), 3);
  }

  @Test
  public void testSecurityPropsWithNoSetter() {
    Properties props = new Properties();
    props.put(SECURITY_MANAGER, SampleSecurityManager.class.getName());
    props.put(SECURITY_POST_PROCESSOR, SamplePostProcessor.class.getName());
    props.put(SECURITY_LOG_LEVEL, "config");
    // add another non-security property to verify it won't get put in the security properties
    props.put(ACK_WAIT_THRESHOLD, 2);
    props.put("security-username", "testName");

    DistributionConfig config = new DistributionConfigImpl(props);
    // SECURITY_ENABLED_COMPONENTS is automatically added to getSecurityProps
    assertEquals(config.getSecurityProps().size(), 4);
  }

  @Test
  public void testSSLEnabledComponents() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(SSL_ENABLED_COMPONENTS, "all");

    DistributionConfig config = new DistributionConfigImpl(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSSLEnabledComponentsLegacyFail() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(CLUSTER_SSL_ENABLED, "true");
    props.put(HTTP_SERVICE_SSL_ENABLED, "true");
    props.put(SSL_ENABLED_COMPONENTS, "all");

    DistributionConfig config = new DistributionConfigImpl(props);
  }

  @Test
  public void testSSLEnabledComponentsLegacyPass() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(CLUSTER_SSL_ENABLED, "true");
    props.put(HTTP_SERVICE_SSL_ENABLED, "true");
    props.put(SSL_ENABLED_COMPONENTS, "");

    DistributionConfig config = new DistributionConfigImpl(props);
  }
}
