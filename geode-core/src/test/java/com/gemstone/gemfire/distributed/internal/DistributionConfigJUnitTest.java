/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal;

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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.UnmodifiableException;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.management.internal.security.JSONAuthorization;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(UnitTest.class)
public class DistributionConfigJUnitTest {
  static Map<String, ConfigAttribute> attributes;
  static Map<String, Method> setters;
  static Map<String, Method> getters;
  static Map<String, Method> isModifiables;
  static Map<String, Method> checkers;
  static String[] attNames;
  DistributionConfigImpl config;

  @BeforeClass
  public static void beforeClass() {
    attributes = DistributionConfig.attributes;
    setters = DistributionConfig.setters;
    getters = DistributionConfig.getters;
    attNames = DistributionConfig.dcValidAttributeNames;
    checkers = AbstractDistributionConfig.checkers;
  }

  @Before
  public void before() {
    config = new DistributionConfigImpl(new Properties());
  }

  @Test
  public void testGetAttributeNames() {
    String[] attNames = AbstractDistributionConfig._getAttNames();
    assertEquals(attNames.length, 141);

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
    assertEquals(boolList.size(), 30);
    assertEquals(intList.size(), 33);
    assertEquals(stringList.size(), 70);
    assertEquals(fileList.size(), 5);
    assertEquals(otherList.size(), 3);
  }

  @Test
  public void testAttributeDesc(){
    String[] attNames = AbstractDistributionConfig._getAttNames();
    for(String attName:attNames){
      assertTrue("Does not contain description for attribute "+ attName, AbstractDistributionConfig.dcAttDescriptions.containsKey(attName));
    }
    List<String> attList = Arrays.asList(attNames);
    for(Object attName:AbstractDistributionConfig.dcAttDescriptions.keySet()){
      if(!attList.contains(attName)){
        System.out.println("Has unused description for "+attName.toString());
      }
      //assertTrue("Has unused description for "+attName.toString(), attList.contains(attName));
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

      if (!(attr.equalsIgnoreCase(DistributionConfig.LOG_LEVEL_NAME) || attr.equalsIgnoreCase(DistributionConfig.SECURITY_LOG_LEVEL_NAME))) {
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

      if (!(attr.equalsIgnoreCase(DistributionConfig.LOG_LEVEL_NAME) || attr.equalsIgnoreCase(DistributionConfig.SECURITY_LOG_LEVEL_NAME))) {
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
      assertEquals(setterName.substring(setterName.indexOf("set") + 3), getterName.substring(getterName.indexOf("get") + 3));
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
    assertEquals(config.getAttributeObject(DistributionConfig.LOG_LEVEL_NAME), "config");
    assertEquals(config.getAttributeObject(DistributionConfig.SECURITY_LOG_LEVEL_NAME), "config");
    assertEquals(config.getAttributeObject(DistributionConfig.REDUNDANCY_ZONE_NAME), "");
    assertEquals(config.getAttributeObject(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME).getClass(), Boolean.class);
  }

  @Test
  public void testCheckerChecksValidAttribute() {
    for (String att : checkers.keySet()) {
      assertTrue(attributes.containsKey(att));
      Method checker = checkers.get(att);
      assertEquals(checker.getParameterCount(), 1);
      assertEquals("invalid checker: " + checker.getName(), checker.getReturnType(), checker.getParameterTypes()[0]);

      //TODO assert checker and setter accepts this same type of parameter
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
    assertEquals(modifiables.get(0), "http-service-port");
    assertEquals(modifiables.get(1), "jmx-manager-http-port");
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
    assertEquals(modifiables.get(0), "archive-disk-space-limit");
    assertEquals(modifiables.get(1), "archive-file-size-limit");
    assertEquals(modifiables.get(2), "http-service-port");
    assertEquals(modifiables.get(3), "jmx-manager-http-port");
    assertEquals(modifiables.get(4), "log-disk-space-limit");
    assertEquals(modifiables.get(5), "log-file-size-limit");
    assertEquals(modifiables.get(6), "log-level");
    assertEquals(modifiables.get(7), "statistic-archive-file");
    assertEquals(modifiables.get(8), "statistic-sample-rate");
    assertEquals(modifiables.get(9), "statistic-sampling-enabled");

  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetInvalidAttributeObject() {
    config.setAttributeObject("fake attribute", "test", ConfigSource.api());
  }

  @Test(expected = UnmodifiableException.class)
  public void testSetUnmodifiableAttributeObject() {
    config.setAttributeObject("archive-disk-space-limit", 0, ConfigSource.api());
  }

  @Test
  public void testValidAttributeObject() {
    config.setAttributeObject("http-service-port", 8080, ConfigSource.api());
    assertEquals(config.getHttpServicePort(), 8080);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOutOfRangeAttributeObject() {
    config.setAttributeObject("http-service-port", -1, ConfigSource.api());
  }

  @Test
  public void testLogLevel() {
    config.modifiable = true;
    config.setAttribute(DistributionConfig.LOG_LEVEL_NAME, "config", ConfigSource.api());
    assertEquals(config.getLogLevel(), 700);

    config.setAttributeObject(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "debug", ConfigSource.api());
    assertEquals(config.getSecurityLogLevel(), 500);
  }

  @Test
  public void testValidLocatorAddress() {
    String address = "81.240.0.1[7056]";
    config.modifiable = true;
    config.setStartLocator(address);
    assertEquals(config.getStartLocator(), address);
  }

  @Test(expected = InternalGemFireException.class)
  public void testInvalidLocatorAddress() {
    String address = "bad.bad[7056]";
    config.modifiable = true;
    config.setStartLocator(address);
  }

  @Test
  public void testAttributesAlwaysModifiable() {
    config.modifiable = false;
    assertTrue(config.isAttributeModifiable(DistributionConfig.HTTP_SERVICE_PORT_NAME));
    assertTrue(config.isAttributeModifiable("jmx-manager-http-port"));

    config.modifiable = true;
    assertTrue(config.isAttributeModifiable(DistributionConfig.HTTP_SERVICE_PORT_NAME));
    assertTrue(config.isAttributeModifiable("jmx-manager-http-port"));
  }


  @Test
  public void testSecurityProps(){
    Properties props = new Properties();
    props.put(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, JSONAuthorization.class.getName() + ".create");
    props.put(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME, JSONAuthorization.class.getName() + ".create");
    props.put(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "config");
    // add another non-security property to verify it won't get put in the security properties
    props.put(DistributionConfig.ACK_WAIT_THRESHOLD_NAME, 2);

    DistributionConfig config = new DistributionConfigImpl(props);
    assertEquals(config.getSecurityProps().size(), 3);
  }

  @Test
  public void testSecurityPropsWithNoSetter(){
    Properties props = new Properties();
    props.put(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, JSONAuthorization.class.getName() + ".create");
    props.put(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME, JSONAuthorization.class.getName() + ".create");
    props.put(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "config");
    // add another non-security property to verify it won't get put in the security properties
    props.put(DistributionConfig.ACK_WAIT_THRESHOLD_NAME, 2);
    props.put("security-username", "testName");

    DistributionConfig config = new DistributionConfigImpl(props);
    assertEquals(config.getSecurityProps().size(), 4);
  }

  public final static Map<Class<?>, Class<?>> classMap = new HashMap<Class<?>, Class<?>>();

  static {
    classMap.put(boolean.class, Boolean.class);
    classMap.put(byte.class, Byte.class);
    classMap.put(short.class, Short.class);
    classMap.put(char.class, Character.class);
    classMap.put(int.class, Integer.class);
    classMap.put(long.class, Long.class);
    classMap.put(float.class, Float.class);
    classMap.put(double.class, Double.class);
  }
}
