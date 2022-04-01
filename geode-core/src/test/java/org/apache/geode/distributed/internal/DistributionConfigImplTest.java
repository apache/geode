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

import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CLIENT_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_USE_DEFAULT_CONTEXT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SSL_CLIENT_PROTOCOLS;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SSL_PROTOCOLS;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_SSL_SERVER_PROTOCOLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.geode.UnmodifiableException;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.serialization.filter.SerializableObjectConfig;
import org.apache.geode.security.TestPostProcessor;
import org.apache.geode.security.TestSecurityManager;

@Tag("membership")
public class DistributionConfigImplTest {

  private Map<Class<?>, Class<?>> classMap;

  private Map<String, ConfigAttribute> attributes;
  private Map<String, Method> setters;
  private Map<String, Method> getters;
  private Map<String, Method> checkers;
  private String[] attNames;

  private DistributionConfigImpl config;

  @BeforeEach
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
    assertThat(attNames).hasSize(167);

    List<String> boolList = new ArrayList<>();
    List<String> intList = new ArrayList<>();
    List<String> fileList = new ArrayList<>();
    List<String> stringList = new ArrayList<>();
    List<String> otherList = new ArrayList<>();
    for (String attName : attNames) {
      Class<?> clazz = AbstractDistributionConfig._getAttributeType(attName);
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
    System.out.println("stringList: " + stringList);
    System.out.println();
    System.out.println("fileList: " + fileList);
    System.out.println();
    System.out.println("otherList: " + otherList);

    // TODO - This makes no sense. One has no idea what the correct expected number of attributes
    // are.
    assertThat(boolList).hasSize(35);
    assertThat(intList).hasSize(34);
    assertThat(stringList).hasSize(88);
    assertThat(fileList).hasSize(5);
    assertThat(otherList).hasSize(5);
  }

  @Test
  public void testAttributeDesc() {
    final String[] attNames = AbstractDistributionConfig._getAttNames();
    for (final String attName : attNames) {
      assertThat(AbstractDistributionConfig.dcAttDescriptions)
          .as("Does not contain description for attribute " + attName).containsKey(attName);
    }
    final List<String> attList = Arrays.asList(attNames);
    for (final String attName : AbstractDistributionConfig.dcAttDescriptions.keySet()) {
      if (!attList.contains(attName)) {
        System.out.println("Has unused description for " + attName);
      }
    }
  }

  @Test
  public void sameCount() {
    assertThat(setters).hasSameSizeAs(attributes);
    assertThat(getters).hasSameSizeAs(setters);
  }

  @Test
  public void everyAttrHasValidSetter() {
    for (String attr : attributes.keySet()) {
      Method setter = setters.get(attr);
      assertThat(setter).as(attr + " should have a setter").isNotNull();
      assertThat(setter.getName()).startsWith("set");
      assertThat(setter.getParameterCount()).isEqualTo(1);

      if (!(attr.equalsIgnoreCase(LOG_LEVEL) || attr.equalsIgnoreCase(SECURITY_LOG_LEVEL))) {
        Class<?> clazz = attributes.get(attr).type();
        try {
          setter.invoke(mock(DistributionConfig.class), any(clazz));
        } catch (Exception e) {
          throw new RuntimeException("Error calling setter " + setter.getName(), e);
        }
      }

    }
  }

  @Test
  public void internalPropertiesAreIgnoredInSameAsCheck() {
    DistributionConfigImpl config = new DistributionConfigImpl(new Properties());
    Properties configProperties = new Properties();
    configProperties.putAll(config.getProps());
    Set<String> internalAttributeNames = config.getInternalAttributeNames();
    assertThat(internalAttributeNames).isNotEmpty();
    // make sure that DS_QUORUM_CHECKER_NAME is tested (GEODE-8389)
    assertThat(internalAttributeNames).contains(DistributionConfig.DS_QUORUM_CHECKER_NAME);
    for (String attributeName : config.getInternalAttributeNames()) {
      assertThat(config.isInternalAttribute(attributeName))
          .withFailMessage(
              attributeName + " is not considered to be internal, but is annotated to be internal")
          .isTrue();
      assertThat(config.sameAs(DistributionConfigImpl.produce(configProperties, false)))
          .withFailMessage("sameAs failed for " + attributeName).isTrue();
      assertThat(config.sameAs(DistributionConfigImpl.produce(configProperties, true)))
          .withFailMessage("sameAs failed for " + attributeName).isTrue();
      configProperties.put(attributeName, new Object());
      assertThat(config.sameAs(DistributionConfigImpl.produce(configProperties, false)))
          .withFailMessage("sameAs failed for " + attributeName).isTrue();
      assertThat(config.sameAs(DistributionConfigImpl.produce(configProperties, true)))
          .withFailMessage("sameAs failed for " + attributeName).isTrue();
    }
  }

  @Test
  public void everyAttrHasValidGetter() {
    for (String attr : attributes.keySet()) {
      Method getter = getters.get(attr);
      assertThat(getter).as(attr + " should have a getter").isNotNull();
      assertThat(getter.getName().startsWith("get")).isTrue();
      assertThat(getter.getParameterCount()).isEqualTo(0);

      if (!(attr.equalsIgnoreCase(LOG_LEVEL) || attr.equalsIgnoreCase(SECURITY_LOG_LEVEL))) {
        Class<?> clazz = attributes.get(attr).type();
        Class<?> returnClass = getter.getReturnType();
        if (returnClass.isPrimitive()) {
          returnClass = classMap.get(returnClass);
        }
        assertThat(clazz).isEqualTo(returnClass);
      }
    }
  }

  @Test
  public void everyGetterSetterSameNameSameType() {
    for (String attr : getters.keySet()) {
      Method getter = getters.get(attr);
      Method setter = setters.get(attr);
      assertThat(setter).as("every getter should have a corresponding setter " + attr).isNotNull();
      String attrNameInGetterSignature = getter.getName().substring(3);
      assertThat(setter.getName()).contains(attrNameInGetterSignature);
      assertThat(getter.getReturnType()).isEqualTo(setter.getParameterTypes()[0]);
    }

    for (String attr : setters.keySet()) {
      Method getter = getters.get(attr);
      assertThat(getter).as("every setter should have a corresponding getter: " + attr).isNotNull();
    }
  }

  @Test
  public void everySetterHasAttributeDefined() {
    for (String attr : setters.keySet()) {
      ConfigAttribute configAttribute = attributes.get(attr);
      assertThat(configAttribute).as(attr + " should be defined a ConfigAttribute").isNotNull();
    }
  }

  @Test
  public void everyGetterHasAttributeDefined() {
    for (String attr : getters.keySet()) {
      ConfigAttribute configAttribute = attributes.get(attr);
      assertThat(configAttribute).as(attr + " should be defined a ConfigAttribute").isNotNull();
    }
  }

  @Test
  public void testGetAttributeObject() {
    assertThat(config.getAttributeObject(LOG_LEVEL)).isEqualTo("config");
    assertThat(config.getAttributeObject(SECURITY_LOG_LEVEL)).isEqualTo("config");
    assertThat(config.getAttributeObject(REDUNDANCY_ZONE)).isEqualTo("");
    assertThat(config.getAttributeObject(ENABLE_CLUSTER_CONFIGURATION)).isInstanceOf(Boolean.class);
  }

  @Test
  public void testCheckerChecksValidAttribute() {
    for (String att : checkers.keySet()) {
      System.out.println("att = " + att);
      assertThat(attributes).containsKey(att);
      Method checker = checkers.get(att);
      assertThat(checker.getParameterCount()).isEqualTo(1);
      assertThat(checker.getParameterTypes()[0]).as("invalid checker: " + checker.getName())
          .isEqualTo(checker.getReturnType());
      Method setter = setters.get(att);
      assertThat(checker.getParameterTypes()[0]).as("checker '" + checker.getName()
          + "' param type is '" + checker.getParameterTypes()[0] + "' but setter '"
          + setter.getName() + "' param type is '" + setter.getParameterTypes()[0] + "'")
          .isEqualTo(setter.getParameterTypes()[0]);
    }
  }

  @Test
  public void testDistributionConfigImplModifiable() {
    // default DistributionConfigImpl contains only 2 modifiable attributes
    List<String> modifiables = new ArrayList<>();
    for (String attName : attNames) {
      if (config.isAttributeModifiable(attName)) {
        modifiables.add(attName);
      }
    }
    assertThat(modifiables.size()).isEqualTo(2);
    assertThat(modifiables.get(0)).isEqualTo(HTTP_SERVICE_PORT);
    assertThat(modifiables.get(1)).isEqualTo(JMX_MANAGER_HTTP_PORT);
  }

  @Test
  public void testRuntimeConfigModifiable() {
    InternalDistributedSystem ds = mock(InternalDistributedSystem.class);
    when(ds.getOriginalConfig()).thenReturn(config);
    RuntimeDistributionConfigImpl runtime = new RuntimeDistributionConfigImpl(ds);
    List<String> modifiables = new ArrayList<>();
    for (String attName : attNames) {
      if (runtime.isAttributeModifiable(attName)) {
        modifiables.add(attName);
      }
    }

    assertThat(modifiables).hasSize(10);
    assertThat(modifiables.get(0)).isEqualTo(ARCHIVE_DISK_SPACE_LIMIT);
    assertThat(modifiables.get(1)).isEqualTo(ARCHIVE_FILE_SIZE_LIMIT);
    assertThat(modifiables.get(2)).isEqualTo(HTTP_SERVICE_PORT);
    assertThat(modifiables.get(3)).isEqualTo(JMX_MANAGER_HTTP_PORT);
    assertThat(modifiables.get(4)).isEqualTo(LOG_DISK_SPACE_LIMIT);
    assertThat(modifiables.get(5)).isEqualTo(LOG_FILE_SIZE_LIMIT);
    assertThat(modifiables.get(6)).isEqualTo(LOG_LEVEL);
    assertThat(modifiables.get(7)).isEqualTo(STATISTIC_ARCHIVE_FILE);
    assertThat(modifiables.get(8)).isEqualTo(STATISTIC_SAMPLE_RATE);
    assertThat(modifiables.get(9)).isEqualTo(STATISTIC_SAMPLING_ENABLED);
  }

  @Test()
  public void testSetInvalidAttributeObject() {
    assertThatThrownBy(
        () -> config.setAttributeObject("fake attribute", "test", ConfigSource.api()))
            .isInstanceOf(IllegalArgumentException.class);
  }

  @Test()
  public void testSetUnmodifiableAttributeObject() {
    assertThatThrownBy(
        () -> config.setAttributeObject(ARCHIVE_DISK_SPACE_LIMIT, 0, ConfigSource.api()))
            .isInstanceOf(UnmodifiableException.class);
  }

  @Test
  public void testValidAttributeObject() {
    config.setAttributeObject(HTTP_SERVICE_PORT, 8080, ConfigSource.api());
    assertThat(config.getHttpServicePort()).isEqualTo(8080);
  }

  @Test()
  public void testOutOfRangeAttributeObject() {
    assertThatThrownBy(() -> config.setAttributeObject(HTTP_SERVICE_PORT, -1, ConfigSource.api()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testLogLevel() {
    config.modifiable = true;
    config.setAttribute(LOG_LEVEL, "config", ConfigSource.api());
    assertThat(config.getLogLevel()).isEqualTo(700);

    config.setAttributeObject(SECURITY_LOG_LEVEL, "debug", ConfigSource.api());
    assertThat(config.getSecurityLogLevel()).isEqualTo(500);
  }

  @Test
  public void testLog4jLogLevel() {
    config.modifiable = true;
    config.setAttribute(LOG_LEVEL, "fatal", ConfigSource.api());
    assertThat(config.getLogLevel()).isEqualTo(1000);
  }

  @Test
  public void testValidLocatorAddress() {
    String address = "81.240.0.1[7056]";
    config.modifiable = true;
    config.setAttributeObject(START_LOCATOR, address, ConfigSource.api());
    assertThat(config.getStartLocator()).isEqualTo(address);
  }

  @Test
  public void testInvalidLocatorAddressDoesntThrowException() {
    String address = "bad.bad[7056]";
    config.modifiable = true;
    // config.setStartLocator(address);
    config.setAttributeObject(START_LOCATOR, address, ConfigSource.api());
  }

  @Test
  public void testAttributesAlwaysModifiable() {
    config.modifiable = false;
    assertThat(config.isAttributeModifiable(HTTP_SERVICE_PORT)).isTrue();
    assertThat(config.isAttributeModifiable(JMX_MANAGER_HTTP_PORT)).isTrue();

    config.modifiable = true;
    assertThat(config.isAttributeModifiable(HTTP_SERVICE_PORT)).isTrue();
    assertThat(config.isAttributeModifiable(JMX_MANAGER_HTTP_PORT)).isTrue();
  }

  @Test
  public void testSecurityProps() {
    Properties props = new Properties();
    props.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
    props.put(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName());
    props.put(SECURITY_LOG_LEVEL, "config");
    // add another non-security property to verify it won't get put in the security properties
    props.put(ACK_WAIT_THRESHOLD, 2);

    DistributionConfig config = new DistributionConfigImpl(props);
    // SECURITY_ENABLED_COMPONENTS is automatically added to getSecurityProps
    assertThat(config.getSecurityProps().size()).isEqualTo(3);
  }

  @Test
  public void testSecurityPropsWithNoSetter() {
    Properties props = new Properties();
    props.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
    props.put(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName());
    props.put(SECURITY_LOG_LEVEL, "config");
    // add another non-security property to verify it won't get put in the security properties
    props.put(ACK_WAIT_THRESHOLD, 2);
    props.put("security-username", "testName");

    DistributionConfig config = new DistributionConfigImpl(props);
    // SECURITY_ENABLED_COMPONENTS is automatically added to getSecurityProps
    assertThat(config.getSecurityProps().size()).isEqualTo(4);
  }

  @Test
  public void testSSLEnabledComponents() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(SSL_ENABLED_COMPONENTS, "all");

    DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSecurableCommunicationChannels()).containsExactlyInAnyOrder(
        SecurableCommunicationChannel.ALL);
  }

  @Test
  public void testSSLEnabledComponentsLegacyFail() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(CLUSTER_SSL_ENABLED, "true");
    props.put(HTTP_SERVICE_SSL_ENABLED, "true");
    props.put(SSL_ENABLED_COMPONENTS, "all");

    assertThatThrownBy(() -> new DistributionConfigImpl(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testSSLEnabledComponentsLegacyPass() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(CLUSTER_SSL_ENABLED, "true");
    props.put(HTTP_SERVICE_SSL_ENABLED, "true");
    props.put(SSL_ENABLED_COMPONENTS, "");

    DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getClusterSSLEnabled()).isTrue();
    assertThat(config.getHttpServiceSSLEnabled()).isTrue();
  }

  @Test
  public void testSSLEnabledEndpointValidationIsSetDefaultToTrueWhenSetUseDefaultContextIsUsed() {
    Properties props = new Properties();
    props.put(SSL_ENABLED_COMPONENTS, "all");
    props.put(SSL_USE_DEFAULT_CONTEXT, "true");

    DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSSLEndPointIdentificationEnabled()).isEqualTo(true);
  }

  @Test
  public void testSSLEnabledEndpointValidationIsSetDefaultToFalseWhenDefaultContextNotUsed() {
    Properties props = new Properties();
    props.put(SSL_ENABLED_COMPONENTS, "all");

    DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSSLEndPointIdentificationEnabled()).isEqualTo(false);
  }

  @Test
  public void testSSLUseEndpointValidationIsSet() {
    Properties props = new Properties();
    props.put(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSSLEndPointIdentificationEnabled()).isEqualTo(true);
  }

  @Test
  public void invalidAuthToken() {
    Properties props = new Properties();
    props.put(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS, "manager");
    assertThatThrownBy(() -> new DistributionConfigImpl(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void authTokenIsCaseInsensitive() {
    Properties props = new Properties();
    props.put(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS, "MANAGEment");
    DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSecurityAuthTokenEnabledComponents()).containsExactly("MANAGEMENT");
  }

  @Test
  public void isASerializableObjectConfig() {
    DistributionConfig config = new DistributionConfigImpl(new Properties());

    assertThat(config).isInstanceOf(SerializableObjectConfig.class);
  }

  @Test
  void getSSLProtocolsReturnsValueOfPropertySSL_PROTOCOLS() {
    final Properties props = new Properties();
    final String protocols = "SuperProtocol1";
    props.put(SSL_PROTOCOLS, protocols);
    final DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSSLProtocols()).isEqualTo(protocols);
  }

  @Test
  void getSSLProtocolsReturnsDefaultValue() {
    final DistributionConfig config = new DistributionConfigImpl(new Properties());
    assertThat(config.getSSLProtocols()).isEqualTo(DEFAULT_SSL_PROTOCOLS);
  }

  @Test
  void getSSLProtocolsReturnsValueFromSetSSLProtocols() {
    final DistributionConfig config = new DistributionConfigImpl(new Properties());
    final String protocols = "SuperProtocol1";
    config.setSSLProtocols(protocols);
    assertThat(config.getSSLProtocols()).isEqualTo(protocols);
  }

  @Test
  void getSSLProtocolsReturnsValueAfterCopyConstructed() {
    final Properties props = new Properties();
    final String protocols = "SuperProtocol1";
    props.put(SSL_PROTOCOLS, protocols);
    final DistributionConfig config = new DistributionConfigImpl(props);
    final DistributionConfig copy = new DistributionConfigImpl(config);
    assertThat(copy.getSSLProtocols()).isEqualTo(protocols);
  }

  @Test
  void equalsConsidersSSLProtocols() {
    final DistributionConfig configA = new DistributionConfigImpl(new Properties());
    final DistributionConfig configB = new DistributionConfigImpl(new Properties());
    assertThat(configA).isEqualTo(configB);
    final String protocols = "SuperProtocol1";
    configA.setSSLProtocols(protocols);
    assertThat(configA).isNotEqualTo(configB);
    configB.setSSLProtocols(protocols);
    assertThat(configA).isEqualTo(configB);
  }

  @Test
  void hashCodeConsidersSSLProtocols() {
    final DistributionConfig configA = new DistributionConfigImpl(new Properties());
    final DistributionConfig configB = new DistributionConfigImpl(new Properties());
    assertThat(configA).hasSameHashCodeAs(configB);
    final String protocols = "SuperProtocol1";
    configA.setSSLProtocols(protocols);
    assertThat(configA).doesNotHaveSameHashCodeAs(configB);
    configB.setSSLProtocols(protocols);
    assertThat(configA).hasSameHashCodeAs(configB);
  }

  @Test
  void getSSLClientProtocolsReturnsValueOfPropertySSL_CLIENT_PROTOCOLS() {
    final Properties props = new Properties();
    final String protocols = "SuperProtocol1";
    props.put(SSL_CLIENT_PROTOCOLS, protocols);
    final DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSSLClientProtocols()).isEqualTo(protocols);
  }

  @Test
  void getSSLClientProtocolsReturnsDefaultValue() {
    final DistributionConfig config = new DistributionConfigImpl(new Properties());
    assertThat(config.getSSLClientProtocols()).isEqualTo(DEFAULT_SSL_CLIENT_PROTOCOLS);
  }

  @Test
  void getSSLClientProtocolsReturnsValueFromSetSSLClientProtocols() {
    final DistributionConfig config = new DistributionConfigImpl(new Properties());
    final String protocols = "SuperProtocol1";
    config.setSSLClientProtocols(protocols);
    assertThat(config.getSSLClientProtocols()).isEqualTo(protocols);
  }

  @Test
  void getSSLClientProtocolsReturnsValueAfterCopyConstructed() {
    final Properties props = new Properties();
    final String protocols = "SuperProtocol1";
    props.put(SSL_CLIENT_PROTOCOLS, protocols);
    final DistributionConfig config = new DistributionConfigImpl(props);
    final DistributionConfig copy = new DistributionConfigImpl(config);
    assertThat(copy.getSSLClientProtocols()).isEqualTo(protocols);
  }

  @Test
  void equalsConsidersSSLClientProtocols() {
    final DistributionConfig configA = new DistributionConfigImpl(new Properties());
    final DistributionConfig configB = new DistributionConfigImpl(new Properties());
    assertThat(configA).isEqualTo(configB);
    final String protocols = "SuperProtocol1";
    configA.setSSLClientProtocols(protocols);
    assertThat(configA).isNotEqualTo(configB);
    configB.setSSLClientProtocols(protocols);
    assertThat(configA).isEqualTo(configB);
  }

  @Test
  void hashCodeConsidersSSLClientProtocols() {
    final DistributionConfig configA = new DistributionConfigImpl(new Properties());
    final DistributionConfig configB = new DistributionConfigImpl(new Properties());
    assertThat(configA).hasSameHashCodeAs(configB);
    final String protocols = "SuperProtocol1";
    configA.setSSLClientProtocols(protocols);
    assertThat(configA).doesNotHaveSameHashCodeAs(configB);
    configB.setSSLClientProtocols(protocols);
    assertThat(configA).hasSameHashCodeAs(configB);
  }

  @Test
  void getSSLServerProtocolsReturnsValueOfPropertySSL_SERVER_PROTOCOLS() {
    final Properties props = new Properties();
    final String protocols = "SuperProtocol1";
    props.put(SSL_SERVER_PROTOCOLS, protocols);
    final DistributionConfig config = new DistributionConfigImpl(props);
    assertThat(config.getSSLServerProtocols()).isEqualTo(protocols);
  }

  @Test
  void getSSLServerProtocolsReturnsDefaultValue() {
    final DistributionConfig config = new DistributionConfigImpl(new Properties());
    assertThat(config.getSSLServerProtocols()).isEqualTo(DEFAULT_SSL_SERVER_PROTOCOLS);
  }

  @Test
  void getSSLServerProtocolsReturnsValueFromSetSSLServerProtocols() {
    final DistributionConfig config = new DistributionConfigImpl(new Properties());
    final String protocols = "SuperProtocol1";
    config.setSSLServerProtocols(protocols);
    assertThat(config.getSSLServerProtocols()).isEqualTo(protocols);
  }

  @Test
  void getSSLServerProtocolsReturnsValueAfterCopyConstructed() {
    final Properties props = new Properties();
    final String protocols = "SuperProtocol1";
    props.put(SSL_SERVER_PROTOCOLS, protocols);
    final DistributionConfig config = new DistributionConfigImpl(props);
    final DistributionConfig copy = new DistributionConfigImpl(config);
    assertThat(copy.getSSLServerProtocols()).isEqualTo(protocols);
  }

  @Test
  void equalsConsidersSSLServerProtocols() {
    final DistributionConfig configA = new DistributionConfigImpl(new Properties());
    final DistributionConfig configB = new DistributionConfigImpl(new Properties());
    assertThat(configA).isEqualTo(configB);
    final String protocols = "SuperProtocol1";
    configA.setSSLServerProtocols(protocols);
    assertThat(configA).isNotEqualTo(configB);
    configB.setSSLServerProtocols(protocols);
    assertThat(configA).isEqualTo(configB);
  }

  @Test
  void hashCodeConsidersSSLServerProtocols() {
    final DistributionConfig configA = new DistributionConfigImpl(new Properties());
    final DistributionConfig configB = new DistributionConfigImpl(new Properties());
    assertThat(configA).hasSameHashCodeAs(configB);
    final String protocols = "SuperProtocol1";
    configA.setSSLServerProtocols(protocols);
    assertThat(configA).doesNotHaveSameHashCodeAs(configB);
    configB.setSSLServerProtocols(protocols);
    assertThat(configA).hasSameHashCodeAs(configB);
  }

}
