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

package org.apache.geode.management.internal.configuration.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.ParameterType;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.GatewayReceiver;

public class GatewayReceiverConverterTest {
  private final GatewayReceiverConverter converter = new GatewayReceiverConverter();

  @Test
  public void fromConfig() {
    GatewayReceiver config = new GatewayReceiver();
    config.setEndPort(2);
    Properties classNameProps = new Properties();
    classNameProps.setProperty("key1", "value1");
    config.setGatewayTransportFilters(
        Collections.singletonList(new ClassName("className", classNameProps)));
    config.setManualStart(true);
    config.setMaximumTimeBetweenPings(1000);
    config.setSocketBufferSize(1024);
    config.setStartPort(1);
    GatewayReceiverConfig xml = converter.fromConfigObject(config);
    assertThat(xml.isManualStart()).isTrue();
    assertThat(xml.getStartPort()).isEqualTo("1");
    assertThat(xml.getEndPort()).isEqualTo("2");
    assertThat(xml.getSocketBufferSize()).isEqualTo("1024");
    assertThat(xml.getMaximumTimeBetweenPings()).isEqualTo("1000");
    List<DeclarableType> xmlTransportFilters = xml.getGatewayTransportFilters();
    assertThat(xmlTransportFilters).hasSize(1);
    DeclarableType xmlFilter = xmlTransportFilters.get(0);
    assertThat(xmlFilter.getClassName()).isEqualTo("className");
    List<ParameterType> xmlFilterParams = xmlFilter.getParameters();
    assertThat(xmlFilterParams).hasSize(1);
    ParameterType xmlFilterParam = xmlFilterParams.get(0);
    assertThat(xmlFilterParam.getName()).isEqualTo("key1");
    assertThat(xmlFilterParam.getString()).isEqualTo("value1");
  }

  @Test
  public void fromConfigWithNoTransportFilters() {
    GatewayReceiver config = new GatewayReceiver();
    config.setEndPort(2);
    config.setManualStart(true);
    config.setMaximumTimeBetweenPings(1000);
    config.setSocketBufferSize(1024);
    config.setStartPort(1);
    GatewayReceiverConfig xml = converter.fromConfigObject(config);
    assertThat(xml.isManualStart()).isTrue();
    assertThat(xml.getStartPort()).isEqualTo("1");
    assertThat(xml.getEndPort()).isEqualTo("2");
    assertThat(xml.getSocketBufferSize()).isEqualTo("1024");
    assertThat(xml.getMaximumTimeBetweenPings()).isEqualTo("1000");
    assertThat(xml.getGatewayTransportFilters()).isEmpty();
  }

  @Test
  public void fromXmlObject() {
    GatewayReceiverConfig xml = new GatewayReceiverConfig();
    xml.setEndPort("2");
    xml.setManualStart(true);
    xml.setMaximumTimeBetweenPings("1000");
    xml.setSocketBufferSize("1024");
    xml.setStartPort("1");
    Properties filterProps = new Properties();
    filterProps.setProperty("key1", "value1");
    DeclarableType filter = new DeclarableType("className", filterProps);
    xml.getGatewayTransportFilters().add(filter);
    GatewayReceiver config = converter.fromXmlObject(xml);
    assertThat(config.isManualStart()).isTrue();
    assertThat(config.getEndPort()).isEqualTo(2);
    assertThat(config.getMaximumTimeBetweenPings()).isEqualTo(1000);
    assertThat(config.getSocketBufferSize()).isEqualTo(1024);
    assertThat(config.getStartPort()).isEqualTo(1);
    List<ClassName> configFilters = config.getGatewayTransportFilters();
    assertThat(configFilters).hasSize(1);
    ClassName configFilter = configFilters.get(0);
    assertThat(configFilter.getClassName()).isEqualTo("className");
    assertThat(configFilter.getInitProperties()).isEqualTo(filterProps);
  }

  @Test
  public void fromXmlWithNoTransportFilters() {
    GatewayReceiverConfig xml = new GatewayReceiverConfig();
    xml.setEndPort("2");
    xml.setManualStart(true);
    xml.setMaximumTimeBetweenPings("1000");
    xml.setSocketBufferSize("1024");
    xml.setStartPort("1");
    GatewayReceiver config = converter.fromXmlObject(xml);
    assertThat(config.isManualStart()).isTrue();
    assertThat(config.getEndPort()).isEqualTo(2);
    assertThat(config.getMaximumTimeBetweenPings()).isEqualTo(1000);
    assertThat(config.getSocketBufferSize()).isEqualTo(1024);
    assertThat(config.getStartPort()).isEqualTo(1);
    assertThat(config.getGatewayTransportFilters()).isNull();
  }

  @Test
  public void fromXmlWithBlankIntAttribute() {
    GatewayReceiverConfig xml = new GatewayReceiverConfig();
    xml.setEndPort("");
    GatewayReceiver config = converter.fromXmlObject(xml);
    assertThat(config.getEndPort()).isNull();
  }

  @Test
  public void fromConfigWithNullIntegerAttribute() {
    GatewayReceiver config = new GatewayReceiver();
    config.setEndPort(null);
    GatewayReceiverConfig xml = converter.fromConfigObject(config);
    assertThat(xml.getEndPort()).isNull();
  }

}
