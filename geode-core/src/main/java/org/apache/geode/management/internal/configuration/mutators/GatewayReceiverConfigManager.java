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

package org.apache.geode.management.internal.configuration.mutators;

import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.internal.configuration.converters.GatewayReceiverConverter;

public class GatewayReceiverConfigManager extends CacheConfigurationManager<GatewayReceiver> {

  private final GatewayReceiverConverter converter = new GatewayReceiverConverter();

  public GatewayReceiverConfigManager(ConfigurationPersistenceService persistenceService) {
    super(persistenceService);
  }

  @Override
  public void add(GatewayReceiver config, CacheConfig existing) {
    existing.setGatewayReceiver(converter.fromConfigObject(config));
  }

  @Override
  public void update(GatewayReceiver config, CacheConfig existing) {
    existing.setGatewayReceiver(converter.fromConfigObject(config));
  }

  @Override
  public void delete(GatewayReceiver config, CacheConfig existing) {
    existing.setGatewayReceiver(null);
  }

  @Override
  public List<GatewayReceiver> list(GatewayReceiver filterConfig,
      CacheConfig existing) {
    GatewayReceiverConfig gatewayReceiver = existing.getGatewayReceiver();
    if (gatewayReceiver == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(converter.fromXmlObject(gatewayReceiver));
  }

  @Override
  public GatewayReceiver get(GatewayReceiver config, CacheConfig existing) {
    return converter.fromXmlObject(existing.getGatewayReceiver());
  }
}
