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

package org.apache.geode.management.internal.configuration.validators;

import static org.apache.geode.cache.wan.GatewayReceiver.DEFAULT_END_PORT;
import static org.apache.geode.cache.wan.GatewayReceiver.DEFAULT_START_PORT;

import org.apache.geode.management.configuration.GatewayReceiver;
import org.apache.geode.management.internal.CacheElementOperation;

public class GatewayReceiverConfigValidator
    implements ConfigurationValidator<GatewayReceiver> {
  @Override
  public void validate(CacheElementOperation operation, GatewayReceiver config)
      throws IllegalArgumentException {

    if (operation == CacheElementOperation.CREATE) {
      if (config.getStartPort() == null) {
        config.setStartPort(DEFAULT_START_PORT + "");
      }

      if (config.getEndPort() == null) {
        config.setEndPort(DEFAULT_END_PORT + "");
      }

      if (Integer.parseInt(config.getStartPort()) > Integer.parseInt(config.getEndPort())) {
        throw new IllegalArgumentException("Start port " + config.getStartPort()
            + " must be less than the end port " + config.getEndPort() + ".");
      }
    }
  }
}
