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

import static org.apache.geode.distributed.internal.DistributionConfig.DS_CONFIG_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.DS_QUORUM_CHECKER_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.DS_RECONNECTING_NAME;

import java.util.Properties;
import java.util.function.Supplier;

import org.apache.geode.distributed.internal.membership.api.QuorumChecker;

public class ConnectionConfigImpl implements ConnectionConfig {
  private final boolean isReconnecting;
  private final QuorumChecker quorumChecker;
  private final DistributionConfigImpl distributionConfig;

  ConnectionConfigImpl(Properties properties) {
    isReconnecting = convert(properties.get(DS_RECONNECTING_NAME), Boolean.class,
        () -> Boolean.FALSE);
    quorumChecker = convert(properties.get(DS_QUORUM_CHECKER_NAME), QuorumChecker.class,
        () -> null);
    distributionConfig = convert(properties.get(DS_CONFIG_NAME), DistributionConfigImpl.class,
        () -> new DistributionConfigImpl(removeNonUserProperties(properties)));
  }

  @Override
  public boolean isReconnecting() {
    return isReconnecting;
  }

  @Override
  public QuorumChecker quorumChecker() {
    return quorumChecker;
  }

  @Override
  public DistributionConfig distributionConfig() {
    return distributionConfig;
  }

  /**
   * Remove the non distribution config properties so that they are not passed to the {@code
   * DistributionConfigImpl} constructor
   */
  private static Properties removeNonUserProperties(Properties properties) {
    Properties cleanedProperties = new Properties();
    properties.forEach(cleanedProperties::put);
    cleanedProperties.remove(DS_QUORUM_CHECKER_NAME);
    cleanedProperties.remove(DS_RECONNECTING_NAME);
    cleanedProperties.remove(DS_CONFIG_NAME);
    return cleanedProperties;
  }

  /**
   * Casts the object to the specified type, or returns the default value if the object cannot be
   * cast.
   */
  private static <T> T convert(Object object, Class<T> type, Supplier<T> defaultValueSupplier) {
    return type.isInstance(object) ? type.cast(object) : defaultValueSupplier.get();
  }
}
