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
package org.apache.geode.modules.session.bootstrap;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.distributed.internal.AbstractDistributionConfig;


public abstract class AbstractCacheTest {
  protected List<String> overriddenProperties =
      Arrays.asList(CACHE_XML_FILE, LOG_FILE, STATISTIC_ARCHIVE_FILE, STATISTIC_SAMPLING_ENABLED);
  protected AbstractCache abstractCache;

  @Test
  public void setPropertyAcceptsKnownProperties() {
    for (String gemfireProperty : AbstractDistributionConfig._getAttNames()) {
      abstractCache.setProperty(gemfireProperty, gemfireProperty + "_value");
    }

    Properties properties = abstractCache.createDistributedSystemProperties();
    Arrays.stream(AbstractDistributionConfig._getAttNames())
        .filter(key -> !overriddenProperties.contains(key))
        .forEach(key -> assertThat(properties).containsEntry(key, key + "_value"));
  }

  @Test
  public void setPropertyIgnoresInvalidProperties() {
    abstractCache.setProperty("invalid-property", "true");
    assertThat(abstractCache.createDistributedSystemProperties())
        .doesNotContainKey("invalid-property");
  }

  @Test
  public void setPropertyAcceptsSecurityProperties() {
    abstractCache.setProperty("security-my-property", "custom-configuration-prop");
    assertThat(abstractCache.createDistributedSystemProperties())
        .containsEntry("security-my-property", "custom-configuration-prop");
  }
}
