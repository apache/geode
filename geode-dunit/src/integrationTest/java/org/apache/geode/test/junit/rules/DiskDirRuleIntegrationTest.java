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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;

/**
 * Integration tests for {@link DiskDirRule}.
 */
public class DiskDirRuleIntegrationTest {

  @Rule
  public DiskDirRule diskDirRule = new DiskDirRule();

  @Rule
  public TestName testName = new TestName();

  private String diskDirPath;
  private InternalCache cache;

  @Before
  public void setUp() {
    diskDirPath = diskDirRule.getDiskDir().getAbsolutePath();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void diskDirPathContainsTestClassName() {
    assertThat(diskDirPath).containsOnlyOnce(getClass().getName());
  }

  @Test
  public void diskDirPathContainsTestMethodName() {
    assertThat(diskDirPath).containsOnlyOnce(testName.getMethodName());
  }

  @Test
  public void diskDirPathContainsDiskDirsLiteral() {
    assertThat(diskDirPath).containsOnlyOnce("diskDirs");
  }

  @Test
  public void setsDefaultDiskDirsSystemProperty() {
    String propertyValue = System.getProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY);

    assertThat(propertyValue).isEqualTo(diskDirRule.getDiskDir().getAbsolutePath());
  }

  @Test
  public void cacheUsesDefaultDiskDirProvidedByRule() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").create();

    DiskStoreImpl diskStore = cache.getOrCreateDefaultDiskStore();
    File[] diskDirs = diskStore.getDiskDirs();

    assertThat(diskDirs).containsExactly(diskDirRule.getDiskDir());
  }
}
