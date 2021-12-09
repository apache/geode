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
package org.apache.geode.internal.lang;

import static org.apache.geode.internal.lang.SystemProperty.DEFAULT_PREFIX;
import static org.apache.geode.internal.lang.SystemProperty.GEMFIRE_PREFIX;
import static org.apache.geode.internal.lang.SystemProperty.getProductBooleanProperty;
import static org.apache.geode.internal.lang.SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.restoreSetOperationTransactionBehavior;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class SystemPropertyHelperTest {

  private final String restoreSetOperationTransactionBehavior =
      "restoreSetOperationTransactionBehavior";

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void testRestoreSetOperationTransactionBehaviorDefaultToFalse() {
    assertThat(restoreSetOperationTransactionBehavior()).isFalse();
  }

  @Test
  public void testRestoreSetOperationTransactionBehaviorGemfireSystemProperty() {
    String gemfirePrefixProperty = "gemfire." + restoreSetOperationTransactionBehavior;
    System.setProperty(gemfirePrefixProperty, "true");
    assertThat(restoreSetOperationTransactionBehavior()).isTrue();
  }

  @Test
  public void testRestoreSetOperationTransactionBehaviorGeodeSystemProperty() {
    String geodePrefixProperty = "geode." + restoreSetOperationTransactionBehavior;
    System.setProperty(geodePrefixProperty, "true");
    assertThat(restoreSetOperationTransactionBehavior()).isTrue();
  }

  @Test
  public void testRestoreSetOperationTransactionBehaviorGeodePreference() {
    String gemfirePrefixProperty = "gemfire." + restoreSetOperationTransactionBehavior;
    String geodePrefixProperty = "geode." + restoreSetOperationTransactionBehavior;

    System.setProperty(geodePrefixProperty, "false");
    System.setProperty(gemfirePrefixProperty, "true");

    assertThat(restoreSetOperationTransactionBehavior()).isFalse();
  }

  @Test
  public void getBooleanPropertyReturnsEnableRetryOnPdxSerializationException() {
    String testProperty = "enableQueryRetryOnPdxSerializationException";
    String gemfirePrefixProperty = "gemfire." + testProperty;

    System.setProperty(gemfirePrefixProperty, "true");
    assertThat(getProductBooleanProperty(testProperty).get()).isTrue();

    System.clearProperty(gemfirePrefixProperty);
    assertThat(getProductBooleanProperty(testProperty).orElse(false)).isFalse();
  }

  @Test
  public void getBooleanPropertyParallelDiskStoreRecovery_default() {
    // default
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).isPresent()).isFalse();
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).orElse(true)).isTrue();
  }

  @Test
  public void getBooleanPropertyParallelDiskStoreRecovery_withoutPrefix() {
    // without geode or gemfire prefix
    System.setProperty(PARALLEL_DISK_STORE_RECOVERY, "true");
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).isPresent()).isFalse();
  }

  @Test
  public void getBooleanPropertyParallelDiskStoreRecovery_withGeodePrefix() {
    // with geode prefix
    System.setProperty(DEFAULT_PREFIX + PARALLEL_DISK_STORE_RECOVERY, "true");
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).get()).isTrue();
    System.setProperty(DEFAULT_PREFIX + PARALLEL_DISK_STORE_RECOVERY, "false");
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).get()).isFalse();
  }

  @Test
  public void getBooleanPropertyParallelDiskStoreRecovery_withGemFirePrefix() {
    // with gemfire prefix
    System.clearProperty(DEFAULT_PREFIX + PARALLEL_DISK_STORE_RECOVERY);
    System.setProperty(GEMFIRE_PREFIX + PARALLEL_DISK_STORE_RECOVERY, "true");
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).get()).isTrue();
    System.setProperty(GEMFIRE_PREFIX + PARALLEL_DISK_STORE_RECOVERY, "false");
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).get()).isFalse();
  }

  @Test
  public void getBooleanPropertyParallelDiskStoreRecovery_withBothPrefixes() {
    // with geode and gemfire prefix
    System.setProperty(DEFAULT_PREFIX + PARALLEL_DISK_STORE_RECOVERY, "true");
    System.setProperty(GEMFIRE_PREFIX + PARALLEL_DISK_STORE_RECOVERY, "false");
    assertThat(getProductBooleanProperty(PARALLEL_DISK_STORE_RECOVERY).get()).isTrue();
  }
}
