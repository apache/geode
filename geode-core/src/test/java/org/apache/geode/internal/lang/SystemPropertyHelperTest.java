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

import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_PREFIX;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
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
    assertThat(SystemPropertyHelper.restoreSetOperationTransactionBehavior()).isFalse();
  }

  @Test
  public void testRestoreSetOperationTransactionBehaviorSystemProperty() {
    String gemfirePrefixProperty = "gemfire." + restoreSetOperationTransactionBehavior;
    System.setProperty(gemfirePrefixProperty, "true");
    assertThat(SystemPropertyHelper.restoreSetOperationTransactionBehavior()).isTrue();
    System.clearProperty(gemfirePrefixProperty);

    String geodePrefixProperty = "geode." + restoreSetOperationTransactionBehavior;
    System.setProperty(geodePrefixProperty, "true");
    assertThat(SystemPropertyHelper.restoreSetOperationTransactionBehavior()).isTrue();
    System.clearProperty(geodePrefixProperty);
  }

  @Test
  public void testRestoreSetOperationTransactionBehaviorGeodePreference() {
    String gemfirePrefixProperty = "gemfire." + restoreSetOperationTransactionBehavior;
    String geodePrefixProperty = "geode." + restoreSetOperationTransactionBehavior;
    System.setProperty(geodePrefixProperty, "false");
    System.setProperty(gemfirePrefixProperty, "true");
    assertThat(SystemPropertyHelper.restoreSetOperationTransactionBehavior()).isFalse();
    System.clearProperty(geodePrefixProperty);
    System.clearProperty(gemfirePrefixProperty);
  }

  @Test
  public void getBooleanPropertyReturnsEnableRetryOnPdxSerializationException() {
    String testProperty = "enableQueryRetryOnPdxSerializationException";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "true");
    assertThat(SystemProperty.getProductBooleanProperty(testProperty).get()).isTrue();
    System.clearProperty(gemfirePrefixProperty);
    assertThat(SystemProperty.getProductBooleanProperty(testProperty).orElse(false))
        .isFalse();
  }

  @Test
  public void getBooleanPropertyParallelDiskStoreRecovery() {
    // default
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).isPresent())
            .isFalse();
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).orElse(true))
            .isTrue();

    // without geode or gemfire prefix
    System.setProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "true");
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).isPresent())
            .isFalse();

    // with geode prefix
    System.setProperty(DEFAULT_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "true");
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).get())
            .isTrue();
    System.setProperty(DEFAULT_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "false");
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).get())
            .isFalse();

    // with gemfire prefix
    System.clearProperty(DEFAULT_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY);
    System.setProperty(GEMFIRE_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "true");
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).get())
            .isTrue();
    System.setProperty(GEMFIRE_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "false");
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).get())
            .isFalse();

    // with geode and gemfire prefix
    System.setProperty(DEFAULT_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "true");
    System.setProperty(GEMFIRE_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY, "false");
    assertThat(SystemProperty
        .getProductBooleanProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY).get())
            .isTrue();

    System.clearProperty(SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY);
    System.clearProperty(DEFAULT_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY);
    System.clearProperty(GEMFIRE_PREFIX + SystemPropertyHelper.PARALLEL_DISK_STORE_RECOVERY);
  }
}
