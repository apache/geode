/*
 * Copyright 2022 VMware, Inc.
 * https://network.tanzu.vmware.com/legal_documents/vmware_eula
 */
package org.apache.geode.internal.stats50;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;

public class VMStats50Test {
  @Test
  public void verifyCpuBeanExists() {
    assertThat(VMStats50.cpuBean).isNotNull();
  }

  @Test
  @EnabledOnOs(WINDOWS)
  public void unixBeanNullOnWindows() {
    assertThat(VMStats50.unixBean).isNull();
  }

  @Test
  @DisabledOnOs(WINDOWS)
  public void unixBeanExistsOnUnix() {
    assertThat(VMStats50.unixBean).isNotNull();
  }
}
