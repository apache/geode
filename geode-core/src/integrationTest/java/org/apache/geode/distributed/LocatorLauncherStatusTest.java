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

package org.apache.geode.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.test.junit.rules.LocatorLauncherStartupRule;

public class LocatorLauncherStatusTest {
  private static Properties properties;

  static {
    CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();
    try {
      CertificateMaterial memberMaterial = new CertificateBuilder()
          .commonName("member")
          .issuedBy(ca)
          .generate();

      CertStores memberStore = new CertStores("member");
      memberStore.withCertificate("member", memberMaterial);
      memberStore.trust("ca", ca);

      properties = memberStore.propertiesWith("all", false, false);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @ClassRule
  public static LocatorLauncherStartupRule launcher =
      new LocatorLauncherStartupRule().withProperties(properties).withAutoStart();

  @Test
  public void status() {
    LocatorLauncher.LocatorState locatorState = LocatorLauncher.getLocatorState();
    assertThat(locatorState.getStatus().getDescription()).isEqualTo("online");
  }

}
