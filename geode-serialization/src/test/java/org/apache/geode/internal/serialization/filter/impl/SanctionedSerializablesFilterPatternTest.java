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
package org.apache.geode.internal.serialization.filter.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.StringJoiner;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.serialization.filter.FilterPattern;
import org.apache.geode.internal.serialization.filter.SanctionedSerializablesFilterPattern;

public class SanctionedSerializablesFilterPatternTest {

  private String defaultPattern;

  @Before
  public void setUp() {
    defaultPattern = new SanctionedSerializablesFilterPattern().pattern();
  }

  @Test
  public void includesJavaWithSubpackages() {
    assertThat(defaultPattern).contains("java.**");
  }

  @Test
  public void includesJavaxManagementWithSubpackages() {
    assertThat(defaultPattern).contains("javax.management.**");
  }

  @Test
  public void includesEnumSyntax() {
    assertThat(defaultPattern).contains("javax.print.attribute.EnumSyntax");
  }

  @Test
  public void includesAntlrWithSubpackages() {
    assertThat(defaultPattern).contains("antlr.**");
  }

  @Test
  public void includesCommonsModelerAttributeInfo() {
    assertThat(defaultPattern).contains("org.apache.commons.modeler.AttributeInfo");
  }

  @Test
  public void includesCommonsModelerFeatureInfo() {
    assertThat(defaultPattern).contains("org.apache.commons.modeler.FeatureInfo");
  }

  @Test
  public void includesCommonsModelerManagedBean() {
    assertThat(defaultPattern).contains("org.apache.commons.modeler.ManagedBean");
  }

  @Test
  public void includesDistributionConfigSnapshot() {
    assertThat(defaultPattern)
        .contains("org.apache.geode.distributed.internal.DistributionConfigSnapshot");
  }

  @Test
  public void includesRuntimeDistributionConfigImpl() {
    assertThat(defaultPattern)
        .contains("org.apache.geode.distributed.internal.RuntimeDistributionConfigImpl");
  }

  @Test
  public void includesDistributionConfigImpl() {
    assertThat(defaultPattern)
        .contains("org.apache.geode.distributed.internal.DistributionConfigImpl");
  }

  @Test
  public void includesInternalDistributedMember() {
    assertThat(defaultPattern)
        .contains("org.apache.geode.distributed.internal.membership.InternalDistributedMember");
  }

  @Test
  public void includesPersistentMemberID() {
    assertThat(defaultPattern)
        .contains("org.apache.geode.internal.cache.persistence.PersistentMemberID");
  }

  @Test
  public void includesDiskStoreID() {
    assertThat(defaultPattern).contains("org.apache.geode.internal.cache.persistence.DiskStoreID");
  }

  @Test
  public void includesVersionedObjectList() {
    assertThat(defaultPattern)
        .contains("org.apache.geode.internal.cache.tier.sockets.VersionedObjectList");
  }

  @Test
  public void includesShiroPackage() {
    assertThat(defaultPattern).contains("org.apache.shiro.**");
  }

  @Test
  public void includesLog4jLevel() {
    assertThat(defaultPattern).contains("org.apache.logging.log4j.Level");
  }

  @Test
  public void includesLog4jStandardLevel() {
    assertThat(defaultPattern).contains("org.apache.logging.log4j.spi.StandardLevel");
  }

  @Test
  public void includesSunProxy() {
    assertThat(defaultPattern).contains("com.sun.proxy.$Proxy*");
  }

  @Test
  public void includesRmiioRemoteInputStream() {
    assertThat(defaultPattern).contains("com.healthmarketscience.rmiio.RemoteInputStream");
  }

  @Test
  public void includesSslRMIClientSocketFactory() {
    assertThat(defaultPattern).contains("javax.rmi.ssl.SslRMIClientSocketFactory");
  }

  @Test
  public void includesSSLHandshakeException() {
    assertThat(defaultPattern).contains("javax.net.ssl.SSLHandshakeException");
  }

  @Test
  public void includesSSLException() {
    assertThat(defaultPattern).contains("javax.net.ssl.SSLException");
  }

  @Test
  public void includesSunValidatorException() {
    assertThat(defaultPattern).contains("sun.security.validator.ValidatorException");
  }

  @Test
  public void includesSunSunCertPathBuilderException() {
    assertThat(defaultPattern)
        .contains("sun.security.provider.certpath.SunCertPathBuilderException");
  }

  @Test
  public void includesSessionCustomExpiry() {
    assertThat(defaultPattern).contains("org.apache.geode.modules.util.SessionCustomExpiry");
  }

  @Test
  public void rejectsAllOtherTypes() {
    assertThat(defaultPattern).endsWith(";!*");
  }

  @Test
  public void appendsToPattern() {
    String extraToAppend = "extra";
    String pattern = new SanctionedSerializablesFilterPattern()
        .append(extraToAppend)
        .pattern();

    assertThat(pattern).contains(extraToAppend);
  }

  @Test
  public void appendsJustBeforeRejectPattern() {
    String extraToAppend = "extra";
    String pattern = new SanctionedSerializablesFilterPattern()
        .append(extraToAppend)
        .pattern();

    String expectedEnd = new StringJoiner(";")
        .add(extraToAppend)
        .add("!*")
        .toString();

    assertThat(pattern).endsWith(expectedEnd);
  }

  @Test
  public void patternReturnsSameValueMultipleTimes() {
    FilterPattern sanctionedSerializablesFilterPattern =
        new SanctionedSerializablesFilterPattern();
    String expectedPattern = sanctionedSerializablesFilterPattern
        .pattern();

    assertThat(sanctionedSerializablesFilterPattern.pattern()).isEqualTo(expectedPattern);
    assertThat(sanctionedSerializablesFilterPattern.pattern()).isEqualTo(expectedPattern);
    assertThat(sanctionedSerializablesFilterPattern.pattern()).isEqualTo(expectedPattern);
  }
}
