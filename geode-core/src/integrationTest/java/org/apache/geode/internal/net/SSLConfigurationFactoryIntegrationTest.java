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

package org.apache.geode.internal.net;

import static org.apache.geode.internal.net.SSLConfigurationFactory.JAVAX_KEYSTORE;
import static org.apache.geode.internal.net.SSLConfigurationFactory.JAVAX_KEYSTORE_PASSWORD;
import static org.apache.geode.internal.net.SSLConfigurationFactory.JAVAX_KEYSTORE_TYPE;
import static org.apache.geode.internal.net.SSLConfigurationFactory.JAVAX_TRUSTSTORE;
import static org.apache.geode.internal.net.SSLConfigurationFactory.JAVAX_TRUSTSTORE_PASSWORD;
import static org.apache.geode.internal.net.SSLConfigurationFactory.JAVAX_TRUSTSTORE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

@Tag("membership")
class SSLConfigurationFactoryIntegrationTest {

  private static final String A_KEYSTORE = "a-keystore";
  private static final String A_KEYSTORE_TYPE = "a-keystore-type";
  private static final String A_KEYSTORE_PASSWORD = "a-keystore-password";
  private static final String A_TRUSTSTOR = "a-truststor";
  private static final String A_TRUSTSTOR_TYPE = "a-truststor-type";
  private static final String A_TRUSTSTOR_PASSWORD = "a-truststor-password";

  @Test
  @SetSystemProperty(key = JAVAX_KEYSTORE, value = A_KEYSTORE)
  @SetSystemProperty(key = JAVAX_KEYSTORE_TYPE, value = A_KEYSTORE_TYPE)
  @SetSystemProperty(key = JAVAX_KEYSTORE_PASSWORD, value = A_KEYSTORE_PASSWORD)
  @SetSystemProperty(key = JAVAX_TRUSTSTORE, value = A_TRUSTSTOR)
  @SetSystemProperty(key = JAVAX_TRUSTSTORE_TYPE, value = A_TRUSTSTOR_TYPE)
  @SetSystemProperty(key = JAVAX_TRUSTSTORE_PASSWORD, value = A_TRUSTSTOR_PASSWORD)
  void getSSLConfigUsingJavaProperties() {
    final Properties properties = new Properties();
    properties.setProperty(ConfigurationProperties.SSL_ENABLED_COMPONENTS, "cluster");
    final DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    final SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
        SecurableCommunicationChannel.CLUSTER);

    assertThat(sslConfig.isEnabled()).isTrue();
    assertThat(sslConfig.getKeystore()).isEqualTo(A_KEYSTORE);
    assertThat(sslConfig.getKeystoreType()).isEqualTo(A_KEYSTORE_TYPE);
    assertThat(sslConfig.getKeystorePassword()).isEqualTo(A_KEYSTORE_PASSWORD);
    assertThat(sslConfig.getTruststore()).isEqualTo(A_TRUSTSTOR);
    assertThat(sslConfig.getTruststoreType()).isEqualTo(A_TRUSTSTOR_TYPE);
    assertThat(sslConfig.getTruststorePassword()).isEqualTo(A_TRUSTSTOR_PASSWORD);
  }

}
