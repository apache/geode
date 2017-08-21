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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.util.test.TestUtil.getResourcePath;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.test.dunit.rules.gfsh.GfshRule;
import org.apache.geode.test.dunit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.categories.AcceptanceTest;

@Category(AcceptanceTest.class)
public class GfshConnectToLocatorWithSSLAcceptanceTest {
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File sslPropertiesFile;

  @Before
  public void setup() throws IOException {
    File jks = new File(getResourcePath(getClass(), "/ssl/trusted.keystore"));
    assertThat(jks).exists();

    Properties serverProps = new Properties();
    serverProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.ALL);
    serverProps.setProperty(SSL_KEYSTORE, jks.getAbsolutePath());
    serverProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    serverProps.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    serverProps.setProperty(SSL_TRUSTSTORE, jks.getAbsolutePath());
    serverProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    serverProps.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    serverProps.setProperty(SSL_CIPHERS, "any");
    serverProps.setProperty(SSL_PROTOCOLS, "any");

    sslPropertiesFile = temporaryFolder.newFile("ssl.properties");
    serverProps.store(new FileOutputStream(sslPropertiesFile), null);

    GfshScript startLocator =
        GfshScript.of("start locator --name=locator --security-properties-file="
            + sslPropertiesFile.getAbsolutePath());
    gfshRule.execute(startLocator);
  }

  @Test
  public void canConnectOverHttpWithUnsignedSSLCertificateIfSkipSslValidationIsSet()
      throws Exception {
    GfshScript connect =
        GfshScript.of("connect --use-http --skip-ssl-validation --security-properties-file="
            + sslPropertiesFile.getAbsolutePath());
    gfshRule.execute(connect);
  }

  @Test
  public void cannotConnectOverHttpWithUnsignedSSLCertificateIfSkipSslValidationIsNotSet()
      throws Exception {
    GfshScript connect = GfshScript
        .of("connect --use-http --security-properties-file=" + sslPropertiesFile.getAbsolutePath())
        .expectFailure();
    gfshRule.execute(connect);
  }

  @Test
  public void cannotConnectOverHttpWithoutSSL() throws Exception {
    GfshScript connect = GfshScript.of("connect --use-http").expectFailure();
    gfshRule.execute(connect);
  }

  @Test
  public void canConnectOverJmxWithSSL() throws Exception {
    GfshScript connect = GfshScript.of("connect --use-http=false --security-properties-file="
        + sslPropertiesFile.getAbsolutePath());
    gfshRule.execute(connect);
  }
}
