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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.security.ResourceConstants;

public class UserInputProperty {
  @Immutable
  public static final UserInputProperty USERNAME =
      new UserInputProperty(ResourceConstants.USER_NAME, "user", false);
  @Immutable
  public static final UserInputProperty PASSWORD =
      new UserInputProperty(ResourceConstants.PASSWORD, "password", "", true);
  @Immutable
  public static final UserInputProperty KEYSTORE =
      new UserInputProperty(SSL_KEYSTORE, "key-store", "", false);
  @Immutable
  public static final UserInputProperty KEYSTORE_PASSWORD =
      new UserInputProperty(SSL_KEYSTORE_PASSWORD, "key-store-password", "", true);
  @Immutable
  public static final UserInputProperty KEYSTORE_TYPE =
      new UserInputProperty(SSL_KEYSTORE_TYPE, "key-store-type", "JKS", false);
  @Immutable
  public static final UserInputProperty TRUSTSTORE =
      new UserInputProperty(SSL_TRUSTSTORE, "trust-store", "", false);
  @Immutable
  public static final UserInputProperty TRUSTSTORE_PASSWORD =
      new UserInputProperty(SSL_TRUSTSTORE_PASSWORD, "trust-store-password", "", true);
  @Immutable
  public static final UserInputProperty TRUSTSTORE_TYPE =
      new UserInputProperty(SSL_TRUSTSTORE_TYPE, "trust-store-type", "JKS", false);
  @Immutable
  public static final UserInputProperty CIPHERS = new UserInputProperty(SSL_CIPHERS, "ssl-ciphers",
      DistributionConfig.DEFAULT_SSL_CIPHERS, false);
  @Immutable
  public static final UserInputProperty PROTOCOL =
      new UserInputProperty(SSL_PROTOCOLS, "ssl-protocols",
          DistributionConfig.DEFAULT_SSL_PROTOCOLS, false);
  @Immutable
  public static final UserInputProperty COMPONENT = new UserInputProperty(SSL_ENABLED_COMPONENTS,
      "ssl-enabled-components", SecurableCommunicationChannel.ALL.getConstant(), false);

  private final String key;
  private final String prompt;
  private final boolean isMasked;
  private final String defaultValue;

  // use this if this property does not allow an empty string and has no default value
  UserInputProperty(String key, String prompt, boolean isMasked) {
    this(key, prompt, null, isMasked);
  }

  // if you allow an empty string for this property, supply a default value of ""
  UserInputProperty(String key, String prompt, String defaultValue, boolean isMasked) {
    this.key = key;
    this.prompt = prompt;
    this.defaultValue = defaultValue;
    this.isMasked = isMasked;
  }

  public String promptForAcceptableValue(Gfsh gfsh) {
    if (gfsh.isQuietMode() || gfsh.isHeadlessMode()) {
      return defaultValue == null ? "" : defaultValue;
    }

    String value = promptForUserInput(gfsh);

    if (value.length() > 0) {
      return value;
    }

    // when user input an empty string and a default value is supplied, return the default value
    if (value.length() == 0 && defaultValue != null) {
      return defaultValue;
    }

    // otherwise prompt till we get a non-empty value, only when this property has no default value
    while (value.length() == 0) {
      value = promptForUserInput(gfsh);
    }
    return value;
  }

  private String promptForUserInput(Gfsh gfsh) {
    String promptText = (StringUtils.isBlank(defaultValue)) ? prompt + ": "
        : prompt + "(default: " + defaultValue + ")" + ": ";
    String value;

    if (isMasked) {
      value = gfsh.readPassword(promptText);
    } else {
      value = gfsh.readText(promptText);
    }
    // when gfsh is mocked or quiet mode, the above would return null
    if (value == null) {
      value = "";
    }
    return value;
  }

  public String getKey() {
    return key;
  }
}
