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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.function.UnaryOperator;

import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

public class ServerLauncherStartupRule extends SerializableExternalResource {
  private ServerLauncher launcher;
  private final TemporaryFolder temp = new SerializableTemporaryFolder();
  private final Properties properties = new Properties();
  private boolean autoStart;
  private UnaryOperator<ServerLauncher.Builder> builderOperator;

  public ServerLauncherStartupRule withAutoStart() {
    autoStart = true;
    return this;
  }

  public ServerLauncherStartupRule withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  public ServerLauncherStartupRule withProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public ServerLauncherStartupRule withBuilder(
      UnaryOperator<ServerLauncher.Builder> builderOperator) {
    this.builderOperator = builderOperator;
    return this;
  }

  @Override
  public void before() {
    ServerLauncher.Builder builder = new ServerLauncher.Builder()
        .setServerPort(0)
        .set(properties)
        .setMemberName("server-0")
        .set(ConfigurationProperties.LOG_LEVEL, "config");
    if (builderOperator != null) {
      builder = builderOperator.apply(builder);
    }
    try {
      temp.create();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    builder.setWorkingDirectory(temp.getRoot().getAbsolutePath());
    launcher = builder.build();

    if (autoStart) {
      start();
    }
  }

  public void start() {
    launcher.start();
  }

  @Override
  public void after() {
    if (launcher != null) {
      launcher.stop();
    }
    temp.delete();
  }

  public Cache getCache() {
    return launcher.getCache();
  }
}
