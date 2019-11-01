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

import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class LocatorLauncherStartupRule extends SerializableExternalResource {
  private LocatorLauncher launcher;
  private final TemporaryFolder temp = new TemporaryFolder();
  private final Properties properties = new Properties();
  private boolean autoStart;

  public LocatorLauncherStartupRule withAutoStart() {
    autoStart = true;
    return this;
  }

  public LocatorLauncherStartupRule withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  public LocatorLauncherStartupRule withProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  @Override
  public void before() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder()
        .setPort(0)
        .set(properties)
        .setMemberName("locator-0")
        .set(ConfigurationProperties.LOG_LEVEL, "config");
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
}
