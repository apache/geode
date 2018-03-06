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
package org.apache.geode.test.dunit.rules;

import static java.lang.System.*;

import java.util.Properties;

import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

/**
 * Distributed version of RestoreSystemProperties which affects all DUnit JVMs including the Locator
 * JVM.
 */
public class DistributedRestoreSystemProperties extends RestoreSystemProperties
    implements SerializableTestRule {

  private static volatile Properties originalProperties;

  private final RemoteInvoker invoker;

  public DistributedRestoreSystemProperties() {
    this(new RemoteInvoker());
  }

  public DistributedRestoreSystemProperties(final RemoteInvoker invoker) {
    super();
    this.invoker = invoker;
  }

  @Override
  public void before() throws Throwable {
    super.before();
    this.invoker.invokeInEveryVMAndController(new SerializableRunnable() {
      @Override
      public void run() {
        if (originalProperties == null) {
          originalProperties = getProperties();
          setProperties(copyOf(originalProperties));
        }
      }
    });
  }

  private Properties copyOf(Properties source) {
    Properties copy = new Properties();
    copy.putAll(source);
    return copy;
  }

  @Override
  public void after() {
    super.after();
    this.invoker.invokeInEveryVMAndController(new SerializableRunnable() {
      @Override
      public void run() {
        if (originalProperties != null) {
          setProperties(originalProperties);
          originalProperties = null;
        }
      }
    });
  }
}
