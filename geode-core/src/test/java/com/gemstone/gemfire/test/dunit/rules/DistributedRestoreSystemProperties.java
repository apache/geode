/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.test.dunit.rules;

import static java.lang.System.getProperties;
import static java.lang.System.setProperties;

import java.util.Properties;

import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.junit.rules.SerializableTestRule;

/**
 * Distributed version of RestoreSystemProperties which affects all DUnit 
 * JVMs including the Locator JVM.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public class DistributedRestoreSystemProperties extends RestoreSystemProperties implements SerializableTestRule {
  
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
  protected void before() throws Throwable {
    super.before();
    this.invoker.remoteInvokeInEveryVMAndLocator(new SerializableRunnable() {
      @Override
      public void run() { 
        originalProperties = getProperties();
        setProperties(new Properties(originalProperties));
      }
    });
  }

  @Override
  protected void after() {
    super.after();
    this.invoker.remoteInvokeInEveryVMAndLocator(new SerializableRunnable() {
      @Override
      public void run() { 
        setProperties(originalProperties);
        originalProperties = null;
      }
    });
  }
}
