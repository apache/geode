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

import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.base.Stopwatch;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.SerializableRunnable;

/**
 * JUnit Rule that disconnects DistributedSystem in all VMs.
 *
 * <p>
 * DistributedDisconnectRule can be used in DistributedTests to disconnect all VMs before or after
 * each test:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedDisconnectRule distributedDisconnectRule = new DistributedDisconnectRule();
 *
 * {@literal @}Test
 * public void createCacheInEveryDUnitVM() throws Exception {
 *   cache = (InternalCache) new CacheFactory().create();
 *   assertThat(cache.isClosed()).isFalse();
 *   assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
 *
 *   for (VM vm: Host.getHost(0).getAllVMs()) {
 *     vm.invoke(() -> {
 *       cache = (InternalCache) new CacheFactory().create();
 *       assertThat(cache.isClosed()).isFalse();
 *       assertThat(cache.getInternalDistributedSystem().isConnected()).isTrue();
 *     });
 *   }
 * }
 * </pre>
 */
public class DistributedDisconnectRule extends DistributedExternalResource {

  private final boolean disconnectBefore;
  private final boolean disconnectAfter;

  public static Builder builder() {
    return new Builder();
  }

  public DistributedDisconnectRule(final Builder builder) {
    this(new RemoteInvoker(), builder);
  }

  public DistributedDisconnectRule(final RemoteInvoker invoker, final Builder builder) {
    super(invoker);
    this.disconnectBefore = builder.disconnectBefore;
    this.disconnectAfter = builder.disconnectAfter;
  }

  @Override
  protected void before() throws Throwable {
    if (this.disconnectBefore) {
      invoker().invokeInEveryVMAndController(serializableRunnable());
    }
  }

  @Override
  protected void after() {
    if (this.disconnectAfter) {
      invoker().invokeInEveryVMAndController(serializableRunnable());
    }
  }

  private static SerializableRunnable serializableRunnable() {
    return new SerializableRunnable() {
      @Override
      public void run() {
        disconnect();
      }
    };
  }

  public static void disconnect() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();

    while (system != null && stopwatch.elapsed(MINUTES) < 10) {
      system = InternalDistributedSystem.getConnectedInstance();
      try {
        system.disconnect();
      } catch (Exception ignore) {
        // ignored
      }
    }

    AdminDistributedSystemImpl adminSystem = AdminDistributedSystemImpl.getConnectedInstance();
    if (adminSystem != null) {
      adminSystem.disconnect();
    }
  }

  /**
   * Builds an instance of DistributedDisconnectRule
   */
  public static class Builder {
    private boolean disconnectBefore;
    private boolean disconnectAfter;

    public Builder() {
      // nothing
    }

    public Builder disconnectBefore(final boolean disconnectBefore) {
      this.disconnectBefore = disconnectBefore;
      return this;
    }

    public Builder disconnectAfter(final boolean disconnectAfter) {
      this.disconnectAfter = disconnectAfter;
      return this;
    }

    public DistributedDisconnectRule build() {
      return new DistributedDisconnectRule(this);
    }
  }
}
