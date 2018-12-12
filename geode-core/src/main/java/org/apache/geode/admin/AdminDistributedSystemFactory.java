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
package org.apache.geode.admin;

import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.admin.internal.DistributedSystemConfigImpl;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LocalLogWriter;

/**
 * Factory for creating GemFire administration entities.
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public class AdminDistributedSystemFactory {

  /**
   * Sets the address this VM should bind to when connecting to the distributed system. This
   * involves a system property, so using this option will limit all connections to distributed
   * systems to this one network interface.
   * <p>
   * Using a null or empty bindAddress will clear the usage of this option and connections to
   * distributed systems will return to using all available network interfaces.
   * <p>
   * This method always throws UnsupportedOperationException because it is now deprecated and is
   * unsafe to use. Please use {@link DistributedSystemConfig#setBindAddress} instead.
   *
   * @param bindAddress machine name or IP address to bind to
   * @throws UnsupportedOperationException because of deprecation
   * @deprecated Use {@link DistributedSystemConfig#setBindAddress} instead.
   */
  @Deprecated
  public static void bindToAddress(String bindAddress) {
    throw new UnsupportedOperationException(
        "Please use DistributedSystemConfig.setBindAddress instead.");
  }

  /**
   * Defines a "default" distributed system configuration based on VM system properties and the
   * content of <code>gemfire.properties</code>. The
   * {@linkplain DistributedSystemConfig#DEFAULT_REMOTE_COMMAND} default remote command is used.
   *
   * @see DistributedSystem#connect
   */
  public static DistributedSystemConfig defineDistributedSystem() {
    DistributionConfig dc = new DistributionConfigImpl(new Properties());

    String remoteCommand = DistributedSystemConfig.DEFAULT_REMOTE_COMMAND;
    return new DistributedSystemConfigImpl(dc, remoteCommand);
  }

  /**
   * Call this method with a value of <code>true</code> to dedicate the VM to GemFire administration
   * only. Default is <code>false</code>.
   * <p>
   * This method <em>must</em> be called before calling {@link AdminDistributedSystem#connect}. It
   * <em>must</em> also be called before {@link DistributedSystem#connect} is when creating a
   * colocated distributed system.
   * <p>
   * Once it has been enabled be careful to only use GemFire APIs from the
   * <code>org.apache.geode.admin</code> package. In particular do not create a
   * {@link org.apache.geode.cache.Cache} or a normal {@link DistributedSystem}.
   *
   * @param adminOnly <code>true</code> if this VM should be limited to administration APIs;
   *        <code>false</code> if this VM should allow all GemFire APIs.
   * @throws IllegalStateException if a {@link DistributedSystem} or {@link AdminDistributedSystem}
   *         connection already exists.
   *
   * @since GemFire 5.7
   */
  public static void setEnableAdministrationOnly(boolean adminOnly) {
    InternalDistributedSystem.setEnableAdministrationOnly(adminOnly);
  }

  /**
   * Defines a distributed system configuration for administering the distributed system to which
   * this VM is currently connected. The <code>DistributedSystem</code> is used to configure the
   * discovery mechanism (multicast or locators), bind address, SSL attributes, as well as the
   * logger of the <code>DistributedSystemConfig</code>. Note that the distributed system will not
   * be able to be administered until the {@link AdminDistributedSystem#connect connect} method is
   * invoked.
   *
   * @param system A connection to the distributed system
   * @param remoteCommand The shell command that is used to launch processes that run on remote
   *        machines. If <code>null</code>, then the
   *        {@linkplain DistributedSystemConfig#DEFAULT_REMOTE_COMMAND default} will be used.
   *
   * @since GemFire 4.0
   */
  public static DistributedSystemConfig defineDistributedSystem(DistributedSystem system,
      String remoteCommand) throws AdminException {

    InternalDistributedSystem internal = (InternalDistributedSystem) system;
    if (remoteCommand == null) {
      remoteCommand = DistributedSystemConfig.DEFAULT_REMOTE_COMMAND;
    }

    DistributedSystemConfigImpl impl =
        new DistributedSystemConfigImpl(internal.getConfig(), remoteCommand);
    return impl;
  }

  /**
   * Returns the distributed system for administrative monitoring and managing. You must then call
   * {@link AdminDistributedSystem#connect} before interacting with the actual system.
   *
   * @param config configuration definition of the system to administer
   * @return administrative interface for a distributed system
   */
  public static AdminDistributedSystem getDistributedSystem(DistributedSystemConfig config) {
    return new AdminDistributedSystemImpl((DistributedSystemConfigImpl) config);
  }

  /**
   * Returns a default GemFire LogWriterI18n for logging. This LogWriterI18n will log to standard
   * out.
   *
   * @return a GemFire LogWriterI18n for logging
   */
  public static LogWriter getLogWriter() {
    return new LocalLogWriter(DistributionConfig.DEFAULT_LOG_LEVEL);
  }

}
