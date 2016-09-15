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
package org.apache.geode.admin;
import java.util.Properties;

/**
 * Describes the configuration of a {@link DistributionLocator}
 * managed by the GemFire administration APIs.  
 *
 * <P>
 *
 * A <code>DistributionLocatorConfig</code> can be modified using a
 * number of mutator methods until the
 * <code>DistributionLocator</code> configured by this object is
 * {@linkplain ManagedEntity#start started}.  After that,
 * attempts to modify most attributes in the
 * <code>DistributionLocatorConfig</code> will result in an {@link
 * IllegalStateException} being thrown.  If you wish to use the same
 * <code>DistributionLocatorConfig</code> to configure another
 * <code>DistributionLocator</code>s, a copy of the
 * <code>DistributionLocatorConfig</code> object can be made by
 * invoking the {@link Object#clone} method.
 *
 * @see AdminDistributedSystem#addDistributionLocator
 * @see org.apache.geode.distributed.Locator
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface DistributionLocatorConfig
  extends ManagedEntityConfig {

  /**
   * Returns the port on which ths distribution locator listens for
   * members to connect.  There is no default locator port, so a
   * non-default port must be specified.
   */
  public int getPort();

  /**
   * Sets the port on which the distribution locator listens for
   * members to connect.
   */
  public void setPort(int port);

  /**
   * Returns the address to which the distribution locator's port is
   * (or will be) bound.  By default, the bind address is
   * <code>null</code> meaning that the port will be bound to all
   * network addresses on the host.
   */
  public String getBindAddress();

  /**
   * Sets the address to which the distribution locator's port is
   * (or will be) bound.
   */
  public void setBindAddress(String bindAddress);

  /**
   * Sets the properties used to configure the locator's
   * DistributedSystem.
   * @since GemFire 5.0
   */
  public void setDistributedSystemProperties(Properties props);
  
  /**
   * Retrieves the properties used to configure the locator's
   * DistributedSystem.
   * @since GemFire 5.0
   */
  public Properties getDistributedSystemProperties();


}
