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
package org.apache.geode.admin.jmx;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.jmx.internal.AgentConfigImpl;
import org.apache.geode.admin.jmx.internal.AgentImpl;
import org.apache.geode.services.module.ModuleService;

/**
 * A factory class that creates JMX administration entities.
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public class AgentFactory {

  /**
   * Defines a "default" GemFire JMX administration agent configuration.
   */
  public static AgentConfig defineAgent() {
    return new AgentConfigImpl();
  }

  /**
   * Creates an unstarted GemFire JMX administration agent with the given configuration.
   *
   * @see Agent#start
   */
  public static Agent getAgent(AgentConfig config, ModuleService moduleService)
      throws AdminException {
    return new AgentImpl((AgentConfigImpl) config, moduleService);
  }

}
