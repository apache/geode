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

package org.apache.geode.tools.pulse.internal.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Logs a warning at startup when Pulse runs with the default authentication profile, so that
 * operators can see that Pulse is not authenticating users with the cluster.
 */
@Component
@Profile("pulse.authentication.default")
public class DefaultAuthenticationProfileWarning implements InitializingBean {
  private static final Logger logger = LogManager.getLogger();

  static final String MESSAGE =
      "Pulse is using the pulse.authentication.default profile, which accepts only the built-in "
          + "admin login and does not authenticate users with the cluster. To authenticate Pulse "
          + "users with the cluster, configure a security manager, or when hosting Pulse on a web "
          + "application server, start it with "
          + "-Dspring.profiles.active=pulse.authentication.gemfire.";

  @Override
  public void afterPropertiesSet() {
    logger.warn(MESSAGE);
  }
}
