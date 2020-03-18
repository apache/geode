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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.stereotype.Component;

import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Closes the user's JMX connection and releases the user's resources in the Repository.
 */
@Component
public class RepositoryLogoutHandler implements LogoutHandler {
  private static final Logger logger = LogManager.getLogger();
  private final Repository repository;

  RepositoryLogoutHandler(Repository repository) {
    this.repository = repository;
  }

  @Override
  public void logout(HttpServletRequest request, HttpServletResponse response,
      Authentication authentication) {
    if (authentication != null) {
      repository.logoutUser(authentication.getName());
      logger.info("#RepositoryLogoutHandler: GemFireAuthentication JMX Connection Closed.");
    }
  }
}
