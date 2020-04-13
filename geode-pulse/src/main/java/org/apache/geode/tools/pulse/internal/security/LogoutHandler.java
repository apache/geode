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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;

import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Handler is used to close jmx connection maintained at user-level
 */
public class LogoutHandler extends SimpleUrlLogoutSuccessHandler implements
    ApplicationContextAware {
  private static final Logger logger = LogManager.getLogger();
  private ApplicationContext applicationContext;

  public LogoutHandler(String logoutTargetURL) {
    setDefaultTargetUrl(logoutTargetURL);
  }

  @Override
  public void onLogoutSuccess(HttpServletRequest request, HttpServletResponse response,
      Authentication authentication) throws IOException, ServletException {

    if (authentication != null) {
      Repository repository = applicationContext.getBean("repository", Repository.class);
      repository.logoutUser(authentication.getName());
      logger.info("#LogoutHandler: GemFireAuthentication JMX Connection Closed.");
    }

    super.onLogoutSuccess(request, response, authentication);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }
}
