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

package org.apache.geode.management.internal.rest;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.examples.SimpleSecurityManager;


@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = SecuredLocatorWithAuthTokenEnabledContextLoader.class)
@WebAppConfiguration
public class RequestWithAuthTokenTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);

  }

  @Test
  public void pingWithValidToken() throws Exception {
    context.perform(get("/v1/ping")
        .header("Authorization", "Bearer " + SimpleSecurityManager.VALID_TOKEN))
        .andExpect(status().isOk())
        .andExpect(content().string("pong"));
  }

  @Test
  public void pingWithInvalidToken() throws Exception {
    context.perform(get("/v1/ping")
        .header("Authorization", "Bearer invalidToken"))
        .andExpect(status().isUnauthorized());
  }

}
