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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = MockLocatorContextLoader.class)
@WebAppConfiguration
public class GenericCliControllerTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;
  private ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);

  }

  @Test
  public void test() throws Exception {
    // command: create region -g group -r -param {}
    context.perform(get("/v2/cli?command=list region"));
  }

}
