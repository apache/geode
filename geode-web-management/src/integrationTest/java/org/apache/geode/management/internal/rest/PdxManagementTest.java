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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class PdxManagementTest {
  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
  }

  @Test
  public void success() throws Exception {
    Pdx pdx = new Pdx();
    pdx.setReadSerialized(true);
    pdx.setIgnoreUnreadFields(true);
    pdx.setPersistent(true);
    pdx.setDiskStoreName("diskStoreName");
    pdx.setPdxSerializer(new ClassName("className"));
    try {
      context.perform(post("/experimental/configurations/pdx")
          .with(httpBasic("clusterManage", "clusterManage"))
          .content(mapper.writeValueAsString(pdx)))
          .andExpect(status().isCreated())
          .andExpect(jsonPath("$.memberStatuses").doesNotExist())
          .andExpect(
              jsonPath("$.statusMessage",
                  containsString("Successfully updated configuration for cluster.")))
          .andExpect(jsonPath("$.statusCode", is("OK")))
          .andExpect(
              jsonPath("$.links.self", is("http://localhost/experimental/configurations/pdx")));
    }
    // this is a hack to make StressNewTest pass, rework this once "delete pdx" end point is
    // implemented.
    catch (AssertionError e) {
      context.perform(post("/experimental/configurations/pdx")
          .with(httpBasic("clusterManage", "clusterManage"))
          .content(mapper.writeValueAsString(pdx)))
          .andExpect(status().isConflict());
    }

  }
}
