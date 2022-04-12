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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.configuration.AutoSerializer;
import org.apache.geode.management.configuration.ClassName;
import org.apache.geode.management.configuration.Pdx;
import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class PdxIntegrationTest {
  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
  }

  @Test
  public void canCreatePdxConfigurationWithCustomSerializer() throws Exception {
    Pdx pdx = new Pdx();
    pdx.setReadSerialized(true);
    pdx.setIgnoreUnreadFields(true);
    pdx.setDiskStoreName("diskStoreName");
    pdx.setPdxSerializer(new ClassName("className"));

    context.perform(post("/v1/configurations/pdx")
        .with(httpBasic("clusterManage", "clusterManage"))
        .content(mapper.writeValueAsString(pdx)))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.memberStatuses").doesNotExist())
        .andExpect(
            jsonPath("$.statusMessage",
                containsString("Successfully updated configuration for cluster.")))
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(
            jsonPath("$.links.self", is("http://localhost/v1/configurations/pdx")));

    // Clean Up
    context
        .perform(delete("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk());
  }

  @Test
  public void canCreatePdxConfigurationWithAutoSerializer() throws Exception {
    Pdx pdx = new Pdx();
    pdx.setReadSerialized(true);
    pdx.setIgnoreUnreadFields(true);
    pdx.setDiskStoreName("diskStoreName");
    pdx.setAutoSerializer(new AutoSerializer("pat1", "pat2"));

    context.perform(post("/v1/configurations/pdx")
        .with(httpBasic("clusterManage", "clusterManage"))
        .content(mapper.writeValueAsString(pdx)))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.memberStatuses").doesNotExist())
        .andExpect(
            jsonPath("$.statusMessage",
                containsString("Successfully updated configuration for cluster.")))
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(
            jsonPath("$.links.self", is("http://localhost/v1/configurations/pdx")));

    // Clean Up
    context
        .perform(delete("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk());
  }

  @Test
  public void canUpdatePdxConfiguration() throws Exception {
    Pdx pdx = new Pdx();
    pdx.setReadSerialized(true);
    pdx.setIgnoreUnreadFields(true);
    pdx.setAutoSerializer(new AutoSerializer("org.company.Class1#identity=myValue"));

    // Create
    context.perform(post("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage"))
        .content(mapper.writeValueAsString(pdx))).andExpect(status().isCreated());
    context.perform(get("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result.groups[0].configuration.readSerialized", is(true)))
        .andExpect(jsonPath("$.result.groups[0].configuration.ignoreUnreadFields", is(true)))
        .andExpect(jsonPath("$.result.groups[0].configuration.autoSerializer.patterns[0]",
            is("org.company.Class1#identity=myValue")));

    // Update
    pdx.setReadSerialized(false);
    pdx.setIgnoreUnreadFields(false);
    pdx.setAutoSerializer(
        new AutoSerializer("org.company.MyClass2", "org.company.Class1#identity=myValue"));
    context.perform(put("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage"))
        .content(mapper.writeValueAsString(pdx))).andExpect(status().isCreated());
    context.perform(get("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result.groups[0].configuration.readSerialized", is(false)))
        .andExpect(jsonPath("$.result.groups[0].configuration.ignoreUnreadFields", is(false)))
        .andExpect(jsonPath("$.result.groups[0].configuration.autoSerializer.patterns[0]",
            is("org.company.MyClass2")))
        .andExpect(jsonPath("$.result.groups[0].configuration.autoSerializer.patterns[1]",
            is("org.company.Class1#identity=myValue")));

    // Clean Up
    context.perform(
        delete("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk());
    context.perform(get("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isNotFound());
  }

  @Test
  public void canDeletePdxConfiguration() throws Exception {
    Pdx pdx = new Pdx();
    pdx.setReadSerialized(true);
    pdx.setIgnoreUnreadFields(true);

    // Create
    context.perform(get("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isNotFound());
    context.perform(post("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage"))
        .content(mapper.writeValueAsString(pdx))).andExpect(status().isCreated());
    context.perform(get("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk());

    // Delete
    context.perform(
        delete("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isOk());
    context.perform(get("/v1/configurations/pdx").with(httpBasic("clusterManage", "clusterManage")))
        .andExpect(status().isNotFound());
  }

  @Test
  public void invalidClassName() throws Exception {
    String json = "{\"readSerialized\":true,\"pdxSerializer\":{\"className\":\"class name\"}}";

    context.perform(post("/v1/configurations/pdx")
        .with(httpBasic("clusterManage", "clusterManage"))
        .content(json))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.statusCode", is("ILLEGAL_ARGUMENT")))
        .andExpect(jsonPath("$.statusMessage", containsString("Invalid className")));
  }

  @Test
  public void pdxDoesNotAllowAutoSerializerWithNoPatterns() throws Exception {
    String json = "{\"readSerialized\":true,\"autoSerializer\":{\"portable\":true}}";

    context.perform(post("/v1/configurations/pdx")
        .with(httpBasic("clusterManage", "clusterManage"))
        .content(json))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.statusCode", is("ILLEGAL_ARGUMENT")))
        .andExpect(jsonPath("$.statusMessage",
            containsString("The autoSerializer must have at least one pattern.")));
  }

  @Test
  public void pdxDoesNotAllowBothAutoAndPdxSerializer() throws Exception {
    String json =
        "{\"readSerialized\":true,\"autoSerializer\":{\"portable\":true,\"patterns\":[\"pat1\"]},\"pdxSerializer\":{\"className\":\"className\"}}";

    context.perform(post("/v1/configurations/pdx")
        .with(httpBasic("clusterManage", "clusterManage"))
        .content(json))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.statusCode", is("ILLEGAL_ARGUMENT")))
        .andExpect(jsonPath("$.statusMessage", containsString("can not be set")))
        .andExpect(jsonPath("$.statusMessage", containsString("is already set")));
  }
}
