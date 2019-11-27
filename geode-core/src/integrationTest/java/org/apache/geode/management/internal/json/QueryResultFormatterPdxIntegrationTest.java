/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.json;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Integration tests for {@link QueryResultFormatter}.
 */
public class QueryResultFormatterPdxIntegrationTest {
  private static final String RESULT = "result";
  private PdxInstanceFactory pdxInstanceFactory;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() throws Exception {
    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false, (server.getCache()));
  }

  private void checkResult(QueryResultFormatter queryResultFormatter, String expectedJsonString)
      throws IOException {
    String jsonString = queryResultFormatter.toString();
    assertThat(jsonString).isEqualTo(expectedJsonString);

    JsonNode jsonObject = new ObjectMapper().readTree(jsonString);
    assertThat(jsonObject.get(RESULT)).isNotNull();
  }

  @Test
  public void supportsPdxInstance() throws Exception {
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "IBM");
    pdxInstanceFactory.writeObject("object", new SerializableObject(2));
    PdxInstance pdxInstance = pdxInstanceFactory.create();

    QueryResultFormatter queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, pdxInstance);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"org.apache.geode.pdx.PdxInstance\",{\"ID\":[\"java.lang.Integer\",111],\"status\":[\"java.lang.String\",\"active\"],\"secId\":[\"java.lang.String\",\"IBM\"],\"object\":[\"org.apache.geode.management.internal.cli.json.QueryResultFormatterPdxIntegrationTest.SerializableObject\",{}]}]]}");
  }

  @Test
  public void supportsObjectContainingPdxInstance() throws Exception {
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "IBM");
    PdxContainer pdxContainer = new PdxContainer(pdxInstanceFactory.create(), 1);

    QueryResultFormatter queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, pdxContainer);
    checkResult(queryResultFormatter,
        "{\"result\":[[\"org.apache.geode.management.internal.cli.json.QueryResultFormatterPdxIntegrationTest.PdxContainer\",{}]]}");
  }

  private static class SerializableObject implements Serializable {
    private final int id;

    SerializableObject(final int id) {
      this.id = id;
    }
  }

  private static class PdxContainer {
    private final PdxInstance pdxInstance;
    private final int count;

    PdxContainer(final PdxInstance pdxInstance, final int count) {
      this.pdxInstance = pdxInstance;
      this.count = count;
    }
  }
}
