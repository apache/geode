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
package org.apache.geode.management.internal.cli.json;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;

/**
 * Integration tests for {@link TypedJson}.
 * <p>
 *
 * TODO: add actual assertions
 */
public class TypedJsonPdxIntegrationTest {

  private static final String RESULT = "result";

  private DistributedSystem system;
  private PdxInstanceFactory pdxInstanceFactory;

  @Before
  public void setUp() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");

    system = DistributedSystem.connect(config);
    Cache cache = new CacheFactory().create();
    pdxInstanceFactory =
        PdxInstanceFactoryImpl.newCreator("Portfolio", false, ((InternalCache) cache));
  }

  @After
  public void tearDown() throws Exception {
    system.disconnect();
  }

  @Test
  public void supportsPdxInstance() throws Exception {
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "IBM");
    pdxInstanceFactory.writeObject("object", new SerializableObject(2));
    PdxInstance pdxInstance = pdxInstanceFactory.create();

    TypedJson typedJson = new TypedJson(RESULT, pdxInstance);

    checkResult(typedJson);
  }

  @Test
  public void supportsObjectContainingPdxInstance() throws Exception {
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "IBM");
    PdxContainer pdxContainer = new PdxContainer(pdxInstanceFactory.create(), 1);

    TypedJson typedJson = new TypedJson(RESULT, pdxContainer);

    checkResult(typedJson);
  }

  private void checkResult(TypedJson typedJson) throws GfJsonException {
    GfJsonObject gfJsonObject = new GfJsonObject(typedJson.toString());
    System.out.println(gfJsonObject);
    assertThat(gfJsonObject.get(RESULT)).isNotNull();
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
