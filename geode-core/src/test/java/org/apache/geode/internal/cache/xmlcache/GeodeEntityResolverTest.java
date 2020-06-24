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

package org.apache.geode.internal.cache.xmlcache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

public class GeodeEntityResolverTest {

  private static final GeodeEntityResolver geodeEntityResolver = new GeodeEntityResolver();

  @BeforeClass
  public static void setup() {
    geodeEntityResolver.init(new ServiceLoaderModuleService(LogService.getLogger()));
  }

  @Test
  public void resolvesWithHttpUrl() throws IOException, SAXException {
    InputSource inputSource =
        geodeEntityResolver.resolveEntity(null, null, null,
            "http://geode.apache.org/schema/cache/cache-1.0.xsd");
    assertThat(inputSource).isNotNull();
  }

  @Test
  public void resolvesWithHttpsUrl() throws IOException, SAXException {
    InputSource inputSource =
        geodeEntityResolver.resolveEntity(null, null, null,
            "https://geode.apache.org/schema/cache/cache-1.0.xsd");
    assertThat(inputSource).isNotNull();
  }

  @Test
  public void doesNotResolveUnknownUrl() throws IOException, SAXException {
    InputSource inputSource =
        geodeEntityResolver.resolveEntity(null, null, null,
            "http://geode.apache.org/schema/should/not/exist.xsd");
    assertThat(inputSource).isNull();
  }

}
