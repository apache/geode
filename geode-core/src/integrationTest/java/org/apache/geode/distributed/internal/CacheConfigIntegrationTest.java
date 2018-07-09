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

package org.apache.geode.distributed.internal;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class CacheConfigIntegrationTest {
  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File xmlFile;

  @Test
  public void testXmlCreatedByCacheConfigCanBeUsedToStartupServer() throws Exception {
    xmlFile = temporaryFolder.newFile("cache.xml");
    CacheConfig cacheConfig = new CacheConfig();
    cacheConfig.setVersion("1.0");
    JAXBService service = new JAXBService(CacheConfig.class);
    String xml = service.marshall(cacheConfig);
    FileUtils.writeStringToFile(xmlFile, xml, "UTF-8");

    server.withProperty("cache-xml-file", xmlFile.getAbsolutePath());
    server.startServer();
  }

}
