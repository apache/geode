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
package org.apache.geode.test.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ResourceUtilsIntegrationTest {

  private static final String RESOURCE_SUFFIX = "_resource.txt";
  private static final String RESOURCE_CONTENT = "simple string line";

  private String resourceName;
  private URL resource;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    resourceName = ResourceUtilsTest.class.getSimpleName() + RESOURCE_SUFFIX;
    resource = ResourceUtils.getResource(ResourceUtilsTest.class, resourceName);
  }

  @Test
  public void createFileFromResource() throws Exception {
    File file =
        ResourceUtils.createFileFromResource(resource, temporaryFolder.getRoot(), resourceName);
    assertThat(file).isNotNull().exists();

    List<String> content = FileUtils.readLines(file, Charset.defaultCharset());
    assertThat(content).contains(RESOURCE_CONTENT);
  }
}
