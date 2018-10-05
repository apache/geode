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

import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class ResourceUtilsTest {

  private static final String RESOURCE_SUFFIX = "_resource.txt";
  private static final String RESOURCE_CONTENT = "simple string line";

  private String className;
  private String resourceName;

  @Before
  public void setUp() throws Exception {
    className = getClass().getName();
    resourceName = getClass().getSimpleName() + RESOURCE_SUFFIX;
  }

  @Test
  public void getCallerClassName() {
    assertThat(ResourceUtils.getCallerClassName(1)).isEqualTo(className);
  }

  @Test
  public void getCallerClass() throws Exception {
    assertThat(ResourceUtils.getCallerClass(1)).isEqualTo(getClass());
  }

  @Test
  public void getResource() throws Exception {
    URL resource = ResourceUtils.getResource(resourceName);

    assertThat(resource).isNotNull();
    assertThat(IOUtils.toString(resource, Charset.defaultCharset())).contains(RESOURCE_CONTENT);
  }

  @Test
  public void getResourceWithClass() throws Exception {
    URL resource = ResourceUtils.getResource(getClass(), resourceName);

    assertThat(resource).isNotNull();
    assertThat(IOUtils.toString(resource, Charset.defaultCharset())).contains(RESOURCE_CONTENT);
  }
}
