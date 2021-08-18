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
package dale.tests;

import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class DaleResourceTest {
  private static final String RESOURCE_NAME = "expected-pom.xml";

  @Test
  public void resourceURL_toURI_PathsGet_toUri() throws URISyntaxException {
    URL resourceURL = getResource(DaleResourceTest.class, "/" + RESOURCE_NAME);
    assertThat(resourceURL).isNotNull();

    URI resourceURI = resourceURL.toURI();
    assertThat(resourceURI).isNotNull();

    Path path = Paths.get(resourceURI);
    assertThat(path).isNotNull();

    URI pathUri = path.toUri();
    assertThat(pathUri).isEqualTo(resourceURI);
  }
}
