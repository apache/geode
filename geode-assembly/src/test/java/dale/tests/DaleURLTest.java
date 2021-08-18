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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class DaleURLTest {
  private static final String RESOURCE_NAME = "expected-pom.xml";

  @Test
  public void newStyle() throws MalformedURLException, URISyntaxException {
    System.out.println("DHE: new style");

    URL resourceURL = getClass().getResource("/" + RESOURCE_NAME);
    assertThat(resourceURL).isNotNull();
    System.out.printf("DHE: resourceURL: %s%n", resourceURL);
    System.out.printf("DHE: resourceURL.getPath(): %s%n", resourceURL.getPath());
    System.out.printf("DHE: resourceURL.toURI(): %s%n", resourceURL.toURI());

    String resourcePathString = resourceURL.getPath();
    System.out.printf("DHE: resourcePathString: %s%n", resourcePathString);

    Path resourcePath = Paths.get(resourcePathString);
    System.out.printf("DHE: resourcePath: %s%n", resourcePath);

    URI resourcePathURI = resourcePath.toUri();
    System.out.printf("DHE: resourcePathURI: %s%n", resourcePathURI);

    URL resourcePathURL = resourcePathURI.toURL();
    System.out.printf("DHE: resourcePathURL: %s%n", resourcePathURL);
  }

  @Test
  public void styleFromContainerInstall() throws MalformedURLException {
    System.out.println("DHE: style from ContainerInstall");

    URL resourceURL = getResource(getClass(), "/" + RESOURCE_NAME);
    System.out.printf("DHE: resourceURL: %s%n", resourceURL);

    String resourceURLPath = resourceURL.getPath();
    System.out.printf("DHE: resourceURLPath: %s%n", resourceURLPath);

    // I expect this to throw on Windows.
    Path resourcePath = Paths.get(resourceURLPath);
    System.out.printf("DHE: resourcePath: %s%n", resourcePath);

    URI resourcePathURI = resourcePath.toUri();
    System.out.printf("DHE: resourcePathURI: %s%n", resourcePathURI);
    URL resourcePathURL = resourcePathURI.toURL();
    System.out.printf("DHE: resourcePathURL: %s%n", resourcePathURL);
  }
}
