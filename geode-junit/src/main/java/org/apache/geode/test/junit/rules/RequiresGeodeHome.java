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
package org.apache.geode.test.junit.rules;

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * This {@code Rule} is used to indicate tests that require the GEODE_HOME environment varible to be
 * set. (For example, any test that relies on the assembled Pulse WAR or GFSH binary.)
 */
@SuppressWarnings("serial")
public class RequiresGeodeHome extends SerializableExternalResource {

  private static final String GEODE_HOME_NOT_SET_MESSAGE =
      "This test requires a GEODE_HOME environment variable that points to the location "
          + "of geode-assembly/build/install/apache-geode." + lineSeparator()
          + "For instructions on how to set this variable if running tests through IntelliJ, see "
          + "https://stackoverflow.com/a/32761503/3988499";

  @Override
  protected void before() {
    getGeodeHome();
  }

  public File getGeodeHome() {
    String geodeHomePath = System.getenv("GEODE_HOME");
    assertThat(geodeHomePath)
        .withFailMessage(GEODE_HOME_NOT_SET_MESSAGE)
        .isNotNull();

    File geodeHome = new File(geodeHomePath);
    assertThat(geodeHome)
        .exists()
        .isDirectoryContaining(file -> file.getName().startsWith("bin"));

    return geodeHome;
  }

  @Override
  public String toString() {
    return getGeodeHome().getAbsolutePath();
  }
}
