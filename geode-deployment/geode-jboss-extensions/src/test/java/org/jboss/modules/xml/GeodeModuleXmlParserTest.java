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
package org.jboss.modules.xml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.NoSuchFileException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestRule;

public class GeodeModuleXmlParserTest {

  @Rule
  public final TestRule restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void testRelativeSystemPropertyResolver()
      throws URISyntaxException {

    System.setProperty("custom.property", "some/file/location");

    URL resource = GeodeModuleXmlParserTest.class
        .getResource("/org/jboss/modules/xml/module_custom_property.xml");
    File moduleXmlFile = new File(resource.toURI());

    Throwable thrown = catchThrowable(() -> GeodeModuleXmlParser.parseModuleXml(null, "test",
        moduleXmlFile.getParentFile(), moduleXmlFile));

    assertThat(thrown.getCause().getCause())
        .isInstanceOf(getVersion() == 8 ? FileNotFoundException.class : NoSuchFileException.class)
        .hasMessageContaining(
            moduleXmlFile.getParentFile().getAbsolutePath() + "/some/file/location/lib/foo.jar");
  }

  @Test
  public void testAbsoluteSystemPropertyResolver()
      throws URISyntaxException {

    System.setProperty("custom.property", "/some/file/location");

    URL resource = GeodeModuleXmlParserTest.class
        .getResource("/org/jboss/modules/xml/module_custom_property.xml");
    File moduleXmlFile = new File(resource.toURI());

    Throwable thrown = catchThrowable(() -> GeodeModuleXmlParser.parseModuleXml(null, "test",
        moduleXmlFile.getParentFile(), moduleXmlFile));

    assertThat(thrown.getCause().getCause())
        .isInstanceOf(getVersion() == 8 ? FileNotFoundException.class : NoSuchFileException.class)
        .hasMessageContaining("/some/file/location/lib/foo.jar");
  }

  @Test
  public void testParserWithoutSystemPropertyResolverAbsolute()
      throws URISyntaxException {

    URL resource = GeodeModuleXmlParserTest.class
        .getResource("/org/jboss/modules/xml/module_no_property_absolute_path.xml");
    File moduleXmlFile = new File(resource.toURI());

    Throwable thrown = catchThrowable(() -> GeodeModuleXmlParser.parseModuleXml(null, "test",
        moduleXmlFile.getParentFile(), moduleXmlFile));

    assertThat(thrown.getCause().getCause())
        .isInstanceOf(getVersion() == 8 ? FileNotFoundException.class : NoSuchFileException.class)
        .hasMessageContaining("/lib/foo.jar");
  }

  @Test
  public void testParserWithoutSystemPropertyResolverRelative()
      throws URISyntaxException {

    URL resource = GeodeModuleXmlParserTest.class
        .getResource("/org/jboss/modules/xml/module_no_property_relative_path.xml");
    File moduleXmlFile = new File(resource.toURI());

    Throwable thrown = catchThrowable(() -> GeodeModuleXmlParser.parseModuleXml(null, "test",
        moduleXmlFile.getParentFile(), moduleXmlFile));

    assertThat(thrown.getCause().getCause())
        .isInstanceOf(getVersion() == 8 ? FileNotFoundException.class : NoSuchFileException.class)
        .hasMessageContaining(moduleXmlFile.getParentFile().getAbsolutePath() + "/lib/foo.jar");
  }

  private static int getVersion() {
    String version = System.getProperty("java.version");
    if (version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if (dot != -1) {
        version = version.substring(0, dot);
      }
    }
    return Integer.parseInt(version);
  }
}
