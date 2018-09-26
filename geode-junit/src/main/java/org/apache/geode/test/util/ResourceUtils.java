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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;

/**
 * {@code ResourceUtils} is a utility class for tests that use resources and copy them to
 * directories such as {@code TemporaryFolder}.
 *
 * <p>
 * See also {@link Resources#getResource(String)} and {@link Resources#getResource(Class, String)}.
 */
public class ResourceUtils {

  /**
   * Returns the class identified by {@code depth} element of the call stack.
   */
  public static Class<?> getCallerClass(final int depth) throws ClassNotFoundException {
    return Class.forName(getCallerClassName(depth + 1));
  }

  /**
   * Returns the name of the class identified by {@code depth} element of the call stack.
   */
  public static String getCallerClassName(final int depth) {
    return new Throwable().getStackTrace()[depth].getClassName();
  }

  /**
   * Finds {@code resourceName} using the {@code ClassLoader} of the caller class.
   *
   * @return the URL of the resource
   */
  public static URL getResource(final String resourceName) throws ClassNotFoundException {
    URL configResource = getCallerClass(2).getResource(resourceName);
    assertThat(configResource).as(resourceName).isNotNull();
    return configResource;
  }

  /**
   * Finds {@code resourceName} using the {@code ClassLoader} of {@code classInSamePackage}.
   *
   * @return the URL of the resource
   */
  public static URL getResource(final Class<?> classInSamePackage, final String resourceName) {
    URL configResource = classInSamePackage.getResource(resourceName);
    assertThat(configResource).as(resourceName).isNotNull();
    return configResource;
  }

  /**
   * Copies a {@code resource} to a {@code file} in {@code targetFolder}.
   *
   * @return the newly created file
   */
  public static File createFileFromResource(final URL resource, final File targetFolder,
      final String fileName) throws IOException, URISyntaxException {
    File targetFile = new File(targetFolder, fileName);
    IOUtils.copy(resource.openStream(), new FileOutputStream(targetFile));
    assertThat(targetFile).hasSameContentAs(new File(resource.toURI()));
    return targetFile;
  }
}
