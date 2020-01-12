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
package org.apache.geode.util.test;

import java.nio.file.FileSystemNotFoundException;

import org.apache.geode.test.util.ResourceUtils;

/**
 * @deprecated Please use {@link ResourceUtils} instead.
 */
@Deprecated
public class TestUtil {

  /**
   * Return the path to a named resource. This finds the resource on the classpath using the rules
   * of class.getResource. For a relative path it will look in the same package as the class, for an
   * absolute path it will start from the base package.
   *
   * <p>
   * Best practice is to put the resource in the same package as your test class and load it with
   * this method.
   *
   * <p>
   * Deprecated: Please use {@link ResourceUtils} instead. Examples:
   *
   * <pre>
   * String resourcePath = ResourceUtils.getResource(clazz, name).getPath();
   *
   * String tempFilePath = ResourceUtils.createTempFileFromResource(clazz, name).getAbsolutePath();
   * </pre>
   *
   * @param clazz the class to look relative too
   * @param name the name of the resource, eg "cache.xml"
   *
   * @deprecated Please use {@link ResourceUtils} instead.
   */
  @Deprecated
  public static String getResourcePath(Class<?> clazz, String name) {
    try {
      return ResourceUtils.getResource(clazz, name).getPath();
    } catch (FileSystemNotFoundException e) {
      return ResourceUtils.createTempFileFromResource(clazz, name).getAbsolutePath();
    }
  }

  /**
   * Return the path to a named resource. This finds the resource on the classpath using the rules
   * of ClassLoader.getResource.
   *
   * <p>
   * Deprecated: Please use {@link ResourceUtils} instead. Examples:
   *
   * <pre>
   * String resourcePath = ResourceUtils.getResource(classLoader, name).getPath();
   *
   * String tempFilePath =
   *     ResourceUtils.createTempFileFromResource(classLoader, name).getAbsolutePath();
   * </pre>
   *
   * @param classLoader the ClassLoader to look up resource in
   * @param name the name of the resource, eg "cache.xml"
   *
   * @deprecated Please use {@link ResourceUtils} instead.
   */
  @Deprecated
  public static String getResourcePath(ClassLoader classLoader, String name) {
    try {
      return ResourceUtils.getResource(classLoader, name).getPath();
    } catch (FileSystemNotFoundException e) {
      return ResourceUtils.createTempFileFromResource(classLoader, name).getAbsolutePath();
    }
  }
}
