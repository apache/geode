/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.util.test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import com.gemstone.gemfire.internal.FileUtil;

public class TestUtil {
  
  /**
   * Return the path to a named resource. This finds the resource on the classpath
   * using the rules of class.getResource. For a relative path it will look in the
   * same package as the class, for an absolute path it will start from the base
   * package.
   * 
   * Best practice is to put the resource in the same package as your test class
   * and load it with this method.
   *  
   * @param clazz the class to look relative too
   * @param name the name of the resource, eg "cache.xml"
   */
  public static String getResourcePath(Class<?> clazz, String name) {
    URL resource = clazz.getResource(name);
    if(resource == null) {
      throw new RuntimeException("Could not find resource " + name);
    }
    try {
      String path = resource.toURI().getPath();
      if(path == null) {
        String filename = name.replaceFirst(".*/", "");
        File tmpFile = File.createTempFile(filename, null);
        tmpFile.deleteOnExit();
        FileUtil.copy(resource, tmpFile);
        return tmpFile.getAbsolutePath();
      }
      return path;
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException("Failed getting path to resource " + name, e);
    }
  }
}
