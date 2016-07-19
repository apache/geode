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
package com.gemstone.gemfire.management.internal.security;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.geode.security.templates.SampleSecurityManager;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * Used by test code. when using this class for security-manager, you will need explicitly call setUpWithJsonFile
 * to initialize the acl (access control list).
 */
public class JSONAuthorization extends SampleSecurityManager {

  /**
   * Override the child class's implemention to look for jsonFile in the same package as this class instead of
   * in the classpath
   * @param jsonFileName
   * @throws IOException
   */
  public static void setUpWithJsonFile(String jsonFileName) throws IOException {
    String filePath = TestUtil.getResourcePath(JSONAuthorization.class, jsonFileName);
    File file = new File(filePath);
    FileReader reader = new FileReader(file);
    char[] buffer = new char[(int) file.length()];
    reader.read(buffer);
    String json = new String(buffer);
    reader.close();
    readSecurityDescriptor(json);
  }
}
