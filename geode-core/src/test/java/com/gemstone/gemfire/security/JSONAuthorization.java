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
package com.gemstone.gemfire.security;

import java.io.IOException;

import org.apache.geode.security.templates.SampleSecurityManager;
import com.gemstone.gemfire.util.test.TestUtil;

public class JSONAuthorization extends SampleSecurityManager {

  public static String AUTH1_JSON = "/com/gemstone/gemfire/management/internal/security/auth1.json";
  public static String AUTH2_JSON = "/com/gemstone/gemfire/management/internal/security/auth2.json";
  public static String AUTH3_JSON = "/com/gemstone/gemfire/management/internal/security/auth3.json";
  public static String CACHE_SERVER_JSON = "/com/gemstone/gemfire/management/internal/security/cacheServer.json";
  public static String CLIENT_SERVER_JSON = "/com/gemstone/gemfire/management/internal/security/clientServer.json";
  public static String SHIRO_INI_JSON = "/com/gemstone/gemfire/management/internal/security/shiro-ini.json";
  public static String PEER_AUTH_JSON = "/com/gemstone/gemfire/security/peerAuth.json";

  public static JSONAuthorization create() throws IOException {
    return new JSONAuthorization();
  }

  public static void setUpWithJsonFile(String jsonFileName) throws IOException {
    String json = readFile(TestUtil.getResourcePath(JSONAuthorization.class, jsonFileName));
    readSecurityDescriptor(json);
  }
}
