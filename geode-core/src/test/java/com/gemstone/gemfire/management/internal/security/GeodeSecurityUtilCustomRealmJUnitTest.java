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

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * this test and ShiroUtilWithIniFileJunitTest uses the same test body, but initialize the SecurityUtils differently.
 * If you change shiro-ini.json, remmber to change the shiro.ini to match the changes as well.
 */

@Category(UnitTest.class)
public class GeodeSecurityUtilCustomRealmJUnitTest extends GeodeSecurityUtilWithIniFileJUnitTest {
  @BeforeClass
  public static void beforeClass() throws Exception{
    props.put(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, JSONAuthorization.class.getName() + ".create");
    props.put(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME, JSONAuthorization.class.getName() + ".create");
    JSONAuthorization.setUpWithJsonFile("shiro-ini.json");
    GeodeSecurityUtil.initSecurity(props);
  }

}
