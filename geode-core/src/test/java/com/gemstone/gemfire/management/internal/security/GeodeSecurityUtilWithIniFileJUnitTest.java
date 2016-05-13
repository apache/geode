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

import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.apache.shiro.util.ThreadContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * this test and ShiroUtilCustomRealmJUunitTest uses the same test body, but initialize the SecurityUtils differently.
 * If you change shiro.ini, remmber to change the shiro-ini.json to match the changes as well.
 */
@Category(UnitTest.class)
public class GeodeSecurityUtilWithIniFileJUnitTest {
  protected static Properties props = new Properties();
  @BeforeClass
  public static void beforeClass() throws Exception{
    props.setProperty(DistributionConfig.SECURITY_SHIRO_INIT_NAME, "shiro.ini");
    GeodeSecurityUtil.initSecurity(props);
  }

  @AfterClass
  public static void afterClass(){
    ThreadContext.remove();
  }

  @Test
  public void testRoot(){
    GeodeSecurityUtil.login("root", "secret");
    GeodeSecurityUtil.authorize(TestCommand.none);
    GeodeSecurityUtil.authorize(TestCommand.everyOneAllowed);
    GeodeSecurityUtil.authorize(TestCommand.dataRead);
    GeodeSecurityUtil.authorize(TestCommand.dataWrite);
    GeodeSecurityUtil.authorize(TestCommand.regionARead);
    GeodeSecurityUtil.authorize(TestCommand.regionAWrite);
    GeodeSecurityUtil.authorize(TestCommand.clusterWrite);
    GeodeSecurityUtil.authorize(TestCommand.clusterRead);
  }

  @Test
  public void testGuest(){
    GeodeSecurityUtil.login("guest", "guest");
    GeodeSecurityUtil.authorize(TestCommand.none);
    GeodeSecurityUtil.authorize(TestCommand.everyOneAllowed);

    assertNotAuthorized(TestCommand.dataRead);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.regionARead);
    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    GeodeSecurityUtil.logout();
  }

  @Test
  public void testRegionAReader(){
    GeodeSecurityUtil.login("regionAReader", "password");
    GeodeSecurityUtil.authorize(TestCommand.none);
    GeodeSecurityUtil.authorize(TestCommand.everyOneAllowed);
    GeodeSecurityUtil.authorize(TestCommand.regionARead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.dataRead);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    GeodeSecurityUtil.logout();
  }

  @Test
  public void testRegionAUser(){
    GeodeSecurityUtil.login("regionAUser", "password");
    GeodeSecurityUtil.authorize(TestCommand.none);
    GeodeSecurityUtil.authorize(TestCommand.everyOneAllowed);
    GeodeSecurityUtil.authorize(TestCommand.regionAWrite);
    GeodeSecurityUtil.authorize(TestCommand.regionARead);

    assertNotAuthorized(TestCommand.dataRead);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    GeodeSecurityUtil.logout();
  }

  @Test
  public void testDataReader(){
    GeodeSecurityUtil.login("dataReader", "12345");
    GeodeSecurityUtil.authorize(TestCommand.none);
    GeodeSecurityUtil.authorize(TestCommand.everyOneAllowed);
    GeodeSecurityUtil.authorize(TestCommand.regionARead);
    GeodeSecurityUtil.authorize(TestCommand.dataRead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    GeodeSecurityUtil.logout();
  }

  @Test
  public void testReader(){
    GeodeSecurityUtil.login("reader", "12345");
    GeodeSecurityUtil.authorize(TestCommand.none);
    GeodeSecurityUtil.authorize(TestCommand.everyOneAllowed);
    GeodeSecurityUtil.authorize(TestCommand.regionARead);
    GeodeSecurityUtil.authorize(TestCommand.dataRead);
    GeodeSecurityUtil.authorize(TestCommand.clusterRead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterWrite);
    GeodeSecurityUtil.logout();
  }

  private void assertNotAuthorized(OperationContext context){
    assertThatThrownBy(()-> GeodeSecurityUtil.authorize(context)).isInstanceOf(GemFireSecurityException.class).hasMessageContaining(context.toString());
  }

}
