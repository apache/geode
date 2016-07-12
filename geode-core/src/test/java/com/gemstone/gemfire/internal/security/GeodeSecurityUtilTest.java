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
package com.gemstone.gemfire.internal.security;


import com.gemstone.gemfire.internal.ClassNotFoundExceptionDUnitTest;
import com.gemstone.gemfire.management.internal.security.JSONAuthorization;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.security.IntegratedSecurityCacheLifecycleIntegrationTest.SpySecurityManager;
import com.gemstone.gemfire.security.templates.SampleSecurityManager;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import javassist.tools.reflect.Sample;

@Category({ UnitTest.class, SecurityTest.class })
public class GeodeSecurityUtilTest {

  @Test
  public void testGetObjectShouldPass(){
    Object forTetsing = GeodeSecurityUtil.getObject( String.class.getName(), String.class);
    assertThat(forTetsing).isExactlyInstanceOf(String.class);
  }

  @Test
  public void testGetObjectShouldBeNullWithNullClassName(){
    assertThatThrownBy(() -> GeodeSecurityUtil.getObject( null, String.class)).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void testGetObjectShouldBeNullWithNullClassType(){
    assertThatThrownBy(() -> GeodeSecurityUtil.getObject( String.class.getName(), null)).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void testGetObjectShouldFailWithMismatchedType(){
    assertThatThrownBy(() -> GeodeSecurityUtil.getObject( String.class.getName(), Integer.class)).isExactlyInstanceOf(GemFireSecurityException.class);
  }

  @Test
  public void testGetObjectShouldFailWithEmptyString(){
    assertThatThrownBy(() -> GeodeSecurityUtil.getObject( "", Integer.class)).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void testGetObjectShouldFailWithBadClassName(){
    assertThatThrownBy(() -> GeodeSecurityUtil.getObject( "nonsense", Integer.class)).isExactlyInstanceOf(AuthenticationRequiredException.class);
  }
}
