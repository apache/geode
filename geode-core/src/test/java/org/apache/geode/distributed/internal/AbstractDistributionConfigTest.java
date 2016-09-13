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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Answers.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.security.SecurableComponents;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
@RunWith(MockitoJUnitRunner.class)
public class AbstractDistributionConfigTest {

  @Mock(answer = CALLS_REAL_METHODS)
  private AbstractDistributionConfig abstractDistributionConfig;

  @Test
  public void testNoCommaInvalidStringThrows() {
    assertThatThrownBy(() -> abstractDistributionConfig.checkSecurityEnabledComponents("This has no commas in it")).isExactlyInstanceOf(GemFireConfigException.class);
  }

  @Test
  public void testOneSecurityEnabledComponents() {
    String returnValue = abstractDistributionConfig.checkSecurityEnabledComponents(SecurableComponents.JMX);
    assertThat(returnValue).isEqualTo(SecurableComponents.JMX);
  }

  @Test
  public void testEmptySecurityEnabledComponents() {
    String returnValue = abstractDistributionConfig.checkSecurityEnabledComponents("");
    assertThat(returnValue).isEqualTo("");
  }

  @Test
  public void testNoneSecurityEnabledComponents() {
    String returnValue = abstractDistributionConfig.checkSecurityEnabledComponents("none");
    assertThat(returnValue).isEqualTo("none");
  }

  @Test
  public void testNullSecurityEnabledComponents() {
    String returnValue = abstractDistributionConfig.checkSecurityEnabledComponents(null);
    assertThat(returnValue).isEqualTo(null);
  }

  @Test
  public void testTwoSecurityEnabledComponents() {
    String returnValue = abstractDistributionConfig.checkSecurityEnabledComponents(SecurableComponents.JMX + "," + SecurableComponents.SERVER);
    assertThat(returnValue).isEqualTo(SecurableComponents.JMX + "," + SecurableComponents.SERVER);
  }

  @Test
  public void testOneValidSecurityEnabledComponentAndOneInvalid() {
    assertThatThrownBy(() -> abstractDistributionConfig.checkSecurityEnabledComponents(SecurableComponents.JMX + "," + SecurableComponents.SERVER + "," + "this should throw")).isExactlyInstanceOf(GemFireConfigException.class);
  }
}
