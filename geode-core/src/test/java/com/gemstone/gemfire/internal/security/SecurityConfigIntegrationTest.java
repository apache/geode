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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.apache.geode.security.templates.SampleSecurityManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.SecurableComponents;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class SecurityConfigIntegrationTest {

  @Test
  public void securityEnabledComponentsDefaultShouldBeAll() throws Exception {
    Properties props = new Properties();
    props.put(SECURITY_MANAGER, SampleSecurityManager.class.getName());
    props.put(SampleSecurityManager.SECURITY_JSON, "org/apache/geode/security/templates/security.json");

    DistributionConfig config = new DistributionConfigImpl(props);
    Properties securityProps = config.getSecurityProps();

    assertThat(securityProps).containsKeys(SECURITY_MANAGER, SECURITY_ENABLED_COMPONENTS);
    assertThat(securityProps.getProperty(SECURITY_ENABLED_COMPONENTS)).isEqualTo(SecurableComponents.ALL);

    GeodeSecurityUtil.initSecurity(securityProps);

    assertThat(GeodeSecurityUtil.isClientSecurityRequired());
    assertThat(GeodeSecurityUtil.isGatewaySecurityRequired());
    assertThat(GeodeSecurityUtil.isPeerSecurityRequired());
    assertThat(GeodeSecurityUtil.isJmxSecurityRequired());
    assertThat(GeodeSecurityUtil.isHttpServiceSecurityRequired());
  }
}
