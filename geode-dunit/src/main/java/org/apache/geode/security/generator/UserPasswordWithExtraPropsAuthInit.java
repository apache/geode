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
package org.apache.geode.security.generator;

import java.util.Iterator;
import java.util.Properties;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.templates.UserPasswordAuthInit;

/**
 * An {@link AuthInitialize} implementation that obtains the user name and password as the
 * credentials from the given set of properties. If keep-extra-props property exits, it will copy
 * rest of the properties provided in getCredential props argument will also be copied as new
 * credentials.
 *
 * @since GemFire 5.5
 */
public class UserPasswordWithExtraPropsAuthInit extends UserPasswordAuthInit {

  public static final String SECURITY_PREFIX = DistributionConfig.SECURITY_PREFIX_NAME;
  public static final String EXTRA_PROPS = "security-keep-extra-props";

  public static AuthInitialize create() {
    return new UserPasswordWithExtraPropsAuthInit();
  }

  public UserPasswordWithExtraPropsAuthInit() {
    super();
  }

  @Override
  public Properties getCredentials(final Properties securityProperties,
      final DistributedMember server, final boolean isPeer) throws AuthenticationFailedException {
    final Properties securityPropertiesCopy =
        super.getCredentials(securityProperties, server, isPeer);
    final String extraProps = securityProperties.getProperty(EXTRA_PROPS);

    if (extraProps != null) {
      for (Iterator it = securityProperties.keySet().iterator(); it.hasNext();) {
        final String key = (String) it.next();
        if (key.startsWith(SECURITY_PREFIX) && key.equalsIgnoreCase(USER_NAME) == false
            && key.equalsIgnoreCase(PASSWORD) == false
            && key.equalsIgnoreCase(EXTRA_PROPS) == false) {
          securityPropertiesCopy.setProperty(key, securityProperties.getProperty(key));
        }
      }
      this.securityLogWriter
          .fine("got everything and now have: " + securityPropertiesCopy.keySet().toString());
    }

    return securityPropertiesCopy;
  }
}
