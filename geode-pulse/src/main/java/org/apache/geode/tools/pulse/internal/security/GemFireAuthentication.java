/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.tools.pulse.internal.security;

import java.util.ArrayList;
import java.util.Collection;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.SpringSecurityCoreVersion;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

/**
 * Spring security authentication object for GemFire
 * <p>
 * To use GemFire Integrated Security Model set Spring Application Profile to pulse.authentication.gemfire
 * <p>
 * 1. Authentication :
 * 1.a GemFire profile creates JMX connection with given credentials at the login time.
 * 1.b Successful connect is considered as Successful Authentication for Pulse WebApp
 * <p>
 * <p>
 * 2. Authorization :
 * 2.a Using newly created authenticated connection AccessControlMXBean is called to get authentication
 * levels. See @See {@link #populateAuthorities(JMXConnector)}. This sets Spring Security Authorities
 * 2.b DataBrowser end-points are required to be authorized against Spring Granted Authority
 * @since GemFire version 9.0
 */
public class GemFireAuthentication extends UsernamePasswordAuthenticationToken {

  private final static PulseLogWriter logger = PulseLogWriter.getLogger();

  private JMXConnector jmxc = null;

  public GemFireAuthentication(Object principal, Object credentials, Collection<GrantedAuthority> list, JMXConnector jmxc) {
    super(principal, credentials, list);
    this.jmxc = jmxc;
  }

  private static final long serialVersionUID = SpringSecurityCoreVersion.SERIAL_VERSION_UID;

  public static ArrayList<GrantedAuthority> populateAuthorities(JMXConnector jmxc) {
    ObjectName name;
    ArrayList<GrantedAuthority> authorities = new ArrayList<>();
    try {
      name = new ObjectName(PulseConstants.OBJECT_NAME_ACCESSCONTROL_MBEAN);
      MBeanServerConnection mbeanServer = jmxc.getMBeanServerConnection();

      for (String role : PulseConstants.PULSE_ROLES) {
        Object[] params = role.split(":");
        String[] signature = new String[] { String.class.getCanonicalName(), String.class.getCanonicalName() };
        boolean result = (Boolean) mbeanServer.invoke(name, "authorize", params, signature);
        if (result) {
          authorities.add(new SimpleGrantedAuthority(role));
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    return authorities;

  }

  public JMXConnector getJmxc() {
    return jmxc;
  }

}
