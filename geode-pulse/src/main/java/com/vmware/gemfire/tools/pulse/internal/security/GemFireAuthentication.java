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
package com.vmware.gemfire.tools.pulse.internal.security;

import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.SpringSecurityCoreVersion;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Spring security authentication object for GemFire
 * 
 * To use GemFire Integrated Security Model set Spring Application Profile to pulse.authentication.gemfire
 * 
 * 1. Authentication : 
 *    1.a GemFire profile creates JMX connection with given credentials at the login time. 
 *    1.b Successful connect is considered as Successful Authentication for Pulse WebApp
 *    
 *    
 * 2. Authorization :
 *    2.a Using newly created authenticated connection AccessControlMXBean is called to get authentication
 *      levels. See @See {@link #populateAuthorities(JMXConnector)}. This sets Spring Security Authorities
 *    2.b DataBrowser end-points are required to be authorized against Spring Granted Authority
 *      @See spring-security.xml
 *    2.c When executing Data-Browser query, user-level jmx connection is used so at to put access-control
 *      over the resources query is accessing. 
 *      @See #com.vmware.gemfire.tools.pulse.internal.data.JMXDataUpdater#executeQuery
 *         
 * 3. Connection Management - Spring Security LogoutHandler closes session level connection
 *
 * TODO : Better model would be to maintain background connection map for Databrowser instead
 * of each web session creating rmi connection and map user to correct entry in the connection map
 *
 * @since version 9.0
 */
public class GemFireAuthentication extends UsernamePasswordAuthenticationToken {	

  private final static PulseLogWriter LOGGER = PulseLogWriter.getLogger();
  
	private JMXConnector jmxc=null;	
	
	public GemFireAuthentication(Object principal, Object credentials, Collection<GrantedAuthority> list, JMXConnector jmxc) {
		super(principal, credentials, list);
		this.jmxc = jmxc;
	}

	private static final long serialVersionUID = SpringSecurityCoreVersion.SERIAL_VERSION_UID;
		
	
	public void closeJMXConnection(){
		try {
			jmxc.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public MBeanServerConnection getRemoteMBeanServer() {
		try {
			return jmxc.getMBeanServerConnection();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static boolean authorize(String role){
		try {
			GemFireAuthentication authentication = (GemFireAuthentication) SecurityContextHolder
					.getContext().getAuthentication();
			MBeanServerConnection mbeanServer = authentication
					.getRemoteMBeanServer();
			LOGGER.fine("#GemFireAuthentication : Checking for role="+role);
			ObjectName name = new ObjectName(PulseConstants.OBJECT_NAME_ACCESSCONTROL_MBEAN);
			Object[] params = new Object[] {role};
			String[] signature = new String[] {String.class.getCanonicalName()};
			Boolean result = (Boolean)mbeanServer.invoke(name, "authorize", params, signature);
			return result;
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		} catch (InstanceNotFoundException e) {
			throw new RuntimeException(e);
		} catch (MBeanException e) {
			throw new RuntimeException(e);
		} catch (ReflectionException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static ArrayList<GrantedAuthority> populateAuthorities(JMXConnector jmxc) {
		ObjectName name;
		try {
			name = new ObjectName(PulseConstants.OBJECT_NAME_ACCESSCONTROL_MBEAN);
			MBeanServerConnection mbeanServer = jmxc.getMBeanServerConnection();
			ArrayList<GrantedAuthority> authorities = new ArrayList<>();
			authorities.add(new SimpleGrantedAuthority("ROLE_USER"));
			for(String role : PulseConstants.PULSE_ROLES){
				Object[] params = new Object[] {"PULSE", role};
				String[] signature = new String[] {String.class.getCanonicalName(), String.class.getCanonicalName()};
				boolean result = (Boolean)mbeanServer.invoke(name, "authorize", params, signature);
				if(result){
				  //spring sec require ROLE_ prefix
					authorities.add(new SimpleGrantedAuthority("ROLE_"+role)); 
				}
			}
			return authorities;
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		} catch (InstanceNotFoundException e) {
			throw new RuntimeException(e);
		} catch (MBeanException e) {
			throw new RuntimeException(e);
		} catch (ReflectionException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}

	public JMXConnector getJmxc() {
		return jmxc;
	}
	
	

}
