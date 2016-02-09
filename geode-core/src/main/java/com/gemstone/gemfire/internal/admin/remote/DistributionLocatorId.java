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
   
package com.gemstone.gemfire.internal.admin.remote;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.SSLConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Identifies the host, port, and bindAddress a distribution locator is 
 * listening on.
 *
 * @author    Darrel Schneider
 * @author    Pete Matern
 * @author    Kirk Lund
 *
 */
public class DistributionLocatorId implements java.io.Serializable {
  private static final long serialVersionUID = 6587390186971937865L;
  
  private final InetAddress host;
  private final int port;
  private final String bindAddress;
  transient private SSLConfig sslConfig;
  private boolean peerLocator = true;
  private boolean serverLocator = true;
  private String hostnameForClients;

  /**
   * Constructs a DistributionLocatorId with the given host and port.
   */
  public DistributionLocatorId(InetAddress host, 
                               int port, 
                               String bindAddress, 
                               SSLConfig sslConfig) {
    this.host = host;
    this.port = port;
    this.bindAddress = validateBindAddress(bindAddress);
    this.sslConfig = validateSSLConfig(sslConfig);
  }
  
  /**
   * Constructs a DistributionLocatorId with the given port. The host will be 
   * set to the local host.
   */
  public DistributionLocatorId(int port, String bindAddress) {
    try {
      this.host = SocketCreator.getLocalHost();
    } catch (UnknownHostException ex) {
      throw new InternalGemFireException(LocalizedStrings.DistributionLocatorId_FAILED_GETTING_LOCAL_HOST.toLocalizedString(), ex);
    }
    this.port = port;
    this.bindAddress = validateBindAddress(bindAddress);
    this.sslConfig = validateSSLConfig(null);
  }
  
  public DistributionLocatorId(InetAddress host, 
                               int port, 
                               String bindAddress,
                               SSLConfig sslConfig,
                               boolean peerLocator,
                               boolean serverLocator,
                               String hostnameForClients) {
    this.host = host;
    this.port = port;
    this.bindAddress = validateBindAddress(bindAddress);
    this.sslConfig = validateSSLConfig(sslConfig);
    this.peerLocator = peerLocator;
    this.serverLocator = serverLocator;
    this.hostnameForClients = hostnameForClients;
  }
  
  /**
   * Constructs a DistributionLocatorId with a String of the form:
   * hostname[port] or hostname:bindaddress[port] or hostname@bindaddress[port]
   * <p>
   * The :bindaddress portion is optional.  hostname[port] is the more common
   * form.
   * <p>
   * Example: merry.gemstone.com[7056]<br>
   * Example w/ bind address: merry.gemstone.com:81.240.0.1[7056], or
   * merry.gemstone.com@fdf0:76cf:a0ed:9449::16[7056]
   * <p>
   * Use bindaddress[port] or hostname[port].  This object doesn't need to
   * differentiate between the two.
   */
  public DistributionLocatorId(String marshalled) {
    final int portStartIdx = marshalled.indexOf('[');
    final int portEndIdx = marshalled.indexOf(']');
    
    if (portStartIdx < 0 || portEndIdx < portStartIdx) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionLocatorId__0_IS_NOT_IN_THE_FORM_HOSTNAMEPORT.toLocalizedString(marshalled));
    }
    
    int bindIdx = marshalled.lastIndexOf('@');
    if (bindIdx < 0) {
      bindIdx = marshalled.lastIndexOf(':');
    }
    
    String hostname = marshalled.substring(0, bindIdx > -1 ? bindIdx : portStartIdx);

    if (hostname.indexOf(':') >= 0) {
      bindIdx = marshalled.lastIndexOf('@');
      hostname = marshalled.substring(0, bindIdx > -1 ? bindIdx : portStartIdx);
    }


    try {
      this.host = InetAddress.getByName(hostname);
    } catch (UnknownHostException ex) {
      throw new InternalGemFireException(LocalizedStrings.DistributionLocatorId_FAILED_GETTING_HOST_FROM_NAME_0.toLocalizedString(hostname));
    }
    
    try {
      this.port = Integer.parseInt(
          marshalled.substring(portStartIdx+1, portEndIdx));
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionLocatorId_0_DOES_NOT_CONTAIN_A_VALID_PORT_NUMBER.toLocalizedString(marshalled));
    }
    
    if (bindIdx > -1) {
      // found a bindaddress
      this.bindAddress = validateBindAddress(marshalled.substring(bindIdx+1, portStartIdx));
    }
    else {
      this.bindAddress = validateBindAddress(DistributionConfig.DEFAULT_BIND_ADDRESS);
    }
    this.sslConfig = validateSSLConfig(null);
    
    int optionsIndex = marshalled.indexOf(',');
    if(optionsIndex > 0) {
      String[] options = marshalled.substring(optionsIndex).split(",");
      for(int i = 0; i < options.length; i++) {
        String[] optionFields= options[i].split("=");
        if(optionFields.length == 2) {
          if(optionFields[0].equalsIgnoreCase("peer")) {
            this.peerLocator = Boolean.valueOf(optionFields[1]).booleanValue();
          }
          else if(optionFields[0].equalsIgnoreCase("server")) {
            this.serverLocator = Boolean.valueOf(optionFields[1]).booleanValue();
          }
          else if(optionFields[0].equalsIgnoreCase("hostname-for-clients")) {
            this.hostnameForClients = optionFields[1];
          } else {
            throw new IllegalArgumentException(marshalled + " invalid option " + optionFields[0] + ". valid options are \"peer\" or \"server\"");
          }
        }
      }
    }
  }
  
  public DistributionLocatorId(InetAddress address, Locator locator) {
    this(address,
         locator.getPort(),
         locator.getBindAddress() == null ? null : locator.getBindAddress().getHostAddress(),
         null,
         locator.isPeerLocator(),
         locator.isServerLocator(),
         locator.getHostnameForClients());
  }
  
  /**
   * Returns marshaled string that is compatible as input for {@link
   * #DistributionLocatorId(String)}.
   */
  public String marshal() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.host.getHostAddress());
    if (!this.bindAddress.isEmpty()) {
      if (this.bindAddress.contains(":")) {
        sb.append('@');
      }
      else {
        sb.append(':');
      }
      sb.append(this.bindAddress);
    }
    sb.append('[').append(this.port).append(']');
    return sb.toString();
  }

  private SSLConfig validateSSLConfig(SSLConfig sslConfig) {
    if (sslConfig == null) return new SSLConfig(); // uses defaults
    return sslConfig;
  }
  
  public SSLConfig getSSLConfig() {
    return this.sslConfig;
  }
  public void setSSLConfig(SSLConfig sslConfig) {
    this.sslConfig = validateSSLConfig(sslConfig);
  }
  
  /** Returns the communication port. */
  public int getPort() {
    return this.port;
  }
  
  /** Returns the host. */
  public InetAddress getHost() {
    return this.host;
  }
  
  /** Returns true if this is a multicast address:port */
  public boolean isMcastId() {
    return this.host.isMulticastAddress();
  }
  
  /** 
   * Returns the bindAddress; value is "" unless host has multiple network 
   * interfaces. 
   */
  public String getBindAddress() {
    return this.bindAddress;
  }
  
  /**
   * @since 5.7
   */
  public boolean isPeerLocator() {
    return this.peerLocator;
  }
  
  /**
   * @since 5.7
   */
  public boolean isServerLocator() {
    return this.serverLocator;
  }

  /**
   * @since 5.7
   */
  public String getHostnameForClients() {
    return this.hostnameForClients;
  }

//  private String hostNameToString() {
//    if (this.host.isMulticastAddress()) {
//      return this.host.getHostAddress();
//    } else {
//      return this.host.getHostName();
//    }
//  }

  /** Returns the default bindAddress if bindAddress is null. */
  private String validateBindAddress(String bindAddress) {
    if (bindAddress == null) {
      return DistributionConfig.DEFAULT_BIND_ADDRESS;
    }
    return bindAddress;
  }
  
	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 */
  @Override
	public String toString() {
          StringBuffer sb = new StringBuffer();
  
          // if bindAddress then use that instead of host...
          if (this.bindAddress != null && this.bindAddress.length() > 0) {
            sb.append(this.bindAddress);
          }
          else {
            if (isMcastId()) {
              sb.append(this.host.getHostAddress());
            }
            else {
              sb.append(SocketCreator.getHostName(this.host));
            }
          }
    
	  sb.append("[").append(this.port).append("]");
	  return sb.toString();
	}

	/**
	 * Indicates whether some other object is "equal to" this one.
	 *
	 * @param  other  the reference object with which to compare.
	 * @return true if this object is the same as the obj argument;
	 *         false otherwise.
	 */
  @Override
	public boolean equals(Object other) {
	  if (other == this) return true;
	  if (other == null) return false;
	  if (!(other instanceof DistributionLocatorId)) return  false;
	  final DistributionLocatorId that = (DistributionLocatorId) other;

	  if (this.host != that.host &&
	  	  !(this.host != null &&
	  	  this.host.equals(that.host))) return false;
	  if (this.port != that.port) return false;
	  if (this.bindAddress != that.bindAddress &&
	  	  !(this.bindAddress != null &&
	  	  this.bindAddress.equals(that.bindAddress))) return false;

	  return true;
	}

	/**
	 * Returns a hash code for the object. This method is supported for the
	 * benefit of hashtables such as those provided by java.util.Hashtable.
	 *
	 * @return the integer 0 if description is null; otherwise a unique integer.
	 */
  @Override
	public int hashCode() {
	  int result = 17;
	  final int mult = 37;

	  result = mult * result + 
		  (this.host == null ? 0 : this.host.hashCode());
	  result = mult * result + this.port;
	  result = mult * result + 
		  (this.bindAddress == null ? 0 : this.bindAddress.hashCode());

	  return result;
	}
  
  /**
   * Converts a collection of {@link Locator} instances to a collection of
   * DistributionLocatorId instances. Note this will use {@link 
   * SocketCreator#getLocalHost()} as the host for DistributionLocatorId.
   * This is because all instances of Locator are local only.
   * 
   * @param locators collection of Locator instances
   * @return collection of DistributionLocatorId instances
   * @throws UnknownHostException
   * @see Locator
   */
  public static Collection<DistributionLocatorId> asDistributionLocatorIds(Collection<Locator> locators) throws UnknownHostException {
    if (locators.isEmpty()) {
      return Collections.emptyList();
    }
    Collection<DistributionLocatorId> locatorIds = new ArrayList<DistributionLocatorId>();
    for (Locator locator : locators) {
      DistributionLocatorId locatorId = new DistributionLocatorId(
          SocketCreator.getLocalHost(), locator);
      locatorIds.add(locatorId);
    }
    return locatorIds;
  }
  
  /**
   * Marshals a collection of {@link Locator} instances to a collection of
   * DistributionLocatorId instances. Note this will use {@link 
   * SocketCreator#getLocalHost()} as the host for DistributionLocatorId.
   * This is because all instances of Locator are local only.
   * 
   * @param locatorIds collection of DistributionLocatorId instances
   * @return collection of String instances
   * @see #marshal()
   */
  public static Collection<String> asStrings(Collection<DistributionLocatorId> locatorIds) {
    if (locatorIds.isEmpty()) {
      return Collections.emptyList();
    }
    Collection<String> strings = new ArrayList<String>();
    for (DistributionLocatorId locatorId : locatorIds) {
      strings.add(locatorId.marshal());
    }
    return strings;
  }
  
}
