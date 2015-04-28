/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import java.util.Iterator;
import java.util.Properties;

/**
 * The SSL configuration settings for a GemFire distributed system.
 *
 * @author    Kirk Lund
 *
 */
public class SSLConfig {
  
  //private static final String PREFIX = "javax.net.ssl.";

  private boolean enabled = DistributionConfig.DEFAULT_SSL_ENABLED;
  private String protocols = DistributionConfig.DEFAULT_SSL_PROTOCOLS;
  private String ciphers = DistributionConfig.DEFAULT_SSL_CIPHERS;
  private boolean requireAuth = DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
  
  /** 
   * SSL implementation-specific key-value pairs. Each key should be prefixed 
   * with <code>javax.net.ssl.</code>
   */
  private Properties properties = new Properties();

  public SSLConfig() {
  }
  
  public SSLConfig(boolean enabled) {
    this.enabled = enabled;
  }
  
	public boolean isEnabled() {
		return this.enabled;
	}
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public String getProtocols() {
		return this.protocols;
	}
	public void setProtocols(String protocols) {
		this.protocols = protocols;
	}

	public String getCiphers() {
		return this.ciphers;
	}
	public void setCiphers(String ciphers) {
		this.ciphers = ciphers;
	}

	public boolean isRequireAuth() {
		return this.requireAuth;
	}
	public void setRequireAuth(boolean requireAuth) {
		this.requireAuth = requireAuth;
	}

	public Properties getProperties() {
		return this.properties;
	}
	public void setProperties(Properties newProps) {
          this.properties = new Properties();
          for (Iterator iter = newProps.keySet().iterator(); iter.hasNext();) {
            String key = (String) iter.next();
//            String value = newProps.getProperty(key);
            this.properties.setProperty(key, newProps.getProperty(key));
          }
	}

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 */
	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("[SSLConfig: ");
		sb.append("enabled=").append(this.enabled);
		sb.append(", protocols=").append(this.protocols);
		sb.append(", ciphers=").append(this.ciphers);
		sb.append(", requireAuth=").append(this.requireAuth);
		sb.append(", properties=").append(this.properties);
		sb.append("]");
		return sb.toString();
	}

  /**
   * Populates a <code>Properties</code> object with the SSL-related
   * configuration information used by {@link
   * com.gemstone.gemfire.distributed.DistributedSystem#connect}.
   *
   * @since 4.0
   */
  public void toDSProperties(Properties props) {
    props.setProperty(DistributionConfig.SSL_ENABLED_NAME,
                      String.valueOf(this.enabled));

    if (this.enabled) {
      props.setProperty(DistributionConfig.SSL_PROTOCOLS_NAME,
                        this.protocols); 
      props.setProperty(DistributionConfig.SSL_CIPHERS_NAME,
                        this.ciphers);
      props.setProperty(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
                        String.valueOf(this.requireAuth));
    }
  }

}

