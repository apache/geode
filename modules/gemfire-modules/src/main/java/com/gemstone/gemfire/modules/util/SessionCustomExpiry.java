/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import javax.servlet.http.HttpSession;

@SuppressWarnings("serial")
public class SessionCustomExpiry implements CustomExpiry<String, HttpSession>, Serializable, Declarable {

  private static final long serialVersionUID = 182735509690640051L;

  private static final ExpirationAttributes EXPIRE_NOW = new ExpirationAttributes(1, ExpirationAction.DESTROY);

  public ExpirationAttributes getExpiry(Region.Entry<String, HttpSession> entry) {
    HttpSession session = entry.getValue();
    if (session != null) {
      return new ExpirationAttributes(entry.getValue().getMaxInactiveInterval(), ExpirationAction.DESTROY);
    } else {
      return EXPIRE_NOW;
    }
  }

  public void close() {}
  
  public void init(Properties props) {}
  
  public boolean equals(Object obj) {
    // This method is only implemented so that RegionCreator.validateRegion works properly.
    // The EntryIdleTimeout comparison fails because two of these instances are not equal.
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof SessionCustomExpiry)) {
      return false;
    }
    
    return true;
  }
}
