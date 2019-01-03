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
package org.apache.geode.modules.util;

import java.io.Serializable;
import java.util.Properties;

import javax.servlet.http.HttpSession;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;

@SuppressWarnings("serial")
public class SessionCustomExpiry
    implements CustomExpiry<String, HttpSession>, Serializable, Declarable {

  private static final long serialVersionUID = 182735509690640051L;

  private static final ExpirationAttributes EXPIRE_NOW =
      new ExpirationAttributes(1, ExpirationAction.DESTROY);

  public ExpirationAttributes getExpiry(Region.Entry<String, HttpSession> entry) {
    HttpSession session = entry.getValue();
    if (session != null) {
      return new ExpirationAttributes(entry.getValue().getMaxInactiveInterval(),
          ExpirationAction.DESTROY);
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


  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public String toString() {
    return this.getClass().toString();
  }
}
