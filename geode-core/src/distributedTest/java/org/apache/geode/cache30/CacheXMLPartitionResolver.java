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
package org.apache.geode.cache30;


import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.internal.cache.xmlcache.Declarable2;

public class CacheXMLPartitionResolver implements PartitionResolver, Serializable, Declarable2 {
  private final Properties resolveProps;

  public CacheXMLPartitionResolver() {
    resolveProps = new Properties();
    resolveProps.setProperty("routingType", "key");
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public Serializable getRoutingObject(EntryOperation opDetails) {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public int hashCode() {
    return Objects.hashCode(resolveProps);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!obj.getClass().equals(getClass())) {
      return false;
    }
    CacheXMLPartitionResolver other = (CacheXMLPartitionResolver) obj;
    return resolveProps.equals(other.getConfig());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.xmlcache.Declarable2#getConfig()
   */
  @Override
  public Properties getConfig() {
    return resolveProps;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
   */
  @Override
  public void init(Properties props) {
    resolveProps.putAll(props);
  }
}
