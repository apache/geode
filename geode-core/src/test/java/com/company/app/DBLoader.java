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
package com.company.app;

import java.util.Properties;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.LoaderHelper;

/**
 * A <code>CacheLoader</code> that is <code>Declarable</code>
 *
 * @since GemFire 3.2.1
 */
public class DBLoader implements CacheLoader, Declarable {

  private Properties props = new Properties();

  @Override
  public Object load(LoaderHelper helper) throws CacheLoaderException {

    throw new UnsupportedOperationException("I do NOTHING");
  }

  @Override
  public void init(java.util.Properties props) {
    this.props = props;
  }

  @Override
  public void close() {}

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof DBLoader)) {
      return false;
    }

    DBLoader other = (DBLoader) obj;
    return props.equals(other.props);
  }

}
