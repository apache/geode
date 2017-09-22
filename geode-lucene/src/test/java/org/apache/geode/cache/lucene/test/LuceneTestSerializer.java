/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.test;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.lucene.document.Document;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * A Test LuceneSerializer that takes properties during construction (init)
 */
public class LuceneTestSerializer implements LuceneSerializer {

  protected final Properties props = new Properties();

  @Override
  public void init(Properties props) {
    this.props.putAll(props);
  }

  public Properties getProperties() {
    return this.props;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LuceneTestSerializer that = (LuceneTestSerializer) o;

    return props.equals(that.props);
  }

  @Override
  public int hashCode() {
    return props.hashCode();
  }

  @Override
  public Collection<Document> toDocuments(LuceneIndex index, Object value) {
    return Collections.emptyList();
  }
}
