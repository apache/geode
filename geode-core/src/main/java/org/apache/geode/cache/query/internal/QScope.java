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
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * Nested name scope for name resolution Currently allow only one iterator per scope, and can be
 * known by zero or one identifier
 *
 * @version $Revision: 1.1 $
 *
 */
class QScope {

  private final List<RuntimeIterator> iterators = new ArrayList<>();
  /** if there is exactly one index lookup in this scope */
  boolean _oneIndexLookup = false;
  /** set if scope evaluation is limited up to this iterator */
  private RuntimeIterator limit = null;
  private final int scopeID;

  /**
   *
   * @param scopeID The scopeID assosciated with the scope
   */
  QScope(int scopeID) {
    this.scopeID = scopeID;
  }

  void setLimit(RuntimeIterator iter) {
    limit = iter;
  }

  RuntimeIterator getLimit() {
    return limit;
  }

  void bindIterator(RuntimeIterator iterator) {
    iterators.add(iterator);
    iterator.setInternalId("iter" + iterators.size());
  }

  CompiledValue resolve(String name) {
    for (RuntimeIterator _iterator : iterators) {
      if (_iterator != null && name.equals(_iterator.getName())) {
        return _iterator;
      }
    }
    return null;
  }

  List<RuntimeIterator> getIterators() {
    return iterators;
  }

  /**
   *
   * @return unique int identifying the scope. It also indicates the relative visibility of scopes,
   *         with higher scope being able to see iterators of lower scope.
   */
  int getScopeID() {
    return scopeID;
  }

}
