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

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.AbstractIndex;
import org.apache.geode.cache.query.internal.index.IndexProtocol;

public class IndexInfo {
  private final CompiledValue _key;

  final CompiledValue _path;

  final int _operator;

  final IndexProtocol _index;

  final int _matchLevel;

  final int[] mapping;

  IndexInfo(CompiledValue key, CompiledValue path, IndexProtocol index, int matchLevel,
      int[] mapping, int op) {
    _key = key;
    _path = path;
    _operator = op;
    _index = index;
    _matchLevel = matchLevel;
    this.mapping = mapping;
  }

  Object evaluateIndexKey(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    if (((AbstractIndex) _index).isMapType()) {
      // assert _path.getType() == OQLLexerTokenTypes.METHOD_INV;
      // Get the map key & value. both need to be passed as index key.
      CompiledValue mapKey = ((MapIndexable) _path).getMapLookupKey();
      return new Object[] {_key.evaluate(context), mapKey.evaluate(context)};
    } else {
      return _key.evaluate(context);
    }
  }

  public CompiledValue _key() {
    return _key;
  }

  public CompiledValue _path() {
    return _path;
  }

  public int _operator() {
    return _operator;
  }

  public IndexProtocol _getIndex() {
    return _index;
  }
}
