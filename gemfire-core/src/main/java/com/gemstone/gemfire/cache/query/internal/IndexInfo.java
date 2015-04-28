/*
 * =========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledComparison.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 * =========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;

public class IndexInfo
{
  final private CompiledValue _key;

  final CompiledValue _path;

  final int _operator;

  final IndexProtocol _index;

  final int _matchLevel;

  final int[] mapping;

  IndexInfo(CompiledValue key, CompiledValue path, IndexProtocol index,
      int matchLevel, int mapping[], int op) {
    _key = key;
    _path = path;
    _operator = op;
    _index = index;
    _matchLevel = matchLevel;
    this.mapping = mapping;
  }
  
  Object evaluateIndexKey(ExecutionContext context) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    if(((AbstractIndex)_index).isMapType()) {
       //assert _path.getType() == OQLLexerTokenTypes.METHOD_INV;
       //Get the map key & value. both need to be passed as index key.      
       CompiledValue mapKey = ((MapIndexable)this._path).getMapLookupKey();
       return new Object[]{ this._key.evaluate(context),mapKey.evaluate(context)};
    }else {
      return _key.evaluate(context);
    }
  }
  
  public CompiledValue _key() {
    return this._key;
  }
   
  public CompiledValue _path() {
    return this._path;
  }

  public int _operator() {
    return this._operator;
  }

  public IndexProtocol _getIndex() {
    return _index;
  }
}
