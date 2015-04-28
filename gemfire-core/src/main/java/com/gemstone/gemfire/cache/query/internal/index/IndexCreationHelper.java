/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * AbstractIndexCreationHelper.java
 *
 * Created on March 20, 2005, 8:26 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * 
 * @author vaibhav
 * @author Asif
 */
public abstract class IndexCreationHelper {  
  
  public static int INDEX_QUERY_SCOPE_ID = -2;
  // Canonicalized attributes.
  //	The value in these fields is set during the execution of prepareFromClause
  // function
  //While the value of fromClause is reset in execution of prepareFromClause,
  // to canonicalized from clause
  String fromClause;
  String indexedExpression;
  String projectionAttributes;
  //String imports;
  QCompiler compiler; // use the same compiler for each query string to use
  // imports
  Cache cache;
  //Asif : The array containing the canonicalized iterator names
  //which will get reused.
  //TODO: Asif : How to make it final so that the invokers do not end up
  // modifying it
  String[] canonicalizedIteratorNames = null;
  //Asif : Array containing canonicalized iterator definitions
  //TODO: Asif : How to make it final so that the invokers do not end up
  // modifying it
  String[] canonicalizedIteratorDefinitions = null;

  IndexCreationHelper(String fromClause, String projectionAttributes,
      Cache cache) throws IndexInvalidException {
    this.cache = cache;
    // Asif:LThe fromClause,indexedExpression & projectionAttributes
    // will get modified with the canonicalized value , once the
    // constructor of derived class is over.
    this.fromClause = fromClause;
    //this.indexedExpression = indexedExpression;
    this.projectionAttributes = projectionAttributes;
    // this.imports = imports;
    this.compiler = new QCompiler(true /* used from index creation*/);
    /*
     * if (this.imports != null) { this.compiler.compileImports(this.imports); }
     */
  }

  public String getCanonicalizedProjectionAttributes() {
    return projectionAttributes;
  }

  /*
   * TODO:Asif . Check if this function is required public String getImports() {
   * return this.imports; }
   */
  public String getCanonicalizedIndexedExpression() {
    return indexedExpression;
  }

  public String getCanonicalizedFromClause() {
    return fromClause;
  }

  public Cache getCache() {
    return cache;
  }

  /*
   *Asif: This function returns the canonicalized Iterator Definitions of the from
   * clauses used in Index creation
   */
  public String[] getCanonicalizedIteratorDefinitions() {
    return this.canonicalizedIteratorDefinitions;
  }
  
  boolean isMapTypeIndex() {
   return false; 
  } 
  
  public abstract List getIterators();
  abstract CompiledValue getCompiledIndexedExpression();
  abstract Region getRegion();
}
