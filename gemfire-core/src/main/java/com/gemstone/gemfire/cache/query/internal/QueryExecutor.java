/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.Set;

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * An interface allowing different Region implementations to support 
 * querying. 
 * 
 * @author Mitch Thomas
 * @since 5.5
 */
public interface QueryExecutor {
  //TODO Yogesh , fix this signature 
  public Object executeQuery(DefaultQuery query, Object[] parameters,  Set buckets)
  throws FunctionDomainException, TypeMismatchException,
  NameResolutionException, QueryInvocationTargetException;
  
  public String getName();
}
