/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PathExpressionEvaluator.java
 *
 * Created on February 11, 2005, 4:25 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.List;

import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.RegionEntry;

/**
 * 
 * @author vaibhav
 */
public interface IndexedExpressionEvaluator {

  public String getIndexedExpression();

  public String getFromClause();

  public String getProjectionAttributes();

  /** @param add true if adding to index, false if removing */ 
  public void evaluate(RegionEntry target, boolean add) throws IMQException;

  public void initializeIndex(boolean loadEntries) throws IMQException;

  public ObjectType getIndexResultSetType();
  
  public void expansion(List expandedResults, Object lowerBoundKey, Object upperBoundKey, int lowerBoundOperator, int upperBoundOperator, Object value) throws IMQException;

  public List getAllDependentIterators();
}
