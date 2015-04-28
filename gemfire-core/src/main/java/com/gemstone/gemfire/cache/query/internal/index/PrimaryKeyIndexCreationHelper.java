/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PrimaryIndexCreationHelper.java
 *
 * Created on March 20, 2005, 7:21 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.internal.*;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import java.util.List;

/**
 * 
 * @author vaibhav
 */
public class PrimaryKeyIndexCreationHelper extends IndexCreationHelper  {

  ExecutionContext context = null;
  
  final Region region;

  public PrimaryKeyIndexCreationHelper(String fromClause,
      String indexedExpression, String projectionAttributes, Cache cache,
      ExecutionContext  externalContext, IndexManager imgr) throws IndexInvalidException {
    super(fromClause, projectionAttributes, cache);
    if( externalContext == null) {
      context = new ExecutionContext(null, cache);
    }else {
      this.context = externalContext;
    }
    context.newScope(1);
    this.region = imgr.region;
    prepareFromClause( imgr);
    prepareIndexExpression(indexedExpression);
    prepareProjectionAttributes(projectionAttributes);
  }

  private void prepareFromClause(IndexManager imgr)
      throws IndexInvalidException {
    List list = this.compiler.compileFromClause(fromClause);
    if (list.size() > 1) { throw new IndexInvalidException(LocalizedStrings.PrimaryKeyIndexCreationHelper_THE_FROMCLAUSE_FOR_A_PRIMARY_KEY_INDEX_SHOULD_ONLY_HAVE_ONE_ITERATOR_AND_THE_COLLECTION_MUST_BE_A_REGION_PATH_ONLY.toLocalizedString()); }
    try {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) list.get(0);
      if (iterDef.getCollectionExpr().getType() != OQLLexerTokenTypes.RegionPath) { throw new IndexInvalidException(LocalizedStrings.PrimaryKeyIndexCreationHelper_THE_FROMCLAUSE_FOR_A_PRIMARY_KEY_INDEX_SHOULD_BE_A_REGION_PATH_ONLY.toLocalizedString()); }
      iterDef.computeDependencies(this.context);
      RuntimeIterator rIter = (iterDef.getRuntimeIterator(this.context));
      String definition = rIter.getDefinition();
      this.canonicalizedIteratorDefinitions = new String[1];
      this.canonicalizedIteratorDefinitions[0] = definition;
      //    Asif: Bind the Index_Internal_ID to the RuntimeIterator
      String name = imgr.putCanonicalizedIteratorNameIfAbsent(definition);
      rIter.setIndexInternalID(name);
      this.canonicalizedIteratorNames = new String[1];
      this.canonicalizedIteratorNames[0] = name;
      this.fromClause = new StringBuffer(definition).append(' ').append(name)
          .toString();
      context.bindIterator(rIter);
    }
    catch (IndexInvalidException e) {
      throw e; //propagate
    }
    catch (Exception e) {
      throw new IndexInvalidException(e); // wrap any other exceptions
    }
  }

  private void prepareIndexExpression(String indexedExpression)
      throws IndexInvalidException {
    List indexedExprs = this.compiler
        .compileProjectionAttributes(indexedExpression);
    if (indexedExprs == null || indexedExprs.size() != 1) { throw new IndexInvalidException(LocalizedStrings.PrimaryKeyIndexCreationHelper_INVALID_INDEXED_EXPRESSOION_0.toLocalizedString(indexedExpression)); }
    CompiledValue expr = (CompiledValue) ((Object[]) indexedExprs.get(0))[1];
    if (expr.getType() == CompiledValue.LITERAL)
        throw new IndexInvalidException(LocalizedStrings.PrimaryKeyIndexCreationHelper_INVALID_INDEXED_EXPRESSOION_0.toLocalizedString(indexedExpression));
    try {
      StringBuffer sb = new StringBuffer();
      expr.generateCanonicalizedExpression(sb, context);
      this.indexedExpression = sb.toString();
    }
    catch (Exception e) {
      //e.printStackTrace();
      throw new IndexInvalidException(LocalizedStrings.PrimaryKeyIndexCreationHelper_INVALID_INDEXED_EXPRESSOION_0_N_1.toLocalizedString(new Object[] {indexedExpression, e.getMessage()}));
    }
  }

  private void prepareProjectionAttributes(String projectionAttributes)
      throws IndexInvalidException {
    if (projectionAttributes != null && !projectionAttributes.equals("*")) { throw new IndexInvalidException(LocalizedStrings.PrimaryKeyIndexCreationHelper_INVALID_PROJECTION_ATTRIBUTES_0.toLocalizedString(projectionAttributes)); }
    this.projectionAttributes = projectionAttributes;
  }

  public Region getRegion() {
    return region;
  }

  public List getIterators() {
    return null;
  }

  public CompiledValue getCompiledIndexedExpression() {
    return null;
  }
  
}
