/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledValue.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.types.*;

/**
 * Class Description
 * 
 * @version $Revision: 1.1 $
 * @author ericz
 */
public interface CompiledValue {

  // extra node types: use negative numbers so they don't collide with token
  // types
  public final static int COMPARISON = -1;
  public final static int FUNCTION = -2;
  public final static int JUNCTION = -3;
  public final static int LITERAL = -4;
  public final static int PATH = -5;
  public final static int CONSTRUCTION = -6;
  public final static int GROUPJUNCTION = -7;
  public final static int ALLGROUPJUNCTION = -8;
  public final static int COMPOSITEGROUPJUNCTION = -10;
  public final static int RANGEJUNCTION = -11;
  public final static int NOTEQUALCONDITIONEVALUATOR = -12;
  public final static int SINGLECONDNEVALUATOR= -13;
  public final static int DOUBLECONDNRANGEJUNCTIONEVALUATOR = -14;
  public final static int LIKE = -15;
  public static  final int INDEX_RESULT_THRESHOLD_DEFAULT = 100;
  public static final String INDX_THRESHOLD_PROP_STR = "gemfire.Query.INDEX_THRESHOLD_SIZE";
  public static final String INDEX_INFO = "index_info";
  public static final int indexThresholdSize = Integer.getInteger(INDX_THRESHOLD_PROP_STR, INDEX_RESULT_THRESHOLD_DEFAULT).intValue();
  public static final String RESULT_TYPE = "result_type";
  public static final String PROJ_ATTRIB = "projection";
  public static final String ORDERBY_ATTRIB = "orderby";
  public static final IndexInfo[] NO_INDEXES_IDENTIFIER = new IndexInfo[0];
  public static final String RESULT_LIMIT = "limit";
  public static final String CAN_APPLY_LIMIT_AT_INDEX = "can_apply_limit_at_index";
  public static final String CAN_APPLY_ORDER_BY_AT_INDEX = "can_apply_orderby_at_index";
  public static final String PREF_INDEX_COND = "preferred_index_condition"; 
  public static final String QUERY_INDEX_HINTS = "query_index_hints";
  public static final CompiledValue MAP_INDEX_ALL_KEYS = new AbstractCompiledValue() {
    
    @Override
    public void generateCanonicalizedExpression(StringBuffer clauseBuffer,
        ExecutionContext context) throws AmbiguousNameException,
        TypeMismatchException, NameResolutionException {
      throw new QueryInvalidException
      ("* cannot be used with index operator. To use as key for map lookup, " +
      		"it should be enclosed in ' '");
    }
    
    public Object evaluate(ExecutionContext context) {
      throw new QueryInvalidException
      ("* cannot be used with index operator. To use as key for map lookup, " +
                "it should be enclosed in ' '");
    }

    @Override
    public CompiledValue getReceiver() {
      return super.getReceiver();
    }
    
    public int getType()
    {
      return OQLLexerTokenTypes.TOK_STAR;
    }

  };
  public int getType();

  public ObjectType getTypecast();

  public Object evaluate(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException;

  // returns null if N/A
  public List getPathOnIterator(RuntimeIterator itr, ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException;

  public PlanInfo getPlanInfo(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException;

  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException;

  public boolean isDependentOnIterator(RuntimeIterator itr,
      ExecutionContext context);

  public boolean isDependentOnCurrentScope(ExecutionContext context);

  /**
   * general-purpose visitor (will be used for extracting region path)
   */
  //  public boolean visit(QVisitor visitor);
  //Asif :Function for generating from clause
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer,
      ExecutionContext context) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException;

  /**
   * Populates the Set passed with  the name of the Region which, if any ,  will be the bottommost
   * object (CompiledRegion). The default implemenation is provided  in the AbstractCompiledValue
   * & overridden in the CompiledSelect as it can contain multiple iterators 
   */
  public void getRegionsInQuery(Set regionNames, Object[] parameters);
  
  /** Get the CompiledValues that this owns */
  public List getChildren();
  
  public void visitNodes(NodeVisitor visitor);
  
  public static interface NodeVisitor {
    /** @return true to continue or false to stop */
    public boolean visit(CompiledValue node);
  }
  
  public CompiledValue getReceiver();
  
}
