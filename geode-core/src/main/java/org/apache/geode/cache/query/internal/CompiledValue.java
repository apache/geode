/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.query.internal;

import org.apache.geode.cache.query.*;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.internal.DistributionConfig;

import java.util.List;
import java.util.Set;

/**
 * Class Description
 * 
 * @version $Revision: 1.1 $
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
  public final static int FIELD = -16;
  public final static int GROUP_BY_SELECT = -17;
  public static  final int INDEX_RESULT_THRESHOLD_DEFAULT = 100;
  public static final String INDX_THRESHOLD_PROP_STR = DistributionConfig.GEMFIRE_PREFIX + "Query.INDEX_THRESHOLD_SIZE";
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
