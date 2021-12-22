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

import static java.lang.String.format;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.GemFireAST;
import org.apache.geode.cache.query.internal.parse.OQLLexer;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.parse.OQLParser;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.MapTypeImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.MapType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Class Description
 *
 * @version $Revision: 1.1 $s
 */
public class QCompiler implements OQLLexerTokenTypes {
  private static final Logger logger = LogService.getLogger();

  private final Stack<Object> stack = new Stack<>();
  private final Map<String, String> imports = new HashMap<>();
  private final boolean isForIndexCompilation;
  private boolean traceOn;

  public QCompiler() {
    isForIndexCompilation = false;
  }

  public QCompiler(boolean isForIndexCompilation) {
    this.isForIndexCompilation = isForIndexCompilation;
  }

  /*
   * compile the string into a Query (returns the root CompiledValue)
   */
  public CompiledValue compileQuery(String oqlSource) {
    try {
      OQLLexer lexer = new OQLLexer(new StringReader(oqlSource));
      OQLParser parser = new OQLParser(lexer);
      // by default use Unsupported AST class, overridden for supported
      // operators in the grammar proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.queryProgram();
      GemFireAST n = (GemFireAST) parser.getAST();
      n.compile(this);
    } catch (Exception ex) {
      // This is to make sure that we are wrapping any antlr exception with Geode Exception.
      throw new QueryInvalidException(format("Syntax error in query: %s", ex.getMessage()), ex);
    }
    Assert.assertTrue(stackSize() == 1, "stack size = " + stackSize());
    return pop();
  }

  /**
   * @return List<CompiledIteratorDef>
   */
  public List<CompiledIteratorDef> compileFromClause(String fromClause) {
    try {
      OQLLexer lexer = new OQLLexer(new StringReader(fromClause));
      OQLParser parser = new OQLParser(lexer);
      // by default use Unsupported AST class, overridden for supported
      // operators in the grammar proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.loneFromClause();
      GemFireAST n = (GemFireAST) parser.getAST();
      n.compile(this);
    } catch (Exception ex) {
      // This is to make sure that we are wrapping any antlr exception with Geode Exception.
      throw new QueryInvalidException(format("Syntax error in query: %s", ex.getMessage()), ex);
    }
    Assert.assertTrue(stackSize() == 1, "stack size = " + stackSize());
    return pop();
  }


  /**
   * @return List<CompiledIteratorDef> or null if projectionAttrs is '*'
   */
  public List<CompiledIteratorDef> compileProjectionAttributes(String projectionAttributes) {
    try {
      OQLLexer lexer = new OQLLexer(new StringReader(projectionAttributes));
      OQLParser parser = new OQLParser(lexer);
      // by default use Unsupported AST class, overridden for supported
      // operators in the grammar proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.loneProjectionAttributes();
      GemFireAST n = (GemFireAST) parser.getAST();
      // don't compile TOK_STAR
      if (n.getType() == TOK_STAR) {
        return null;
      }
      n.compile(this);
    } catch (Exception ex) {
      // This is to make sure that we are wrapping any antlr exception with Geode Exception.
      throw new QueryInvalidException(format("Syntax error in query: %s", ex.getMessage()), ex);
    }
    Assert.assertTrue(stackSize() == 1, "stack size = " + stackSize() + ";stack=" + stack);
    return pop();
  }

  /**
   * Yogesh: compiles order by clause and push into the stack
   *
   */
  public void compileOrderByClause(final int numOfChildren) {
    final List<CompiledSortCriterion> list = new ArrayList<>();
    for (int i = 0; i < numOfChildren; i++) {
      list.add(0, pop());
    }
    push(list);
  }

  public void compileGroupByClause(final int numOfChildren) {
    final List<CompiledPath> list = new ArrayList<>();
    for (int i = 0; i < numOfChildren; i++) {
      list.add(0, pop());
    }
    push(list);
  }

  /**
   * compiles sort criteria present in order by clause and push into the stack
   */
  public void compileSortCriteria(String sortCriterion) {

    CompiledValue obj = pop();
    boolean criterion = sortCriterion.equals("desc");
    CompiledSortCriterion csc = new CompiledSortCriterion(criterion, obj);
    push(csc);

  }

  public void compileLimit(String limitNum) {
    push(Integer.valueOf(limitNum));
  }

  /**
   * Processes import statements only. This compiler instance remembers the imports and can be used
   * to compile other strings with this context info
   */
  public void compileImports(String imports) {
    try {
      OQLLexer lexer = new OQLLexer(new StringReader(imports));
      OQLParser parser = new OQLParser(lexer);
      // by default use Unsupported AST class, overridden for supported
      // operators in the grammar proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.loneImports();
      GemFireAST n = (GemFireAST) parser.getAST();
      n.compile(this);
    } catch (Exception ex) {
      // This is to make sure that we are wrapping any antlr exception with Geode Exception.
      throw new QueryInvalidException(format("Syntax error in query: %s", ex.getMessage()), ex);
    }
    Assert.assertTrue(stackSize() == 0, "stack size = " + stackSize() + ";stack=" + stack);
  }

  private void checkWhereClauseForAggregates(CompiledValue compiledValue) {
    if (compiledValue instanceof CompiledAggregateFunction) {
      throw new QueryInvalidException(
          "Aggregate functions can not be used as part of the WHERE clause.");
    }

    // Inner queries are supported.
    if (compiledValue instanceof CompiledSelect) {
      return;
    }

    for (Object compiledChildren : compiledValue.getChildren()) {
      checkWhereClauseForAggregates((CompiledValue) compiledChildren);
    }
  }

  public void select(Map<Integer, Object> queryComponents) {
    final CompiledValue limit;
    final Object limitObject = queryComponents.remove(OQLLexerTokenTypes.LIMIT);
    if (limitObject instanceof Integer) {
      limit = new CompiledLiteral(limitObject);
    } else {
      limit = (CompiledBindArgument) limitObject;
    }
    @SuppressWarnings("unchecked")
    final List<CompiledSortCriterion> orderByAttrs =
        (List<CompiledSortCriterion>) queryComponents.remove(OQLLexerTokenTypes.LITERAL_order);

    final List<?> iterators = (List<?>) queryComponents.remove(OQLLexerTokenTypes.LITERAL_from);
    @SuppressWarnings("unchecked")
    List<Object[]> projAttrs =
        (List<Object[]>) queryComponents.remove(OQLLexerTokenTypes.PROJECTION_ATTRS);
    if (projAttrs == null) {
      // remove any * or all attribute
      queryComponents.remove(OQLLexerTokenTypes.TOK_STAR);
      queryComponents.remove(OQLLexerTokenTypes.LITERAL_all);
    }

    // "DISTINCT" or null
    final String distinct = (String) queryComponents.remove(OQLLexerTokenTypes.LITERAL_distinct);
    @SuppressWarnings("unchecked")
    final List<String> hints =
        (List<String>) queryComponents.remove(OQLLexerTokenTypes.LITERAL_hint);

    @SuppressWarnings("unchecked")
    final List<CompiledValue> groupByClause =
        (List<CompiledValue>) queryComponents.remove(OQLLexerTokenTypes.LITERAL_group);

    // whatever remains , treat it as where whereClause
    CompiledValue where = null;

    if (queryComponents.size() == 1) {
      where = (CompiledValue) queryComponents.values().iterator().next();
      // Where clause can not contain aggregate functions.
      checkWhereClauseForAggregates(where);
    } else if (queryComponents.size() > 1) {
      throw new QueryInvalidException("Unexpected/unsupported query clauses found");
    }
    LinkedHashMap<Integer, CompiledAggregateFunction> aggMap =
        identifyAggregateExpressions(projAttrs);
    boolean isCountOnly = checkForCountOnly(aggMap, projAttrs, groupByClause);
    if (isCountOnly) {
      projAttrs = null;
    }
    CompiledSelect select = createSelect(distinct != null, isCountOnly, where, iterators, projAttrs,
        orderByAttrs, limit, hints, groupByClause, aggMap);
    push(select);
  }

  private boolean checkForCountOnly(Map<Integer, CompiledAggregateFunction> aggregateMap,
      List<?> projAttribs, List<CompiledValue> groupBy) {
    if (aggregateMap != null && aggregateMap.size() == 1 && projAttribs.size() == 1
        && groupBy == null) {
      for (Map.Entry<Integer, CompiledAggregateFunction> entry : aggregateMap.entrySet()) {
        CompiledAggregateFunction caf = entry.getValue();
        if (caf.getFunctionType() == OQLLexerTokenTypes.COUNT && caf.getParameter() == null) {
          return true;
        }
      }
    }
    return false;
  }

  private CompiledSelect createSelect(boolean isDistinct, boolean isCountOnly, CompiledValue where,
      List<?> iterators, List<?> projAttrs, List<CompiledSortCriterion> orderByAttrs,
      CompiledValue limit,
      List<String> hints, List<CompiledValue> groupByClause,
      LinkedHashMap<Integer, CompiledAggregateFunction> aggMap) {
    if (isCountOnly || (groupByClause == null && aggMap == null)
        || (aggMap == null && orderByAttrs == null)) {
      return new CompiledSelect(isDistinct, isCountOnly, where, iterators, projAttrs, orderByAttrs,
          limit, hints, groupByClause);
    } else {
      return new CompiledGroupBySelect(isDistinct, isCountOnly, where, iterators, projAttrs,
          orderByAttrs, limit, hints, groupByClause, aggMap);
    }
  }

  private LinkedHashMap<Integer, CompiledAggregateFunction> identifyAggregateExpressions(
      final List<Object[]> projAttribs) {
    if (projAttribs != null) {
      final LinkedHashMap<Integer, CompiledAggregateFunction> mapping = new LinkedHashMap<>();
      int index = 0;
      for (Object[] o : projAttribs) {
        CompiledValue proj = (CompiledValue) o[1];
        if (proj.getType() == OQLLexerTokenTypes.AGG_FUNC) {
          mapping.put(index, (CompiledAggregateFunction) proj);
        }
        ++index;
      }
      return mapping.size() == 0 ? null : mapping;
    } else {
      return null;
    }
  }

  public void projection() {
    // find an id or null on the stack, then an expr CompiledValue
    // push an Object[2] on the stack. First element is id, second is CompiledValue
    CompiledID id = pop();
    CompiledValue expr = pop();
    push(new Object[] {id == null ? null : id.getId(), expr});
  }

  public void aggregateFunction(CompiledValue expr, int aggFuncType, boolean distinctOnly) {
    push(new CompiledAggregateFunction(expr, aggFuncType, distinctOnly));
  }

  public void iteratorDef() {
    // find type id and colln on the stack

    ObjectType type = assembleType(); // can be null
    CompiledID id = TypeUtils.checkCast(pop(), CompiledID.class); // can be null
    CompiledValue colln = TypeUtils.checkCast(pop(), CompiledValue.class);

    if (type == null) {
      type = TypeUtils.OBJECT_TYPE;
    }

    push(new CompiledIteratorDef(id == null ? null : id.getId(), type, colln));
  }


  public void undefinedExpr(boolean is_defined) {
    CompiledValue value = pop();
    push(new CompiledUndefined(value, is_defined));
  }

  public void function(int function, int numOfChildren) {
    CompiledValue[] cvArr = new CompiledValue[numOfChildren];
    for (int i = numOfChildren - 1; i >= 0; i--) {
      cvArr[i] = pop();
    }
    push(new CompiledFunction(cvArr, function));
  }

  public void inExpr() {
    CompiledValue collnExpr = TypeUtils.checkCast(pop(), CompiledValue.class);
    CompiledValue elm = TypeUtils.checkCast(pop(), CompiledValue.class);
    push(new CompiledIn(elm, collnExpr));
  }

  public void constructObject(Class<?> clazz) {
    // find argList on stack only support SET for now
    Assert.assertTrue(clazz == ResultsSet.class);
    List<?> argList = TypeUtils.checkCast(pop(), List.class);
    push(new CompiledConstruction(clazz, argList));
  }

  public void pushId(String id) {
    push(new CompiledID(id));
  }

  public void pushRegion(String regionPath) {
    push(new CompiledRegion(regionPath));
  }

  public void appendPathComponent(String id) {
    CompiledValue rcvr = pop();
    push(new CompiledPath(rcvr, id));
  }

  public void pushBindArgument(int i) {
    push(new CompiledBindArgument(i));
  }

  public void pushLiteral(Object obj) {
    push(new CompiledLiteral(obj));
  }

  /**
   * used as a placeholder for a missing clause
   */
  public void pushNull() {
    push(null);
  }

  public void combine(final int num) {
    final List<?> list = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      list.add(0, pop());
    }
    push(list);
  }

  public void methodInvocation() {
    // find on stack:
    // argList, methodName, receiver (which may be null if receiver is implicit)
    List<?> argList = TypeUtils.checkCast(pop(), List.class);
    CompiledID methodName = TypeUtils.checkCast(pop(), CompiledID.class);
    CompiledValue rcvr = TypeUtils.checkCast(pop(), CompiledValue.class);
    push(new CompiledOperation(rcvr, methodName.getId(), argList));
  }

  public void indexOp() {
    // find the List of index expressions and the receiver on the stack
    Object indexParams = pop();
    final CompiledValue rcvr = TypeUtils.checkCast(pop(), CompiledValue.class);
    CompiledValue indexExpr = CompiledValue.MAP_INDEX_ALL_KEYS;

    if (indexParams != null) {
      @SuppressWarnings("unchecked")
      final List<CompiledValue> indexList = TypeUtils.checkCast(indexParams, List.class);
      if (!isForIndexCompilation && indexList.size() != 1) {
        throw new UnsupportedOperationException(
            "Only one index expression supported");
      }
      if (indexList.size() == 1) {
        indexExpr = TypeUtils.checkCast(indexList.get(0), CompiledValue.class);

        if (indexExpr.getType() == TOK_COLON) {
          throw new UnsupportedOperationException(
              "Ranges not supported in index operators");
        }
        indexExpr = TypeUtils.checkCast(indexList.get(0), CompiledValue.class);
        push(new CompiledIndexOperation(rcvr, indexExpr));
      } else {
        MapIndexable mi = new MapIndexOperation(rcvr, indexList);
        push(mi);
      }
    } else {
      if (!isForIndexCompilation) {
        throw new QueryInvalidException("Syntax error in query: * use incorrect");
      }
      push(new CompiledIndexOperation(rcvr, indexExpr));
    }

  }

  /**
   * Creates appropriate CompiledValue for the like predicate based on the sargability of the String
   * or otherwise. It also works on the last character to see if the sargable like predicate results
   * in a CompiledJunction or a Comparison. Currently we are supporting only the '%' terminated
   * "like" predicate.
   *
   * @param var The CompiledValue representing the variable
   * @param patternOrBindParam The CompiledLiteral representing the pattern of the like predicate
   * @return CompiledValue representing the "like" predicate
   *
   */
  CompiledValue createCompiledValueForLikePredicate(CompiledValue var,
      CompiledValue patternOrBindParam) {
    if (!(patternOrBindParam.getType() == CompiledBindArgument.QUERY_PARAM)) {
      CompiledLiteral pattern = (CompiledLiteral) patternOrBindParam;
      if (pattern._obj == null) {
        throw new UnsupportedOperationException(
            "Null values are not supported with LIKE predicate.");
      }
    }
    // From 6.6 Like is enhanced to support special character (% and _) at any
    // position of the string.
    return new CompiledLike(var, patternOrBindParam);
  }


  public void like() {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    CompiledValue cv = createCompiledValueForLikePredicate(v1, v2);
    push(cv);
  }

  public void compare(int opKind) {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    push(new CompiledComparison(v1, v2, opKind));
  }

  public void mod() {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    push(new CompiledMod(v1, v2));
  }

  public void arithmetic(int opKind) {
    switch (opKind) {
      case TOK_PLUS:
        addition();
        break;
      case TOK_MINUS:
        subtraction();
        break;
      case TOK_SLASH:
        division();
        break;
      case TOK_STAR:
        multiplication();
        break;
      case LITERAL_mod:
      case TOK_PERCENTAGE:
        mod();
        break;
    }
  }

  private void addition() {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    push(new CompiledAddition(v1, v2));
  }

  private void subtraction() {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    push(new CompiledSubtraction(v1, v2));
  }

  private void division() {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    push(new CompiledDivision(v1, v2));
  }

  private void multiplication() {
    CompiledValue v2 = pop();
    CompiledValue v1 = pop();
    push(new CompiledMultiplication(v1, v2));
  }

  public void or(int numTerms) {
    junction(numTerms, LITERAL_or);
  }

  public void and(int numTerms) {
    junction(numTerms, LITERAL_and);
  }

  private void junction(int numTerms, int operator) {
    // if any of the operands are junctions with same operator as this one then flatten
    final List<CompiledValue> operands = new ArrayList<>(numTerms);
    for (int i = 0; i < numTerms; i++) {
      final CompiledValue operand = pop();
      // flatten if we can
      if (operand instanceof CompiledJunction
          && ((CompiledJunction) operand).getOperator() == operator) {
        final CompiledJunction junction = (CompiledJunction) operand;
        final List<CompiledValue> jOperands = junction.getOperands();
        operands.addAll(jOperands);
      } else {
        operands.add(operand);
      }
    }

    push(new CompiledJunction(operands.toArray(new CompiledValue[0]), operator));
  }

  public void not() {
    Object obj = stack.peek();
    Assert.assertTrue(obj instanceof CompiledValue);

    if (obj instanceof Negatable) {
      ((Negatable) obj).negate();
    } else {
      push(new CompiledNegation(pop()));
    }
  }

  public void unaryMinus() {
    Object obj = stack.peek();
    Assert.assertTrue(obj instanceof CompiledValue);
    push(new CompiledUnaryMinus(pop()));

  }

  public void typecast() {
    // pop expr and type, apply type, then push result
    AbstractCompiledValue cmpVal =
        TypeUtils.checkCast(pop(), AbstractCompiledValue.class);
    ObjectType objType = assembleType();
    cmpVal.setTypecast(objType);
    push(cmpVal);
  }

  /**
   * @return null if null is on the stack
   */
  public ObjectType assembleType() {
    ObjectType objType = TypeUtils.checkCast(pop(), ObjectType.class);
    if (objType instanceof CollectionType) {
      // pop the elementType
      ObjectType elementType = assembleType();

      if (objType instanceof MapType) {
        // pop the key type
        ObjectType keyType = assembleType();
        return new MapTypeImpl(objType.resolveClass(), keyType, elementType);
      }
      return new CollectionTypeImpl(objType.resolveClass(), elementType);
    }
    return objType;
  }

  public void traceRequest() {
    traceOn = true;
  }

  public boolean isTraceRequested() {
    return traceOn;
  }

  public void setHint(final int numOfChildren) {
    final List<String> list = new ArrayList<>();
    for (int i = 0; i < numOfChildren; i++) {
      list.add(0, pop());
    }
    push(list);
  }

  public void setHintIdentifier(String text) {
    push(text);
  }

  public void importName(String qualifiedName, String asName) {
    if (asName == null) {
      // if no AS, then use the short name from qualifiedName as the AS
      int idx = qualifiedName.lastIndexOf('.');
      if (idx >= 0) {
        asName = qualifiedName.substring(idx + 1);
      } else {
        asName = qualifiedName;
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace("QCompiler.importName: {},{}", asName, qualifiedName);
    }
    imports.put(asName, qualifiedName);
  }

  public <T> T pop() {
    @SuppressWarnings("unchecked")
    final T obj = (T) stack.pop();
    if (logger.isTraceEnabled()) {
      logger.trace("QCompiler.pop: {}", obj);
    }
    return obj;
  }

  public <T> void push(final T obj) {
    if (logger.isTraceEnabled()) {
      logger.trace("QCompiler.push: {}", obj);
    }
    stack.push(obj);
  }

  public int stackSize() {
    return stack.size();
  }

  public ObjectType resolveType(String typeName) {
    if (typeName == null) {
      if (logger.isTraceEnabled()) {
        logger.trace("QCompiler.resolveType= {}", Object.class.getName());
      }
      return TypeUtils.OBJECT_TYPE;
    }
    // resolve with imports
    final String as = imports.get(typeName);
    if (as != null) {
      typeName = as;
    }

    Class<?> resultClass;
    try {
      resultClass = InternalDataSerializer.getCachedClass(typeName);
    } catch (ClassNotFoundException e) {
      throw new QueryInvalidException(format("Type not found: %s", typeName), e);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("QCompiler.resolveType= {}", resultClass.getName());
    }
    return new ObjectTypeImpl(resultClass);
  }

  private static class MapIndexOperation extends AbstractCompiledValue implements MapIndexable {
    private final CompiledValue rcvr;
    private final List<CompiledValue> indexList;

    public MapIndexOperation(CompiledValue rcvr, List<CompiledValue> indexList) {
      this.rcvr = rcvr;
      this.indexList = indexList;
    }

    @Override
    public CompiledValue getReceiverSansIndexArgs() {
      return rcvr;
    }

    @Override
    public CompiledValue getMapLookupKey() {
      throw new UnsupportedOperationException("Function invocation not expected");
    }

    @Override
    public List<CompiledValue> getIndexingKeys() {
      return indexList;
    }

    @Override
    public Object evaluate(ExecutionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException("Method execution not expected");
    }

    @Override
    public int getType() {
      throw new UnsupportedOperationException("Method execution not expected");
    }
  }

}
