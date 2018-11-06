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
import org.apache.geode.internal.logging.LogService;

/**
 * Class Description
 *
 * @version $Revision: 1.1 $s
 */
public class QCompiler implements OQLLexerTokenTypes {
  private static final Logger logger = LogService.getLogger();

  private Stack stack = new Stack();
  private Map imports = new HashMap();
  private final boolean isForIndexCompilation;
  private boolean traceOn;

  public QCompiler() {
    this.isForIndexCompilation = false;
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
      // operators in the grammer proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.queryProgram();
      GemFireAST n = (GemFireAST) parser.getAST();
      n.compile(this);
    } catch (Exception ex) { // This is to make sure that we are wrapping any antlr exception with
                             // GemFire Exception.
      throw new QueryInvalidException(
          String.format("Syntax error in query: %s", ex.getMessage()),
          ex);
    }
    Assert.assertTrue(stackSize() == 1, "stack size = " + stackSize());
    return (CompiledValue) pop();
  }

  /** Returns List<CompiledIteratorDef> */
  public List compileFromClause(String fromClause) {
    try {
      OQLLexer lexer = new OQLLexer(new StringReader(fromClause));
      OQLParser parser = new OQLParser(lexer);
      // by default use Unsupported AST class, overridden for supported
      // operators in the grammer proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.loneFromClause();
      GemFireAST n = (GemFireAST) parser.getAST();
      n.compile(this);
    } catch (Exception ex) { // This is to make sure that we are wrapping any antlr exception with
                             // GemFire Exception.
      throw new QueryInvalidException(
          String.format("Syntax error in query: %s", ex.getMessage()),
          ex);
    }
    Assert.assertTrue(stackSize() == 1, "stack size = " + stackSize());
    return (List) pop();
  }


  /** Returns List<CompiledIteratorDef> or null if projectionAttrs is '*' */
  public List compileProjectionAttributes(String projectionAttributes) {
    try {
      OQLLexer lexer = new OQLLexer(new StringReader(projectionAttributes));
      OQLParser parser = new OQLParser(lexer);
      // by default use Unsupported AST class, overridden for supported
      // operators in the grammer proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.loneProjectionAttributes();
      GemFireAST n = (GemFireAST) parser.getAST();
      // don't compile TOK_STAR
      if (n.getType() == TOK_STAR) {
        return null;
      }
      n.compile(this);
    } catch (Exception ex) { // This is to make sure that we are wrapping any antlr exception with
                             // GemFire Exception.
      throw new QueryInvalidException(
          String.format("Syntax error in query: %s", ex.getMessage()),
          ex);
    }
    Assert.assertTrue(stackSize() == 1, "stack size = " + stackSize() + ";stack=" + this.stack);
    return (List) pop();
  }

  /**
   * Yogesh: compiles order by clause and push into the stack
   *
   */
  public void compileOrederByClause(int numOfChildren) {
    List list = new ArrayList();
    for (int i = 0; i < numOfChildren; i++) {
      CompiledSortCriterion csc = (CompiledSortCriterion) this.stack.pop();
      list.add(0, csc);
    }
    push(list);
  }

  public void compileGroupByClause(int numOfChildren) {
    List list = new ArrayList();
    for (int i = 0; i < numOfChildren; i++) {
      Object csc = this.stack.pop();
      list.add(0, csc);
    }
    push(list);
  }

  /**
   * Yogesh: compiles sort criteria present in order by clause and push into the stack
   *
   */
  public void compileSortCriteria(String sortCriterion) {

    CompiledValue obj = (CompiledValue) this.stack.pop();
    boolean criterion = false;
    if (sortCriterion.equals("desc"))
      criterion = true;
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
      // operators in the grammer proper
      parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
      parser.loneImports();
      GemFireAST n = (GemFireAST) parser.getAST();
      n.compile(this);
    } catch (Exception ex) { // This is to make sure that we are wrapping any antlr exception with
                             // GemFire Exception.
      throw new QueryInvalidException(
          String.format("Syntax error in query: %s", ex.getMessage()),
          ex);
    }
    Assert.assertTrue(stackSize() == 0, "stack size = " + stackSize() + ";stack=" + this.stack);
  }

  public void select(Map<Integer, Object> queryComponents) {

    CompiledValue limit = null;
    Object limitObject = queryComponents.remove(OQLLexerTokenTypes.LIMIT);
    if (limitObject instanceof Integer) {
      limit = new CompiledLiteral(limitObject);
    } else {
      limit = (CompiledBindArgument) limitObject;
    }
    List<CompiledSortCriterion> orderByAttrs =
        (List<CompiledSortCriterion>) queryComponents.remove(OQLLexerTokenTypes.LITERAL_order);

    List iterators = (List) queryComponents.remove(OQLLexerTokenTypes.LITERAL_from);
    List projAttrs = (List) queryComponents.remove(OQLLexerTokenTypes.PROJECTION_ATTRS);
    if (projAttrs == null) {
      // remove any * or all attribute
      queryComponents.remove(OQLLexerTokenTypes.TOK_STAR);
      queryComponents.remove(OQLLexerTokenTypes.LITERAL_all);
    }
    // "COUNT" or null
    /*
     * String aggrExpr = (String) queryComponents .remove(OQLLexerTokenTypes.LITERAL_count);
     */

    // "DISTINCT" or null
    String distinct = (String) queryComponents.remove(OQLLexerTokenTypes.LITERAL_distinct);
    List<String> hints = null;
    Object hintObject = queryComponents.remove(OQLLexerTokenTypes.LITERAL_hint);
    if (hintObject != null) {
      hints = (List<String>) hintObject;
    }

    List<CompiledValue> groupByClause =
        (List<CompiledValue>) queryComponents.remove(OQLLexerTokenTypes.LITERAL_group);

    // whatever remains , treat it as where
    // whereClause
    CompiledValue where = null;

    if (queryComponents.size() == 1) {
      where = (CompiledValue) queryComponents.values().iterator().next();
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
      List projAttribs, List<CompiledValue> groupBy) {
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
      List iterators, List projAttrs, List<CompiledSortCriterion> orderByAttrs, CompiledValue limit,
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
      List projAttribs) {
    if (projAttribs != null) {
      LinkedHashMap<Integer, CompiledAggregateFunction> mapping =
          new LinkedHashMap<Integer, CompiledAggregateFunction>();
      int index = 0;
      for (Object o : projAttribs) {
        CompiledValue proj = (CompiledValue) ((Object[]) o)[1];
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
    CompiledID id = (CompiledID) pop();
    CompiledValue expr = (CompiledValue) pop();
    push(new Object[] {id == null ? null : id.getId(), expr});
  }

  public void aggregateFunction(CompiledValue expr, int aggFuncType, boolean distinctOnly) {
    push(new CompiledAggregateFunction(expr, aggFuncType, distinctOnly));
  }

  public void iteratorDef() {
    // find type id and colln on the stack

    ObjectType type = assembleType(); // can be null
    CompiledID id = (CompiledID) TypeUtils.checkCast(pop(), CompiledID.class); // can be null
    CompiledValue colln = (CompiledValue) TypeUtils.checkCast(pop(), CompiledValue.class);

    if (type == null) {
      type = TypeUtils.OBJECT_TYPE;
    }

    push(new CompiledIteratorDef(id == null ? null : id.getId(), type, colln));
  }


  public void undefinedExpr(boolean is_defined) {
    CompiledValue value = (CompiledValue) pop();
    push(new CompiledUndefined(value, is_defined));
  }

  public void function(int function, int numOfChildren) {
    CompiledValue[] cvArr = new CompiledValue[numOfChildren];
    for (int i = numOfChildren - 1; i >= 0; i--) {
      cvArr[i] = (CompiledValue) pop();
    }
    push(new CompiledFunction(cvArr, function));
  }

  public void inExpr() {
    CompiledValue collnExpr = (CompiledValue) TypeUtils.checkCast(pop(), CompiledValue.class);
    CompiledValue elm = (CompiledValue) TypeUtils.checkCast(pop(), CompiledValue.class);
    push(new CompiledIn(elm, collnExpr));
  }

  public void constructObject(Class clazz) {
    // find argList on stack
    // only support SET for now
    Assert.assertTrue(clazz == ResultsSet.class);
    List argList = (List) TypeUtils.checkCast(pop(), List.class);
    push(new CompiledConstruction(clazz, argList));
  }

  public void pushId(String id) {
    push(new CompiledID(id));
  }

  public void pushRegion(String regionPath) {
    push(new CompiledRegion(regionPath));
  }

  public void appendPathComponent(String id) {
    CompiledValue rcvr = (CompiledValue) pop();
    push(new CompiledPath(rcvr, id));
  }


  public void pushBindArgument(int i) {
    push(new CompiledBindArgument(i));
  }


  public void pushLiteral(Object obj) {
    push(new CompiledLiteral(obj));
  }

  public void pushNull() // used as a placeholder for a missing clause
  {
    push(null);
  }


  public void combine(int num) {
    List list = new ArrayList();
    for (int i = 0; i < num; i++) {
      list.add(0, pop());
    }
    push(list);
  }


  public void methodInvocation() {
    // find on stack:
    // argList, methodName, receiver (which may be null if receiver is implicit)
    List argList = (List) TypeUtils.checkCast(pop(), List.class);
    CompiledID methodName = (CompiledID) TypeUtils.checkCast(pop(), CompiledID.class);
    CompiledValue rcvr = (CompiledValue) TypeUtils.checkCast(pop(), CompiledValue.class);
    push(new CompiledOperation(rcvr, methodName.getId(), argList));
  }

  public void indexOp() {
    // find the List of index expressions and the receiver on the stack
    Object indexParams = pop();
    final CompiledValue rcvr = (CompiledValue) TypeUtils.checkCast(pop(), CompiledValue.class);
    CompiledValue indexExpr = CompiledValue.MAP_INDEX_ALL_KEYS;

    if (indexParams != null) {
      final List indexList = (List) TypeUtils.checkCast(indexParams, List.class);
      if (!isForIndexCompilation && indexList.size() != 1) {
        throw new UnsupportedOperationException(
            "Only one index expression supported");
      }
      if (indexList.size() == 1) {
        indexExpr = (CompiledValue) TypeUtils.checkCast(indexList.get(0), CompiledValue.class);

        if (indexExpr.getType() == TOK_COLON) {
          throw new UnsupportedOperationException(
              "Ranges not supported in index operators");
        }
        indexExpr = (CompiledValue) TypeUtils.checkCast(indexList.get(0), CompiledValue.class);
        push(new CompiledIndexOperation(rcvr, indexExpr));
      } else {
        assert this.isForIndexCompilation;

        MapIndexable mi = new MapIndexOperation(rcvr, indexList);
        push(mi);
      }
    } else {
      if (!this.isForIndexCompilation) {
        throw new QueryInvalidException(String.format("Syntax error in query: %s",
            "* use incorrect"));
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
   * @param patternOrBindParam The CompiledLiteral reprsenting the pattern of the like predicate
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
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    CompiledValue cv = createCompiledValueForLikePredicate(v1, v2);
    push(cv);
  }

  public void compare(int opKind) {
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    push(new CompiledComparison(v1, v2, opKind));
  }

  public void mod(int opKind) {
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    push(new CompiledMod(v1, v2, opKind));
  }

  public void arithmetic(int opKind) {
    switch (opKind) {
      case TOK_PLUS:
        addition(opKind);
        break;
      case TOK_MINUS:
        subtraction(opKind);
        break;
      case TOK_SLASH:
        division(opKind);
        break;
      case TOK_STAR:
        multiplication(opKind);
        break;
      case LITERAL_mod:
        mod(opKind);
        break;
      case TOK_PERCENTAGE:
        mod(opKind);
        break;
    }
  }

  private void addition(int opKind) {
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    push(new CompiledAddition(v1, v2, opKind));
  }

  private void subtraction(int opKind) {
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    push(new CompiledSubtraction(v1, v2, opKind));
  }

  private void division(int opKind) {
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    push(new CompiledDivision(v1, v2, opKind));
  }

  private void multiplication(int opKind) {
    CompiledValue v2 = (CompiledValue) pop();
    CompiledValue v1 = (CompiledValue) pop();
    push(new CompiledMultiplication(v1, v2, opKind));
  }


  public void or(int numTerms) {
    junction(numTerms, LITERAL_or);
  }

  public void and(int numTerms) {
    junction(numTerms, LITERAL_and);
  }

  private void junction(int numTerms, int operator) {
    /*
     * if any of the operands are junctions with same operator as this one then flatten
     */
    List operands = new ArrayList(numTerms);
    for (int i = 0; i < numTerms; i++) {
      CompiledValue operand = (CompiledValue) pop();
      // flatten if we can
      if (operand instanceof CompiledJunction
          && ((CompiledJunction) operand).getOperator() == operator) {
        CompiledJunction junction = (CompiledJunction) operand;
        List jOperands = junction.getOperands();
        for (int j = 0; j < jOperands.size(); j++)
          operands.add(jOperands.get(j));
      } else
        operands.add(operand);
    }

    push(new CompiledJunction(
        (CompiledValue[]) operands.toArray(new CompiledValue[operands.size()]), operator));
  }



  public void not() {
    Object obj = this.stack.peek();
    Assert.assertTrue(obj instanceof CompiledValue);

    if (obj instanceof Negatable)
      ((Negatable) obj).negate();
    else
      push(new CompiledNegation((CompiledValue) pop()));
  }

  public void unaryMinus() {
    Object obj = this.stack.peek();
    Assert.assertTrue(obj instanceof CompiledValue);
    push(new CompiledUnaryMinus((CompiledValue) pop()));

  }

  public void typecast() {
    // pop expr and type, apply type, then push result
    AbstractCompiledValue cmpVal =
        (AbstractCompiledValue) TypeUtils.checkCast(pop(), AbstractCompiledValue.class);
    ObjectType objType = assembleType();
    cmpVal.setTypecast(objType);
    push(cmpVal);
  }


  // returns null if null is on the stack
  public ObjectType assembleType() {
    ObjectType objType = (ObjectType) TypeUtils.checkCast(pop(), ObjectType.class);
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
    this.traceOn = true;
  }

  public boolean isTraceRequested() {
    return traceOn;
  }

  public void setHint(int numOfChildren) {
    ArrayList list = new ArrayList();
    for (int i = 0; i < numOfChildren; i++) {
      String hi = (String) this.stack.pop();
      list.add(0, hi);
    }
    push(list);
    // setHints(list);
  }

  public void setHintIdentifier(String text) {
    push(text);
  }

  public void importName(String qualifiedName, String asName) {
    if (asName == null) {
      // if no AS, then use the short name from qualifiedName
      // as the AS
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
    this.imports.put(asName, qualifiedName);
  }


  public Object pop() {
    Object obj = this.stack.pop();
    if (logger.isTraceEnabled()) {
      logger.trace("QCompiler.pop: {}", obj);
    }
    return obj;
  }

  public void push(Object obj) {
    if (logger.isTraceEnabled()) {
      logger.trace("QCompiler.push: {}", obj);
    }
    this.stack.push(obj);
  }

  public int stackSize() {
    return this.stack.size();
  }

  public ObjectType resolveType(String typeName) {
    if (typeName == null) {
      if (logger.isTraceEnabled()) {
        logger.trace("QCompiler.resolveType= {}", Object.class.getName());
      }
      return TypeUtils.OBJECT_TYPE;
    }
    // resolve with imports
    String as = null;
    if (this.imports != null) {
      as = (String) this.imports.get(typeName);
    }
    if (as != null)
      typeName = as;

    Class resultClass;
    try {
      resultClass = InternalDataSerializer.getCachedClass(typeName);
    } catch (ClassNotFoundException e) {
      throw new QueryInvalidException(
          String.format("Type not found: %s", typeName), e);
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

    public CompiledValue getReceiverSansIndexArgs() {
      return rcvr;
    }

    public CompiledValue getMapLookupKey() {
      throw new UnsupportedOperationException("Function invocation not expected");
    }

    public List<CompiledValue> getIndexingKeys() {
      return (List<CompiledValue>) indexList;
    }

    public Object evaluate(ExecutionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
      throw new UnsupportedOperationException("Method execution not expected");
    }

    public int getType() {
      throw new UnsupportedOperationException("Method execution not expected");
    }
  }

}
