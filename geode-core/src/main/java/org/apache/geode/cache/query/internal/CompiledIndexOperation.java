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

import java.util.*;

import org.apache.geode.cache.query.*;
import org.apache.geode.cache.*;
import org.apache.geode.internal.i18n.LocalizedStrings;

import java.lang.reflect.Array;

/**
 * Class Description
 * 
 * @version $Revision: 1.1 $
 */
public class CompiledIndexOperation extends AbstractCompiledValue implements 
   MapIndexable 
{

  private CompiledValue receiver;
  private CompiledValue indexExpr;
  
  private boolean evalRegionAsEntry = false;

  public CompiledIndexOperation(CompiledValue receiver, CompiledValue indexExpr) {
    this.receiver = receiver;
    this.indexExpr = indexExpr;
  }

  public CompiledIndexOperation(CompiledValue receiver,
      CompiledValue indexExpr, boolean evalRegionAsEntry) {
    this.receiver = receiver;
    this.indexExpr = indexExpr;
    this.evalRegionAsEntry = evalRegionAsEntry;
  }

  @Override
  public List getChildren() {
    List list = new ArrayList();
    list.add(receiver);
    list.add(indexExpr);
    return list;
  }
  
  public int getType() {
    return TOK_LBRACK;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    context.addDependencies(this, this.receiver.computeDependencies(context));
    return context.addDependencies(this, this.indexExpr
        .computeDependencies(context));
  }

  public Object evaluate(ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    Object rcvr = this.receiver.evaluate(context);
    Object index = this.indexExpr.evaluate(context);

    if (rcvr == null || rcvr == QueryService.UNDEFINED){ 
      return QueryService.UNDEFINED;
    }
    // In case of cq, the rcvr could be Region.Entry or CqEntry
    // get the value from the entry for further processing
    if (context.isCqQueryContext()
        && (rcvr instanceof Region.Entry || rcvr instanceof CqEntry)) {
      try {
        if (rcvr instanceof Region.Entry) {
          Region.Entry re = (Region.Entry) rcvr;
          if (re.isDestroyed()) {
            return QueryService.UNDEFINED;
          }
          rcvr = re.getValue();
        } else if (rcvr instanceof CqEntry) {
          CqEntry re = (CqEntry) rcvr;
          rcvr = re.getValue();
        }
      } catch (EntryDestroyedException ede) {
        // Even though isDestory() check is made, the entry could
        // throw EntryDestroyedException if the value becomes null.
        return QueryService.UNDEFINED;
      }
    }
    
    if (rcvr instanceof Map) { return ((Map) rcvr).get(index); }
    if ((rcvr instanceof List) || rcvr.getClass().isArray()
        || (rcvr instanceof String)) {
      if (!(index instanceof Integer)) { throw new TypeMismatchException(LocalizedStrings.CompiledIndexOperation_INDEX_EXPRESSION_MUST_BE_AN_INTEGER_FOR_LISTS_OR_ARRAYS.toLocalizedString()); }
    }
    if (rcvr instanceof List) { return ((List) rcvr).get(((Integer) index)
        .intValue()); }
    if (rcvr instanceof String) { return new Character(((String) rcvr)
        .charAt(((Integer) index).intValue())); }
    if (rcvr.getClass().isArray()) {
      // @todo we need to handle primitive arrays here and wrap the result //
      /*
       * in the appropriate wrapper type (i.e. java.lang.Integer, etc.) if (rcvr
       * instanceof Object[]) { return
       * ((Object[])rcvr)[((Integer)index).intValue()]; } throw new
       * UnsupportedOperationException("indexing primitive arrays not yet
       * implemented");
       */
      return Array.get(rcvr, ((Integer) index).intValue());
    }
    //Asif : In case of 4.1.0 branch where the Map implementation is not
    // present,
    // receiver will be an instance of Region but for Map implementation ( in
    // trunk)
    // it is an instance of QRegion only
    if (rcvr instanceof QRegion) {
      Region.Entry entry = ((QRegion) rcvr).getEntry(index);
      if (entry == null) { return null; }
      return this.evalRegionAsEntry ? entry : entry.getValue();
    }
    /*
     * if (rcvr instanceof Region) { Region.Entry entry =
     * ((Region)rcvr).getEntry(index); if (entry == null) { return null; }
     * return this.evalRegionAsEntry? entry:entry.getValue(); }
     */
    throw new TypeMismatchException(LocalizedStrings.CompiledIndexOperation_INDEX_EXPRESSION_NOT_SUPPORTED_ON_OBJECTS_OF_TYPE_0.toLocalizedString(rcvr.getClass().getName()));
  }

  //Asif :Function for generating canonicalized expression
  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer,
      ExecutionContext context) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException {
    //  Asif: The canonicalization of Index operator will be of
    // the form IterX.getPositions[IterY.a.b.c]
    clauseBuffer.insert(0, ']');
    indexExpr.generateCanonicalizedExpression(clauseBuffer, context);
    clauseBuffer.insert(0, '[');
    receiver.generateCanonicalizedExpression(clauseBuffer, context);
  }

  public CompiledValue getReceiver() {
    return receiver;
  }

  public CompiledValue getExpression() {
    return indexExpr;
  }


  public CompiledValue getMapLookupKey()
  {
    return this.indexExpr;
  }

  
  public CompiledValue getRecieverSansIndexArgs()
  {
    return this.receiver;
  }

  
  public List<CompiledValue> getIndexingKeys()
  {
    List<CompiledValue> list = new ArrayList<CompiledValue>(1);
    list.add(this.indexExpr);
    return list;
  }
}
