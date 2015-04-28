/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: Scope.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.*;

/**
 * Nested name scope for name resolution Currently allow only one iterator per
 * scope, and can be known by zero or one identifier
 * 
 * @version $Revision: 1.1 $
 * @author ericz
 *  
 */
class QScope  {

  //private RuntimeIterator _iterator;
  private List iterators;
  boolean _oneIndexLookup = false; // if there is exactly one index lookup in
                                   // this scope
  // set if scope evaluation is limited up to this iterator
  private RuntimeIterator limit = null;
  private int scopeID = 0;
  
  /**
   * 
   * @param scopeID The scopeID assosciated with the scope
   */
  QScope(int scopeID) {
    iterators = new ArrayList();
    this.scopeID = scopeID;
  }

  void setLimit(RuntimeIterator iter) {
    this.limit = iter;
  }

  RuntimeIterator getLimit() {
    return this.limit;
  }

  void bindIterator(RuntimeIterator iterator) {
    //_iterator = iterator;
    iterators.add(iterator);
    iterator.setInternalId("iter" + iterators.size());
  }

  CompiledValue resolve(String name) {
    //System.out.println("in Scope.resolve "+(_iterator != null ?
    // _iterator.getName() : null));
    Iterator iter = iterators.iterator();
    while (iter.hasNext()) {
      RuntimeIterator _iterator = (RuntimeIterator) iter.next();
      if (_iterator != null && name.equals(_iterator.getName()))
          return _iterator;
    }
    return null;
  }

  List getIterators() {
    return iterators;
  }

  void setCurrent(RuntimeIterator iterator, Object obj) {
    iterator.setCurrent(obj);
  }
  
  /**
   * 
   * @return unique int identifying the scope. It also indicates the relative visibility
   * of scopes, with higher scope being able to see iterators of lower scope.
   */
  int getScopeID() {
    return this.scopeID;
  }
}
