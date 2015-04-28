/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledID.java,v 1.2 2005/02/01 17:19:20 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;


/**
 * Class Description
 *
 * @version     $Revision: 1.2 $
 * @author      ericz
 */



public class CompiledID extends AbstractCompiledValue {
  private String _id;
  
  
  public CompiledID(String id) {
    _id = id;
  }
  
  @Override
  public List getPathOnIterator(RuntimeIterator itr, ExecutionContext context)
  throws TypeMismatchException, AmbiguousNameException {
    CompiledValue val = context.resolve(getId());
    if (val == itr)
      return new ArrayList(); // empty path
    if (val.getType() == PATH && ((CompiledPath)val).getReceiver() == itr) {
      List list = new ArrayList();
      list.add(_id);
      return list;
    }
    return null;
  }
  
  public String getId() {
    return _id;
  }
  
  
  
  public int getType() {
    return Identifier;
  }
  
  @Override
  public Set  computeDependencies(ExecutionContext context)
  throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    CompiledValue v = context.resolve(getId());
    return context.addDependencies(this, v.computeDependencies(context));
  }
  
  public Object evaluate(ExecutionContext context)
  throws FunctionDomainException, TypeMismatchException, NameResolutionException,
          QueryInvocationTargetException {
    CompiledValue v = context.resolve(getId());
    Object obj = v.evaluate(context);
    // check for BucketRegion substitution
    PartitionedRegion pr = context.getPartitionedRegion();
    if (pr != null && (obj instanceof Region)) {
      if (pr.getFullPath().equals(((Region)obj).getFullPath())) {
        obj = context.getBucketRegion();
      }
    }
    return obj;
  }
  
  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer, ExecutionContext context)
  throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    // The compiled ID can be an iterator variable or it can be a path variable.
    // So first resolve the type of variable using ExecutionContext
    // A compiledID will get resolved either to a RunTimeIterator or a CompiledPath
    context.resolve(_id).generateCanonicalizedExpression(clauseBuffer, context);
    
  }
  
  
}
