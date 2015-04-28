/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledBindArgument.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * Class Description
 *
 * @author      ezoerner
 */


public class CompiledBindArgument extends AbstractCompiledValue {
  private int index; // one-based

    public CompiledBindArgument(int index) {
      this.index = index;
    }

  public int getType() {
    return QUERY_PARAM;
  }

  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer,
      ExecutionContext context) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException {
    Object rgn;
    if ((rgn = context.getBindArgument(this.index)) instanceof Region) {
      clauseBuffer.insert(0, ((Region)rgn).getFullPath());
    }
    else {
      super.generateCanonicalizedExpression(clauseBuffer, context);
    }
  }
    
    public Object evaluate(ExecutionContext context) {
      Object obj = context.getBindArgument(this.index);
      // check for BucketRegion substitution
      if (obj instanceof Region) {
        PartitionedRegion pr = context.getPartitionedRegion();
        if (pr != null) {
          if (pr.getFullPath().equals(((Region)obj).getFullPath())) {
            obj = context.getBucketRegion();
          }
        }
      }
      return obj;
    }
    
    /*
     * provided just the bind parameters, we can evaluate if the expected
     * parameter is all that is needed.  For example a bound limit variable
     */
    public Object evaluate(Object[] bindArguments) {
        if (index > bindArguments.length) {
            throw new IllegalArgumentException(LocalizedStrings.ExecutionContext_TOO_FEW_QUERY_PARAMETERS.toLocalizedString());
        }
        return bindArguments[index - 1];
    }
    
    @Override
    public void getRegionsInQuery(Set regionsInQuery, Object[] parameters) {
      Object v = parameters[this.index - 1];
      if (v instanceof Region) {
        regionsInQuery.add(((Region)v).getFullPath());
      }
    }
    
    public PdxString getSavedPdxString(ExecutionContext context){
      return context.getSavedPdxString(this.index);
    }
}
