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

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.pdx.internal.PdxString;

/**
 * Class Description
 *
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
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {

    // When compiling a new query, a context is created where there are no bind arguments at this
    // point
    if (context.isBindArgsSet()) {
      Object bindArgumentValue = context.getBindArgument(this.index);
      if (bindArgumentValue instanceof Region) {
        clauseBuffer.insert(0, ((Region) bindArgumentValue).getFullPath());
      } else if (bindArgumentValue instanceof String) {
        clauseBuffer.insert(0, '\'').insert(0, bindArgumentValue).insert(0, '\'');
      } else {
        super.generateCanonicalizedExpression(clauseBuffer, context);
      }
    }

  }

  public Object evaluate(ExecutionContext context) {
    if (!context.isBindArgsSet()) {
      return null;
    }
    Object obj = context.getBindArgument(this.index);
    // check for BucketRegion substitution
    if (obj instanceof Region) {
      PartitionedRegion pr = context.getPartitionedRegion();
      if (pr != null) {
        if (pr.getFullPath().equals(((Region) obj).getFullPath())) {
          obj = context.getBucketRegion();
        }
      }
    }
    return obj;
  }

  /*
   * provided just the bind parameters, we can evaluate if the expected parameter is all that is
   * needed. For example a bound limit variable
   */
  public Object evaluate(Object[] bindArguments) {
    if (index > bindArguments.length) {
      throw new IllegalArgumentException(
          "Too few query parameters");
    }
    return bindArguments[index - 1];
  }

  @Override
  public void getRegionsInQuery(Set regionsInQuery, Object[] parameters) {
    Object v = parameters[this.index - 1];
    if (v instanceof Region) {
      regionsInQuery.add(((Region) v).getFullPath());
    }
  }

  public PdxString getSavedPdxString(ExecutionContext context) {
    return context.getSavedPdxString(this.index);
  }
}
