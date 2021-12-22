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
package org.apache.geode.cache.query.internal.index;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;

public class PrimaryKeyIndexCreationHelper extends IndexCreationHelper {

  ExecutionContext context = null;

  final Region region;

  public PrimaryKeyIndexCreationHelper(String fromClause, String indexedExpression,
      String projectionAttributes, InternalCache cache, ExecutionContext externalContext,
      IndexManager imgr) throws IndexInvalidException {
    super(fromClause, projectionAttributes, cache);
    if (externalContext == null) {
      context = new ExecutionContext(null, cache);
    } else {
      context = externalContext;
    }
    context.newScope(1);
    region = imgr.region;
    prepareFromClause(imgr);
    prepareIndexExpression(indexedExpression);
    prepareProjectionAttributes(projectionAttributes);
  }

  private void prepareFromClause(IndexManager imgr) throws IndexInvalidException {
    List list = compiler.compileFromClause(fromClause);
    if (list.size() > 1) {
      throw new IndexInvalidException(
          "The fromClause for a Primary Key index should only have one iterator and the collection must be a Region Path only");
    }
    try {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) list.get(0);
      if (iterDef.getCollectionExpr().getType() != OQLLexerTokenTypes.RegionPath) {
        throw new IndexInvalidException(
            "The fromClause for a Primary Key index should be a Region Path only");
      }
      iterDef.computeDependencies(context);
      RuntimeIterator rIter = (iterDef.getRuntimeIterator(context));
      String definition = rIter.getDefinition();
      canonicalizedIteratorDefinitions = new String[1];
      canonicalizedIteratorDefinitions[0] = definition;
      // Bind the Index_Internal_ID to the RuntimeIterator
      PartitionedRegion pr = context.getPartitionedRegion();
      canonicalizedIteratorNames = new String[1];
      String name = null;
      if (pr != null) {
        name = pr.getIndexManager().putCanonicalizedIteratorNameIfAbsent(definition);
      } else {
        name = imgr.putCanonicalizedIteratorNameIfAbsent(definition);
      }
      rIter.setIndexInternalID(name);
      canonicalizedIteratorNames = new String[1];
      canonicalizedIteratorNames[0] = name;
      fromClause = definition + ' ' + name;
      context.bindIterator(rIter);
    } catch (IndexInvalidException e) {
      throw e; // propagate
    } catch (Exception e) {
      throw new IndexInvalidException(e); // wrap any other exceptions
    }
  }

  private void prepareIndexExpression(String indexedExpression) throws IndexInvalidException {
    List indexedExprs = compiler.compileProjectionAttributes(indexedExpression);
    if (indexedExprs == null || indexedExprs.size() != 1) {
      throw new IndexInvalidException(
          String.format("Invalid indexed expressoion : ' %s '",
              indexedExpression));
    }
    CompiledValue expr = (CompiledValue) ((Object[]) indexedExprs.get(0))[1];
    if (expr.getType() == CompiledValue.LITERAL) {
      throw new IndexInvalidException(
          String.format("Invalid indexed expressoion : ' %s '",
              indexedExpression));
    }
    try {
      StringBuilder sb = new StringBuilder();
      expr.generateCanonicalizedExpression(sb, context);
      this.indexedExpression = sb.toString();
    } catch (Exception e) {
      throw new IndexInvalidException(
          String.format("Invalid indexed expressoion : ' %s ' %s",
              indexedExpression, e.getMessage()));
    }
  }

  private void prepareProjectionAttributes(String projectionAttributes)
      throws IndexInvalidException {
    if (projectionAttributes != null && !projectionAttributes.equals("*")) {
      throw new IndexInvalidException(
          String.format("Invalid projection attributes : ' %s '",
              projectionAttributes));
    }
    this.projectionAttributes = projectionAttributes;
  }

  @Override
  public Region getRegion() {
    return region;
  }

  @Override
  public List getIterators() {
    return null;
  }

  @Override
  public CompiledValue getCompiledIndexedExpression() {
    return null;
  }

}
