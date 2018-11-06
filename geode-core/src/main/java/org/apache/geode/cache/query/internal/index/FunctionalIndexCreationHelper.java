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

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledBindArgument;
import org.apache.geode.cache.query.internal.CompiledComparison;
import org.apache.geode.cache.query.internal.CompiledFunction;
import org.apache.geode.cache.query.internal.CompiledID;
import org.apache.geode.cache.query.internal.CompiledIndexOperation;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.CompiledLiteral;
import org.apache.geode.cache.query.internal.CompiledNegation;
import org.apache.geode.cache.query.internal.CompiledOperation;
import org.apache.geode.cache.query.internal.CompiledPath;
import org.apache.geode.cache.query.internal.CompiledRegion;
import org.apache.geode.cache.query.internal.CompiledUndefined;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.MapIndexable;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;

class FunctionalIndexCreationHelper extends IndexCreationHelper {

  private boolean isMapTypeIndex;

  /**
   * If true means pattern is *, if false & still map type index that means more than 1 specific
   * keys
   */
  private boolean isAllKeys = false;

  ExecutionContext context = null;

  private CompiledValue indexedExpr;

  private List fromClauseIterators;

  QRegion region;

  String[] multiIndexKeysPattern;

  Object[] mapKeys;

  /**
   * The Iterators for index creation are different then those which are used for index updates as
   * in case of Index creation the 0th iterator is modified such that it always represents
   * collection of Region.Entry objects. As a result all the rest of iterators as well as indexed
   * expression have to be modified to appropriately resolve the dependency on 0th iterator.The
   * missing link indicates the dependency. The original 0th iterator is evaluated as additional
   * projection attribute. These changes provide significant improvement in Index creation as
   * compared to previous method. In this approach the IMQ acts on all the entries of the region
   * while in previous , it iterated over the individual entry of the Region & applied IMQ to it.
   */
  List indexInitIterators = null;

  CompiledValue missingLink = null;

  CompiledValue additionalProj = null;

  ObjectType addnlProjType = null;

  CompiledValue modifiedIndexExpr = null;

  boolean isFirstIteratorRegionEntry = false;

  boolean isFirstIteratorRegionKey = false;

  private final String imports;

  // TODO: Remove the fromClause being passed as parameter to the constructor
  FunctionalIndexCreationHelper(String fromClause, String indexedExpression,
      String projectionAttributes, String imports, InternalCache cache,
      ExecutionContext externalContext, IndexManager imgr) throws IndexInvalidException {
    super(fromClause, projectionAttributes, cache);
    if (externalContext == null) {
      this.context = new ExecutionContext(null, cache);
    } else {
      this.context = externalContext;
    }
    this.context.newScope(1);
    this.imports = imports;
    prepareFromClause(imgr);
    prepareIndexExpression(indexedExpression);
    prepareProjectionAttributes(projectionAttributes);
    Object[] data = modifyIterDefToSuiteIMQ((CompiledIteratorDef) this.fromClauseIterators.get(0));
    if (data[0] == null || data[1] == null) {
      throw new IndexInvalidException(
          String.format("Invalid FROM Clause : ' %s '",
              fromClause));
    }
    this.fromClauseIterators.remove(0);
    this.fromClauseIterators.add(0, data[1]);
    this.region = (QRegion) data[0];
  }

  @Override
  public List getIterators() {
    return this.fromClauseIterators;
  }

  @Override
  public CompiledValue getCompiledIndexedExpression() {
    return this.indexedExpr;
  }

  @Override
  public Region getRegion() {
    return this.region.getRegion();
  }

  @Override
  boolean isMapTypeIndex() {
    return this.isMapTypeIndex;
  }

  boolean isAllKeys() {
    return this.isAllKeys;
  }

  /**
   * The function is modified to optimize the index creation code. If the 0th iterator of from
   * clause is not on Entries, then the 0th iterator is replaced with that of entries & the value
   * corresponding to original iterator is derived from the 0th iterator as additional projection
   * attribute. All the other iterators & index expression if were dependent on 0th iterator are
   * also appropriately modified such that they are correctly derived on the modified 0th iterator.
   * <p>
   * TODO: method is too complex for IDE to analyze -- refactor prepareFromClause
   */
  private void prepareFromClause(IndexManager imgr) throws IndexInvalidException {
    if (this.imports != null) {
      this.compiler.compileImports(this.imports);
    }
    List list = this.compiler.compileFromClause(this.fromClause);

    if (list == null) {
      throw new IndexInvalidException(
          String.format("Invalid FROM Clause : ' %s '",
              this.fromClause));
    }

    int size = list.size();
    this.canonicalizedIteratorNames = new String[size];
    this.canonicalizedIteratorDefinitions = new String[size];
    StringBuilder tempBuff = new StringBuilder();
    boolean isFromClauseNull = true;

    try {
      PartitionedRegion pr = this.context.getPartitionedRegion();
      for (int i = 0; i < size; i++) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) list.get(i);
        iterDef.computeDependencies(this.context);
        RuntimeIterator rIter = iterDef.getRuntimeIterator(this.context);
        this.context.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
        this.context.bindIterator(rIter);
        if (i != 0 && !iterDef.isDependentOnCurrentScope(this.context)) {
          throw new IndexInvalidException(
              String.format(
                  "Invalid FROM Clause : ' %s '; subsequent iterator expressions in from clause must be dependent on previous iterators",
                  this.fromClause));
        }

        String definition = rIter.getDefinition();
        this.canonicalizedIteratorDefinitions[i] = definition;

        // Bind the Index_Internal_ID to the RuntimeIterator
        this.canonicalizedIteratorNames[i] = imgr.putCanonicalizedIteratorNameIfAbsent(definition);

        if (pr != null) {
          this.canonicalizedIteratorNames[i] =
              pr.getIndexManager().putCanonicalizedIteratorNameIfAbsent(definition);
        } else {
          this.canonicalizedIteratorNames[i] =
              imgr.putCanonicalizedIteratorNameIfAbsent(definition);
        }

        rIter.setIndexInternalID(this.canonicalizedIteratorNames[i]);
        tempBuff.append(definition).append(' ').append(this.canonicalizedIteratorNames[i])
            .append(", ");
        isFromClauseNull = false;
        CompiledIteratorDef newItr;

        if (i == 0) {
          CompiledValue cv = iterDef.getCollectionExpr();
          this.addnlProjType = rIter.getElementType();
          String name = iterDef.getName();
          if (isEmpty(name)) {
            // In case the name of iterator is null or blank set it to index_internal_id
            name = this.canonicalizedIteratorNames[i];
          }
          CompiledValue newCollExpr = new CompiledPath(new CompiledBindArgument(1), "entries");

          // TODO: What if cv is not an instance of CompiledRegion
          if (cv instanceof CompiledRegion) {
            this.missingLink = new CompiledPath(new CompiledID(name), "value");
            this.additionalProj = this.missingLink;

          } else if (cv instanceof CompiledOperation || cv instanceof CompiledPath
              || cv instanceof CompiledIndexOperation) {
            CompiledValue prevCV;
            List reconstruct = new ArrayList();
            while (!(cv instanceof CompiledRegion)) {
              prevCV = cv;
              if (cv instanceof CompiledOperation) {
                reconstruct.add(0, ((CompiledOperation) cv).getArguments());
                reconstruct.add(0, ((CompiledOperation) cv).getMethodName());
                cv = ((CompiledOperation) cv).getReceiver(this.context);
              } else if (cv instanceof CompiledPath) {
                reconstruct.add(0, ((CompiledPath) cv).getTailID());
                cv = cv.getReceiver();
              } else if (cv instanceof CompiledIndexOperation) {
                reconstruct.add(0, ((CompiledIndexOperation) cv).getExpression());
                cv = cv.getReceiver();
              } else {
                throw new IndexInvalidException(
                    "FunctionalIndexCreationHelper::prepareFromClause:From clause is neither a CompiledPath nor CompiledOperation");
              }
              reconstruct.add(0, prevCV.getType());
            }

            int firstTokenType = (Integer) reconstruct.get(0);
            if (firstTokenType == CompiledValue.PATH) {
              String tailID = (String) reconstruct.get(1);

              if (tailID.equals("asList") || tailID.equals("asSet") || tailID.equals("values")
                  || tailID.equals("toArray") || tailID.equals("getValues")) {
                this.missingLink = new CompiledPath(new CompiledID(name), "value");
              } else if (tailID.equals("keys") || tailID.equals("getKeys")
                  || tailID.equals("keySet")) {
                this.missingLink = new CompiledPath(new CompiledID(name), "key");
                this.isFirstIteratorRegionKey = true;
              } else if (tailID.equals("entries") || tailID.equals("getEntries")
                  || tailID.equals("entrySet")) {
                this.isFirstIteratorRegionEntry = true;
              } else {
                throw new IndexInvalidException(
                    "FunctionalIndexCreationHelper::prepareFromClause:From clause does not evaluate to valid collection");
              }

              remove(reconstruct, 2, 0);
              int secondTokenType = reconstruct.size() > 1 ? (Integer) reconstruct.get(0) : -1;
              if (!this.isFirstIteratorRegionEntry
                  && secondTokenType == OQLLexerTokenTypes.TOK_LBRACK) {

                // If the field just next to region , is values or getValues & next to it is
                // CompiledIndexOpn, it indirectly means Map operation & we are able to take care of
                // it by adding a flag in CompiledIndexOp which indicates to it whether to return
                // entry or value. But if the field is asList or toArray , we have a problem as we
                // don't have a corresponding list of entries. If the field is keys , an exception
                // should be thrown as IndexOpn on set is not defined.
                if (tailID.equals("values") || tailID.equals("getValues")) {
                  boolean returnEntryForRegionCollection = true;
                  this.additionalProj = new CompiledIndexOperation(new CompiledBindArgument(1),
                      (CompiledValue) reconstruct.get(1), returnEntryForRegionCollection);
                  this.isFirstIteratorRegionEntry = true;

                } else if (tailID.equals("toList") || tailID.equals("toArray")) {
                  // TODO: add support for toList and toArray
                  throw new IndexInvalidException(
                      "FunctionalIndexCreationHelper::prepareFromClause:toList and toArray not supported");

                } else {
                  throw new IndexInvalidException(
                      "FunctionalIndexCreationHelper::prepareFromClause:toList and toArray not supported");
                }
                remove(reconstruct, 2, 0);

              } else if (!this.isFirstIteratorRegionEntry
                  && (secondTokenType == OQLLexerTokenTypes.METHOD_INV
                      || secondTokenType == CompiledValue.PATH)
                  && (tailID.equals("values") || tailID.equals("getValues")
                      || tailID.equals("keySet") || tailID.equals("keys")
                      || tailID.equals("getKeys"))) {

                // Check if the second token name is toList or toArray or asSet.We need to remove
                // those
                String secTokName = (String) reconstruct.get(1);
                if (secTokName.equals("asList") || secTokName.equals("asSet")
                    || secTokName.equals("toArray")) {
                  remove(reconstruct, secondTokenType == OQLLexerTokenTypes.METHOD_INV ? 3 : 2, 0);
                }
              }

            } else if (firstTokenType == OQLLexerTokenTypes.TOK_LBRACK) {
              boolean returnEntryForRegionCollection = true;
              this.additionalProj = new CompiledIndexOperation(new CompiledBindArgument(1),
                  (CompiledValue) reconstruct.get(1), returnEntryForRegionCollection);
              this.isFirstIteratorRegionEntry = true;

            } else if (firstTokenType == OQLLexerTokenTypes.METHOD_INV) {
              String methodName = (String) reconstruct.get(1);
              if (methodName.equals("asList") || methodName.equals("asSet")
                  || methodName.equals("values") || methodName.equals("toArray")
                  || methodName.equals("getValues")) {
                this.missingLink = new CompiledPath(new CompiledID(name), "value");
              } else if (methodName.equals("keys") || methodName.equals("getKeys")
                  || methodName.equals("keySet")) {
                this.missingLink = new CompiledPath(new CompiledID(name), "key");
                this.isFirstIteratorRegionKey = true;
              } else if (methodName.equals("entries") || methodName.equals("getEntries")
                  || methodName.equals("entrySet")) {
                this.isFirstIteratorRegionEntry = true;
                List args = (List) reconstruct.get(2);
                if (args != null && args.size() == 1) {
                  Object obj = args.get(0);
                  if (obj instanceof CompiledBindArgument) {
                    throw new IndexInvalidException(
                        "FunctionalIndexCreationHelper::prepareFromClause:entries method called with CompiledBindArgument");
                  }
                }
              }

              remove(reconstruct, 3, 0);
              int secondTokenType = reconstruct.size() > 1 ? (Integer) reconstruct.get(0) : -1;
              if (!this.isFirstIteratorRegionEntry
                  && secondTokenType == OQLLexerTokenTypes.TOK_LBRACK) {

                if (methodName.equals("values") || methodName.equals("getValues")) {
                  boolean returnEntryForRegionCollection = true;
                  newCollExpr = new CompiledIndexOperation(new CompiledBindArgument(1),
                      (CompiledValue) reconstruct.get(1), returnEntryForRegionCollection);
                } else if (methodName.equals("toList") || methodName.equals("toArray")) {
                  // TODO: add support for toList and toArray
                  throw new IndexInvalidException(
                      "FunctionalIndexCreationHelper::prepareFromClause:toList and toArray not supported yet");
                } else {
                  throw new IndexInvalidException(
                      "FunctionalIndexCreationHelper::prepareFromClause:toList and toArray not supported yet");
                }

                remove(reconstruct, 2, 0);
              } else if (!this.isFirstIteratorRegionEntry
                  && (secondTokenType == OQLLexerTokenTypes.METHOD_INV
                      || secondTokenType == CompiledValue.PATH)
                  && (methodName.equals("values") || methodName.equals("getValues")
                      || methodName.equals("keys") || methodName.equals("getKeys")
                      || methodName.equals("keySet"))) {

                // Check if the second token name is toList or toArray or asSet.We need to remove
                // those
                String secTokName = (String) reconstruct.get(1);
                if (secTokName.equals("asList") || secTokName.equals("asSet")
                    || secTokName.equals("toArray")) {
                  remove(reconstruct, secondTokenType == OQLLexerTokenTypes.METHOD_INV ? 3 : 2, 0);
                }
              }
            }

            if (!this.isFirstIteratorRegionEntry) {
              this.additionalProj = this.missingLink;
              int len = reconstruct.size();
              for (int j = 0; j < len; ++j) {
                Object obj = reconstruct.get(j);
                if (obj instanceof Integer) {
                  int tokenType = (Integer) obj;
                  if (tokenType == CompiledValue.PATH) {
                    this.additionalProj =
                        new CompiledPath(this.additionalProj, (String) reconstruct.get(++j));
                  } else if (tokenType == OQLLexerTokenTypes.TOK_LBRACK) {
                    this.additionalProj = new CompiledIndexOperation(this.additionalProj,
                        (CompiledValue) reconstruct.get(++j));
                  } else if (tokenType == OQLLexerTokenTypes.METHOD_INV) {
                    this.additionalProj = new CompiledOperation(this.additionalProj,
                        (String) reconstruct.get(++j), (List) reconstruct.get(++j));
                  }
                }
              }
            }
          } else {
            throw new IndexInvalidException(
                "FunctionalIndexCreationHelper::prepareFromClause:From clause is neither a CompiledPath nor CompiledOperation");
          }

          if (!this.isFirstIteratorRegionEntry) {
            newItr = new CompiledIteratorDef(name, null, newCollExpr);
            this.indexInitIterators = new ArrayList();
            this.indexInitIterators.add(newItr);
          }

        } else if (!this.isFirstIteratorRegionEntry) {
          newItr = iterDef;
          if (rIter.getDefinition().contains(this.canonicalizedIteratorNames[0])) {
            newItr = (CompiledIteratorDef) getModifiedDependentCompiledValue(this.context, i,
                iterDef, true);
          }
          this.indexInitIterators.add(newItr);
        }
      }
    } catch (IndexInvalidException e) {
      throw e;
    } catch (Exception e) {
      throw new IndexInvalidException(e);
    }
    if (isFromClauseNull)
      throw new IndexInvalidException(
          String.format("Invalid FROM Clause : ' %s '",
              this.fromClause));
    this.fromClause = tempBuff.substring(0, tempBuff.length() - 2);
    this.fromClauseIterators = list;
  }

  /**
   * This function is modified so that if the indexed expression has any dependency on the 0th
   * iterator, then it needs to modified by using the missing link so that it is derivable from the
   * 0th iterator.
   * <p>
   * TODO: refactor large method prepareIndexExpression
   */
  private void prepareIndexExpression(String indexedExpression) throws IndexInvalidException {
    CompiledValue expr = this.compiler.compileQuery(indexedExpression);
    if (expr == null) {
      throw new IndexInvalidException(
          String.format("Invalid indexed expression : ' %s '",
              indexedExpression));
    }

    if (expr instanceof CompiledUndefined || expr instanceof CompiledLiteral
        || expr instanceof CompiledComparison || expr instanceof CompiledBindArgument
        || expr instanceof CompiledNegation) {
      throw new IndexInvalidException(
          String.format("Invalid indexed expression : ' %s '",
              indexedExpression));
    }

    try {
      StringBuilder sb = new StringBuilder();
      if (expr instanceof MapIndexable) {
        MapIndexable mi = (MapIndexable) expr;
        List<CompiledValue> indexingKeys = mi.getIndexingKeys();

        if (indexingKeys.size() == 1 && indexingKeys.get(0) == CompiledValue.MAP_INDEX_ALL_KEYS) {
          this.isMapTypeIndex = true;
          this.isAllKeys = true;
          // Strip the index operator
          expr = mi.getReceiverSansIndexArgs();
          expr.generateCanonicalizedExpression(sb, this.context);
          sb.append('[').append('*').append(']');

        } else if (indexingKeys.size() == 1) {
          expr.generateCanonicalizedExpression(sb, this.context);

        } else {
          this.isMapTypeIndex = true;
          this.multiIndexKeysPattern = new String[indexingKeys.size()];
          this.mapKeys = new Object[indexingKeys.size()];
          expr = mi.getReceiverSansIndexArgs();
          expr.generateCanonicalizedExpression(sb, this.context);
          sb.append('[');
          String prefixStr = sb.toString();
          StringBuilder sb2 = new StringBuilder();

          int size = indexingKeys.size();
          for (int j = 0; j < size; ++j) {
            CompiledValue cv = indexingKeys.get(size - j - 1);
            this.mapKeys[size - j - 1] = cv.evaluate(this.context);
            StringBuilder sb3 = new StringBuilder();
            cv.generateCanonicalizedExpression(sb3, this.context);
            sb3.insert(0, prefixStr);
            sb3.append(']');
            this.multiIndexKeysPattern[j] = sb3.toString();
            cv.generateCanonicalizedExpression(sb2, this.context);
            sb2.insert(0, ',');
          }
          sb2.deleteCharAt(0);
          sb.append(sb2);
          sb.append(']');

        }
      } else {
        expr.generateCanonicalizedExpression(sb, this.context);
      }

      this.indexedExpression = sb.toString();
      this.modifiedIndexExpr = expr;
      if (!this.isFirstIteratorRegionEntry
          && this.indexedExpression.contains(this.canonicalizedIteratorNames[0])) {
        this.modifiedIndexExpr = getModifiedDependentCompiledValue(this.context, -1, expr, true);
      }
    } catch (Exception e) {
      throw new IndexInvalidException(
          String.format("Invalid indexed expression : ' %s '",
              indexedExpression),
          e);
    }
    this.indexedExpr = expr;
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

  private Object[] modifyIterDefToSuiteIMQ(CompiledIteratorDef iterDef)
      throws IndexInvalidException {
    Object[] retValues = {null, null};
    try {
      CompiledValue def = iterDef.getCollectionExpr();
      if (def instanceof CompiledRegion) {
        CompiledBindArgument bindArg = new CompiledBindArgument(1);
        CompiledIteratorDef newDef = new CompiledIteratorDef(iterDef.getName(), null, bindArg);
        retValues[0] = def.evaluate(this.context);
        retValues[1] = newDef;
        return retValues;
      }

      if (def instanceof CompiledPath || def instanceof CompiledOperation
          || def instanceof CompiledIndexOperation) {
        CompiledValue cv = def;
        List reconstruct = new ArrayList();

        while (!(cv instanceof CompiledRegion)) {
          CompiledValue prevCV = cv;
          if (cv instanceof CompiledOperation) {
            reconstruct.add(0, ((CompiledOperation) cv).getArguments());
            reconstruct.add(0, ((CompiledOperation) cv).getMethodName());
            cv = ((CompiledOperation) cv).getReceiver(this.context);
          } else if (cv instanceof CompiledPath) {
            reconstruct.add(0, ((CompiledPath) cv).getTailID());
            cv = ((CompiledPath) cv).getReceiver();
          } else if (cv instanceof CompiledIndexOperation) {
            reconstruct.add(0, ((CompiledIndexOperation) cv).getExpression());
            cv = ((CompiledIndexOperation) cv).getReceiver();
          } else {
            throw new IndexInvalidException(
                "FunctionalIndexCreationHelper::prepareFromClause:From clause is neither a CompiledPath nor CompiledOperation");
          }
          reconstruct.add(0, prevCV.getType());
        }

        CompiledValue v = cv;
        cv = new CompiledBindArgument(1);
        int len = reconstruct.size();
        for (int j = 0; j < len; ++j) {
          Object obj = reconstruct.get(j);
          if (obj instanceof Integer) {
            int tokenType = (Integer) obj;
            if (tokenType == CompiledValue.PATH) {
              cv = new CompiledPath(cv, (String) reconstruct.get(++j));
            } else if (tokenType == OQLLexerTokenTypes.TOK_LBRACK) {
              cv = new CompiledIndexOperation(cv, (CompiledValue) reconstruct.get(++j));
            } else if (tokenType == OQLLexerTokenTypes.METHOD_INV) {
              cv = new CompiledOperation(cv, (String) reconstruct.get(++j),
                  (List) reconstruct.get(++j));
            }
          }
        }

        CompiledIteratorDef newDef = new CompiledIteratorDef(iterDef.getName(), null, cv);
        retValues[0] = v.evaluate(this.context);
        retValues[1] = newDef;
        return retValues;
      }
    } catch (Exception e) {
      throw new IndexInvalidException(e);
    }
    return retValues;
  }

  /**
   * This function is used to correct the complied value's dependency , in case the compiledValue is
   * dependent on the 0th RuntimeIterator in some way. Thus the dependent compiled value is prefixed
   * with the missing link so that it is derivable from the 0th iterator.
   */
  private CompiledValue getModifiedDependentCompiledValue(ExecutionContext context, int currItrID,
      CompiledValue cv, boolean isDependent)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {

    if (cv instanceof CompiledIteratorDef) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) cv;
      RuntimeIterator rItr = (RuntimeIterator) context.getCurrentIterators().get(currItrID);
      String canonFrmClause = rItr.getDefinition();

      // TODO: original value of isDependent is always ignored
      isDependent = canonFrmClause.startsWith(this.canonicalizedIteratorNames[0]);

      return new CompiledIteratorDef(iterDef.getName(), rItr.getElementType(),
          getModifiedDependentCompiledValue(context, currItrID, iterDef.getCollectionExpr(),
              isDependent));

    } else if (cv instanceof CompiledPath) {
      CompiledPath path = (CompiledPath) cv;
      return new CompiledPath(
          getModifiedDependentCompiledValue(context, currItrID, path.getReceiver(), isDependent),
          path.getTailID());

    } else if (cv instanceof CompiledOperation) {
      CompiledOperation oper = (CompiledOperation) cv;
      List list = oper.getArguments();
      List newList = new ArrayList();
      for (Object aList : list) {
        CompiledValue cv1 = (CompiledValue) aList;
        StringBuilder sb = new StringBuilder();
        cv1.generateCanonicalizedExpression(sb, context);
        if (sb.toString().startsWith(this.canonicalizedIteratorNames[0])) {
          newList.add(getModifiedDependentCompiledValue(context, currItrID, cv1, true));
        } else {
          newList.add(getModifiedDependentCompiledValue(context, currItrID, cv1, false));
        }
      }

      // What if the receiver is null?
      CompiledValue rec = oper.getReceiver(context);
      if (rec == null) {
        if (isDependent) {
          rec = this.missingLink;
        }
        return new CompiledOperation(rec, oper.getMethodName(), newList);
      } else {
        return new CompiledOperation(
            getModifiedDependentCompiledValue(context, currItrID, rec, isDependent),
            oper.getMethodName(), newList);
      }

    } else if (cv instanceof CompiledFunction) {
      CompiledFunction cf = (CompiledFunction) cv;
      CompiledValue[] cvArray = cf.getArguments();
      int function = cf.getFunction();
      int len = cvArray.length;
      CompiledValue[] newCvArray = new CompiledValue[len];
      for (int i = 0; i < len; ++i) {
        CompiledValue cv1 = cvArray[i];
        StringBuilder sb = new StringBuilder();
        cv1.generateCanonicalizedExpression(sb, context);
        if (sb.toString().startsWith(this.canonicalizedIteratorNames[0])) {
          newCvArray[i] = getModifiedDependentCompiledValue(context, currItrID, cv1, true);
        } else {
          newCvArray[i] = getModifiedDependentCompiledValue(context, currItrID, cv1, false);
        }
      }
      return new CompiledFunction(newCvArray, function);

    } else if (cv instanceof CompiledID) {
      CompiledID id = (CompiledID) cv;
      RuntimeIterator rItr0 = (RuntimeIterator) context.getCurrentIterators().get(0);
      if (isDependent) {
        String name;
        if ((name = rItr0.getName()) != null && name.equals(id.getId())) {
          // The CompiledID is a RuneTimeIterator & so it needs to be replaced by the missing link
          return this.missingLink;
        } else {
          // The compiledID is a compiledPath
          return new CompiledPath(this.missingLink, id.getId());
        }
      } else {
        return cv;
      }

    } else if (cv instanceof CompiledIndexOperation) {
      CompiledIndexOperation co = (CompiledIndexOperation) cv;
      CompiledValue cv1 = co.getExpression();
      StringBuilder sb = new StringBuilder();
      cv1.generateCanonicalizedExpression(sb, context);
      if (sb.toString().startsWith(this.canonicalizedIteratorNames[0])) {
        cv1 = getModifiedDependentCompiledValue(context, currItrID, cv1, true);
      } else {
        cv1 = getModifiedDependentCompiledValue(context, currItrID, cv1, false);
      }
      return new CompiledIndexOperation(
          getModifiedDependentCompiledValue(context, currItrID, co.getReceiver(), isDependent),
          cv1);
    } else {
      return cv;
    }
  }

  private void remove(List list, int count, int index) {
    for (int j = 0; j < count; ++j) {
      list.remove(index);
    }
  }
}
