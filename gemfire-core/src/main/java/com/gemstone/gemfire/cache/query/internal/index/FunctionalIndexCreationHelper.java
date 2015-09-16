/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexCreationHelper.java
 *
 * Created on March 16, 2005, 6:20 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.CompiledBindArgument;
import com.gemstone.gemfire.cache.query.internal.CompiledComparison;
import com.gemstone.gemfire.cache.query.internal.CompiledFunction;
import com.gemstone.gemfire.cache.query.internal.CompiledID;
import com.gemstone.gemfire.cache.query.internal.CompiledIndexOperation;
import com.gemstone.gemfire.cache.query.internal.CompiledIteratorDef;
import com.gemstone.gemfire.cache.query.internal.CompiledLiteral;
import com.gemstone.gemfire.cache.query.internal.CompiledNegation;
import com.gemstone.gemfire.cache.query.internal.CompiledOperation;
import com.gemstone.gemfire.cache.query.internal.CompiledPath;
import com.gemstone.gemfire.cache.query.internal.CompiledRegion;
import com.gemstone.gemfire.cache.query.internal.CompiledUndefined;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.MapIndexable;
import com.gemstone.gemfire.cache.query.internal.QRegion;
import com.gemstone.gemfire.cache.query.internal.RuntimeIterator;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author asif
 * @author vaibhav
 *
 */
class FunctionalIndexCreationHelper extends IndexCreationHelper {
  private boolean isMapTypeIndex ; 
  //If true means pattern is *, if false & still map type index that means 
  // more than 1 specific keys
  private boolean isAllKeys = false;
  
  ExecutionContext context = null;
  CompiledValue indexedExpr;
  List fromClauseIterators;
  QRegion region;
  String []  multiIndexKeysPattern ;
  Object [] mapKeys;
  /**
   * Asif : The Iterators for index creation are different then those which are
   * used for index updates as in case of Index creation the 0th iterator is
   * modified such that it always represents collection of Region.Entry objects.
   * As a result all the rest of iterators as well as indexed expression have to
   * be modified to appropriately resolve the dependency on 0th iterator.The
   * missing link indicates the dependency. The original 0th iterator is
   * evaluated as additional projection attribute. These changes provide
   * significant improvement in Index creation as compared to previous method.
   * In this approach the IMQ acts on all the entries of the region while in
   * previous , it iterated over the individual entry of the Region & applied
   * IMQ to it.
   */
  
  List indexInitIterators = null;
  CompiledValue missingLink = null;
  CompiledValue additionalProj = null;
  ObjectType addnlProjType = null;
  CompiledValue modifiedIndexExpr = null;
  boolean isFirstIteratorRegionEntry = false;
  boolean isFirstIteratorRegionKey = false;
  final String imports;

  //TODO: Asif Remove the fromClause being passed as parameter to the
  // constructor
  FunctionalIndexCreationHelper(String fromClause, String indexedExpression,
      String projectionAttributes, String imports, Cache cache, ExecutionContext externalContext, 
      IndexManager imgr)
      throws IndexInvalidException {
    super(fromClause, projectionAttributes, cache);
    if( externalContext == null) {
      this.context = new ExecutionContext(null, cache);
    } else {
      this.context = externalContext;
    }
    this.context.newScope(1);
    this.imports = imports;
    prepareFromClause(imgr);
    prepareIndexExpression(indexedExpression);
    prepareProjectionAttributes(projectionAttributes);
    Object data[] = modfiyIterDefToSuiteIMQ((CompiledIteratorDef) fromClauseIterators
        .get(0));
    if (data[0] == null || data[1] == null) {
      throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_FROM_CLAUSE_0.toLocalizedString(fromClause));
    }
    fromClauseIterators.remove(0);
    fromClauseIterators.add(0, data[1]);
    region = (QRegion) data[0];
  }

  public List getIterators() {
    return fromClauseIterators;
  }

  public CompiledValue getCompiledIndexedExpression() {
    return indexedExpr;
  }

  public Region getRegion() {
    return region.getRegion();
  }
  
  @Override
  boolean isMapTypeIndex() {
    return this.isMapTypeIndex; 
  } 
  
  boolean isAllKeys() {
    return this.isAllKeys;
  }
  
 
  
  /*
   * Asif : The function is modified to optmize the index creation code. If the
   * 0th iterator of from clause is not on Entries, then the 0th iterator is
   * replaced with that of entries & the value corresponding to original
   * iterator is derived from the 0th iterator as additional projection
   * attribute. All the other iterators & index expression if were dependent on
   * 0th iterator are also appropriately modified such that they are correctly
   * derived on the modified 0th iterator.
   */
  private void prepareFromClause(IndexManager imgr)
      throws IndexInvalidException {
    if( imports != null) {
      this.compiler.compileImports(this.imports);
    }
    List list = this.compiler.compileFromClause(fromClause);
     
    if (list == null ) { throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_FROM_CLAUSE_0.toLocalizedString(fromClause)); }
    
    boolean isFromClauseNull = true;
    int size = list.size();
    this.canonicalizedIteratorNames = new String[size];
    this.canonicalizedIteratorDefinitions = new String[size];
    CompiledIteratorDef newItr = null;
    StringBuffer tempBuff = new StringBuffer();
    try {
      PartitionedRegion pr = this.context.getPartitionedRegion();
      for (int i = 0; i < size; i++) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) list.get(i);
        iterDef.computeDependencies(this.context);
        RuntimeIterator rIter = iterDef.getRuntimeIterator(this.context);      
        context.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
        this.context.bindIterator(rIter);
        if (i != 0 && !iterDef.isDependentOnCurrentScope(this.context)) { throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_FROM_CLAUSE_0_SUBSEQUENT_ITERATOR_EXPRESSIONS_IN_FROM_CLAUSE_MUST_BE_DEPENDENT_ON_PREVIOUS_ITERATORS.toLocalizedString(fromClause)); }
        String definition = rIter.getDefinition();
        this.canonicalizedIteratorDefinitions[i] = definition;
        //      Asif: Bind the Index_Internal_ID to the RuntimeIterator
        this.canonicalizedIteratorNames[i] = imgr.putCanonicalizedIteratorNameIfAbsent(definition);
      
        if (pr != null) {
          //if (iterDef.getCollectionExpr() instanceof CompiledRegion || iterDef.getCollectionExpr() instanceof CompiledPath) {
          //  pr.getIndexManager().putCanonicalizedIteratorName(pr.getFullPath(), this.canonicalizedIteratorNames[i]);
          //} else {
          this.canonicalizedIteratorNames[i] = pr.getIndexManager().putCanonicalizedIteratorNameIfAbsent(definition);
          //}
        } else {
          this.canonicalizedIteratorNames[i] = imgr.putCanonicalizedIteratorNameIfAbsent(definition);
        }
 
        rIter.setIndexInternalID(this.canonicalizedIteratorNames[i]);
        tempBuff.append(definition).append(' ').append(
            this.canonicalizedIteratorNames[i]).append(", ");
        isFromClauseNull = false;
        if (i == 0) {
          CompiledValue cv = iterDef.getCollectionExpr();
          addnlProjType = rIter.getElementType();
          String name = null;
          if ((name = iterDef.getName()) == null || name.equals("")) {
            //In case the name of iterator is null or balnk set it to
            // index_internal_id
            name = this.canonicalizedIteratorNames[i];
          }
          CompiledValue newCollExpr = new CompiledPath(
              new CompiledBindArgument(1), "entries");
          //TODO Asif : What if cv is not an instance of CompiledRegion
          if (cv instanceof CompiledRegion) {
            missingLink = new CompiledPath(new CompiledID(name), "value");
            //missingLinkPath = name + ".value";
            additionalProj = missingLink;
          }
          else if (cv instanceof CompiledOperation
              || cv instanceof CompiledPath
              || cv instanceof CompiledIndexOperation) {
            CompiledValue prevCV = null;
            List reconstruct = new ArrayList();
            while (!(cv instanceof CompiledRegion)) {
              prevCV = cv;
              if (cv instanceof CompiledOperation) {
                reconstruct.add(0, ((CompiledOperation) cv).getArguments());
                reconstruct.add(0, ((CompiledOperation) cv).getMethodName());
                cv = ((CompiledOperation) cv).getReceiver(context);
              }
              else if (cv instanceof CompiledPath) {
                reconstruct.add(0, ((CompiledPath) cv).getTailID());
                cv = ((CompiledPath) cv).getReceiver();
              }
              else if (cv instanceof CompiledIndexOperation) {
                reconstruct.add(0, ((CompiledIndexOperation) cv)
                    .getExpression());
                cv = ((CompiledIndexOperation) cv).getReceiver();
              }
              else {
                throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEFROM_CLAUSE_IS_NEITHER_A_COMPILEDPATH_NOR_COMPILEDOPERATION.toLocalizedString());
              }
              reconstruct.add(0, Integer.valueOf(prevCV.getType()));
            }
            int firstTokenType = ((Integer) reconstruct.get(0)).intValue();
            if (firstTokenType == CompiledValue.PATH) {
              //            CompiledPath cp = (CompiledPath) reconstruct.get(1);
              String tailID = (String) reconstruct.get(1);
              if (tailID.equals("asList") || tailID.equals("asSet")
                  || tailID.equals("values") || tailID.equals("toArray")
                  || tailID.equals("getValues")) {
                missingLink = new CompiledPath(new CompiledID(name), "value");
                //  missingLinkPath = name + ".value";
              }
              else if (tailID.equals("keys") || tailID.equals("getKeys") ||  tailID.equals("keySet")) {
                missingLink = new CompiledPath(new CompiledID(name), "key");
                isFirstIteratorRegionKey = true;
                //missingLinkPath = name + ".key";
              }
              else if (tailID.equals("entries") || tailID.equals("getEntries")|| tailID.equals("entrySet")) {
                isFirstIteratorRegionEntry = true;
              }
              else {
                throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEFROM_CLAUSE_DOES_NOT_EVALUATE_TO_VALID_COLLECTION.toLocalizedString());
              }
              remove(reconstruct, 2, 0);
              int secondTokenType = (reconstruct.size() > 1) ? ((Integer) reconstruct
                  .get(0)).intValue()
                  : -1;
              if (!isFirstIteratorRegionEntry
                  && (secondTokenType == OQLLexerTokenTypes.TOK_LBRACK)) {
                //Asif: If the field just next to region , is values or
                // getValues & next to it is
                // CompiledIndexOpn, it indirectly means Map operation & we are
                //able to take care of it by adding a flag in CompiledIndexOp
                // which
                // indicates to it whether to return entry or value.But if the
                // field
                //is asList or toArray , we have a problem as we don't have a
                // corresponding
                //list of entries. If the field is keys , an exception should
                // be thrown
                //as IndexOpn on set is not defined.
                if (tailID.equals("values") || tailID.equals("getValues")) {
                  boolean returnEntryForRegionCollection = true;
                  additionalProj = new CompiledIndexOperation(
                      new CompiledBindArgument(1), (CompiledValue) reconstruct
                          .get(1), returnEntryForRegionCollection);
                  this.isFirstIteratorRegionEntry = true;
                }
                else if (tailID.equals("toList") || tailID.equals("toArray")) {
                  //TODO:Asif . This needs to be supported
                  throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSETOLIST_TOARRAY_NOT_SUPPORTED.toLocalizedString());
                }
                else {
                  throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSETOLIST_TOARRAY_NOT_SUPPORTED.toLocalizedString());
                }
                remove(reconstruct, 2, 0);
              }
              else if (!isFirstIteratorRegionEntry
                  && (secondTokenType == OQLLexerTokenTypes.METHOD_INV || secondTokenType == CompiledValue.PATH)
                  && (tailID.equals("values") || tailID.equals("getValues")
                      || tailID.equals("keySet") || tailID.equals("keys") || tailID.equals("getKeys"))) {
                //Asif :Check if the second token name is toList or toArray or
                // asSet.We need to remove those
                String secTokName = (String) reconstruct.get(1);
                if (secTokName.equals("asList") || secTokName.equals("asSet")
                    || secTokName.equals("toArray")) {
                  remove(reconstruct,
                      ((secondTokenType == OQLLexerTokenTypes.METHOD_INV) ? 3
                          : 2), 0);
                }
              }
            }
            else if (firstTokenType == OQLLexerTokenTypes.TOK_LBRACK) {
              boolean returnEntryForRegionCollection = true;
              additionalProj = new CompiledIndexOperation(
                  new CompiledBindArgument(1), (CompiledValue) reconstruct
                      .get(1), returnEntryForRegionCollection);
              this.isFirstIteratorRegionEntry = true;
            }
            else if (firstTokenType == OQLLexerTokenTypes.METHOD_INV) {
              String methodName = (String) reconstruct.get(1);
              if (methodName.equals("asList") || methodName.equals("asSet")
                  || methodName.equals("values")
                  || methodName.equals("toArray")
                  || methodName.equals("getValues")) {
                missingLink = new CompiledPath(new CompiledID(name), "value");
                //missingLinkPath = name + ".value";
              }
              else if (methodName.equals("keys")
                  || methodName.equals("getKeys") || methodName.equals("keySet")) {
                missingLink = new CompiledPath(new CompiledID(name), "key");
                isFirstIteratorRegionKey = true;
                //missingLinkPath = name + ".key";
              }
              else if (methodName.equals("entries")
                  || methodName.equals("getEntries") || methodName.equals("entrySet") ) {
                isFirstIteratorRegionEntry = true;
                List args = (List) reconstruct.get(2);
                if (args != null && args.size() == 1) {
                  Object obj = args.get(0);
                  if (obj instanceof CompiledBindArgument) { throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEENTRIES_METHOD_CALLED_WITH_COMPILEDBINDARGUMENT.toLocalizedString()); }
                }
              }
              remove(reconstruct, 3, 0);
              int secondTokenType = (reconstruct.size() > 1) ? ((Integer) reconstruct
                  .get(0)).intValue()
                  : -1;
              if (!isFirstIteratorRegionEntry
                  && (secondTokenType == OQLLexerTokenTypes.TOK_LBRACK)) {
                if (methodName.equals("values")
                    || methodName.equals("getValues")) {
                  boolean returnEntryForRegionCollection = true;
                  newCollExpr = new CompiledIndexOperation(
                      new CompiledBindArgument(1), (CompiledValue) reconstruct
                          .get(1), returnEntryForRegionCollection);
                }
                else if (methodName.equals("toList")
                    || methodName.equals("toArray")) {
                  //TODO:Asif . This needs to be supported
                  throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSETOLIST_TOARRAY_NOT_SUPPORTED_YET.toLocalizedString());
                }
                else {
                  throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSETOLIST_TOARRAY_NOT_SUPPORTED_YET.toLocalizedString());
                }
                remove(reconstruct, 2, 0);
              }
              else if (!isFirstIteratorRegionEntry
                  && (secondTokenType == OQLLexerTokenTypes.METHOD_INV || secondTokenType == CompiledValue.PATH)
                  && (methodName.equals("values")
                      || methodName.equals("getValues")
                      || methodName.equals("keys") || methodName
                      .equals("getKeys") || methodName.equals("keySet"))) {
                //Asif :Check if the second token name is toList or toArray or
                // asSet.We need to remove those
                String secTokName = (String) reconstruct.get(1);
                if (secTokName.equals("asList") || secTokName.equals("asSet")
                    || secTokName.equals("toArray")) {
                  remove(reconstruct,
                      ((secondTokenType == OQLLexerTokenTypes.METHOD_INV) ? 3
                          : 2), 0);
                }
              }
            }
            if (!isFirstIteratorRegionEntry) {
              additionalProj = missingLink;
              int len = reconstruct.size();
              for (int j = 0; j < len; ++j) {
                Object obj = reconstruct.get(j);
                if (obj instanceof Integer) {
                  int tokenType = ((Integer) obj).intValue();
                  if (tokenType == CompiledValue.PATH) {
                    additionalProj = new CompiledPath(additionalProj,
                        (String) reconstruct.get(++j));
                  }
                  else if (tokenType == OQLLexerTokenTypes.TOK_LBRACK) {
                    additionalProj = new CompiledIndexOperation(additionalProj,
                        (CompiledValue) reconstruct.get(++j));
                  }
                  else if (tokenType == OQLLexerTokenTypes.METHOD_INV) {
                    additionalProj = new CompiledOperation(additionalProj,
                        (String) reconstruct.get(++j), (List) reconstruct
                            .get(++j));
                  }
                }
              }
            }
          }
          else {
            throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEFROM_CLAUSE_IS_NEITHER_A_COMPILEDPATH_NOR_COMPILEDOPERATION.toLocalizedString());
          }
          if (!this.isFirstIteratorRegionEntry) {
            newItr = new CompiledIteratorDef(name, null, newCollExpr);
            this.indexInitIterators = new ArrayList();
            indexInitIterators.add(newItr);
          }
        }
        else if (!this.isFirstIteratorRegionEntry) {
          newItr = iterDef;
          if (rIter.getDefinition().indexOf(this.canonicalizedIteratorNames[0]) != -1) {
            newItr = (CompiledIteratorDef) getModifiedDependentCompiledValue(
                context, i, iterDef, true);
          }
          this.indexInitIterators.add(newItr);
        }
      }
    }
    catch (Exception e) {
      if (e instanceof IndexInvalidException) throw (IndexInvalidException) e;
      throw new IndexInvalidException(e);
    }
    if (isFromClauseNull)
        throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_FROM_CLAUSE_0.toLocalizedString(fromClause));
    this.fromClause = tempBuff.substring(0, tempBuff.length() - 2);
    this.fromClauseIterators = list;
  }

  /*
   * Asif: This fuinction is modified so that if the indexed expression has any
   * dependency on the 0th iterator, then it needs to modified by using the
   * missing link so that it is derivable from the 0th iterator.
   */
  private void prepareIndexExpression(String indexedExpression)
      throws IndexInvalidException {
    CompiledValue expr = this.compiler.compileQuery(indexedExpression);
    //List indexedExprs = this.compiler.compileProjectionAttributes(indexedExpression);
    if (expr == null  ) {
      throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_INDEXED_EXPRESSION_0.toLocalizedString(indexedExpression)); 
    }
    
    if (expr instanceof CompiledUndefined || expr instanceof CompiledLiteral
        || expr instanceof CompiledComparison
        || expr instanceof CompiledBindArgument
        || expr instanceof CompiledNegation)
        throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_INDEXED_EXPRESSION_0.toLocalizedString(indexedExpression));
    try {
      StringBuffer sb = new StringBuffer();
      if(expr instanceof MapIndexable) {
        MapIndexable mi = (MapIndexable)expr;
        //CompiledIndexOperation cio = (CompiledIndexOperation)expr;
        List<CompiledValue> indexingKeys = mi.getIndexingKeys();
        if( indexingKeys.size() == 1 && indexingKeys.get(0) == CompiledValue.MAP_INDEX_ALL_KEYS) {
          this.isMapTypeIndex = true;
          this.isAllKeys = true;
          //Strip the index operator
          expr = mi.getRecieverSansIndexArgs();
          expr.generateCanonicalizedExpression(sb, context);
          sb.append('[').append('*').append(']');          
          
        }else if(indexingKeys.size() == 1) {
          expr.generateCanonicalizedExpression(sb, context);
        }else {
          this.isMapTypeIndex = true;
          this.multiIndexKeysPattern = new String[indexingKeys.size()];
          this.mapKeys = new Object[indexingKeys.size()];
          expr = mi.getRecieverSansIndexArgs();
          expr.generateCanonicalizedExpression(sb, context);
          sb.append('[');
          String prefixStr = sb.toString();
          StringBuffer buff2 = new StringBuffer();
          
          int size = indexingKeys.size();
          for(int j=0; j < size;++j) {
            CompiledValue cv = indexingKeys.get(size-j-1);
            this.mapKeys[size-j-1] = cv.evaluate(context);
            StringBuffer sbuff = new StringBuffer();
            cv.generateCanonicalizedExpression(sbuff, context);
            sbuff.insert(0,prefixStr);
            sbuff.append(']');
            this.multiIndexKeysPattern[j] = sbuff.toString();            
            cv.generateCanonicalizedExpression(buff2, context);
            buff2.insert(0,',');            
          }
          buff2.deleteCharAt(0);
          sb.append(buff2.toString());
          sb.append(']'); 
          
        }
      }else {
        expr.generateCanonicalizedExpression(sb, context);
      }
      
      //expr.generateCanonicalizedExpression(sb, this.context);
      this.indexedExpression = sb.toString();
//      String tempStr = this.indexedExpression;
      modifiedIndexExpr = expr;
      if (!this.isFirstIteratorRegionEntry
          && this.indexedExpression.indexOf(this.canonicalizedIteratorNames[0]) >= 0) {
        modifiedIndexExpr = getModifiedDependentCompiledValue(context, -1,
            expr, true);
      }
    }
    catch (Exception e) {
      //e.printStackTrace();
      throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_INDEXED_EXPRESSION_0.toLocalizedString(indexedExpression), e);
    }
    indexedExpr = expr;
  }

  private void prepareProjectionAttributes(String projectionAttributes)
      throws IndexInvalidException {
    if (projectionAttributes != null && !projectionAttributes.equals("*")) { throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_INVALID_PROJECTION_ATTRIBUTES_0.toLocalizedString(projectionAttributes)); }
    this.projectionAttributes = projectionAttributes;
  }

  private Object[] modfiyIterDefToSuiteIMQ(CompiledIteratorDef iterDef)
      throws IndexInvalidException {
    Object retValues[] = { null, null};
    try {
      CompiledValue def = iterDef.getCollectionExpr();
      //System.out.println("def = "+def);
      if (def instanceof CompiledRegion) {
        CompiledBindArgument bindArg = new CompiledBindArgument(1);
        CompiledIteratorDef newDef = new CompiledIteratorDef(iterDef.getName(),
            null, bindArg);
        retValues[0] = def.evaluate(context);
        retValues[1] = newDef;
        return retValues;
      }
      if (def instanceof CompiledPath || def instanceof CompiledOperation
          || def instanceof CompiledIndexOperation) {
        CompiledValue cv = def;
        CompiledValue prevCV = null;
        List reconstruct = new ArrayList();
        while (!(cv instanceof CompiledRegion)) {
          prevCV = cv;
          if (cv instanceof CompiledOperation) {
            reconstruct.add(0, ((CompiledOperation) cv).getArguments());
            reconstruct.add(0, ((CompiledOperation) cv).getMethodName());
            cv = ((CompiledOperation) cv).getReceiver(context);
          }
          else if (cv instanceof CompiledPath) {
            reconstruct.add(0, ((CompiledPath) cv).getTailID());
            cv = ((CompiledPath) cv).getReceiver();
          }
          else if (cv instanceof CompiledIndexOperation) {
            reconstruct.add(0, ((CompiledIndexOperation) cv).getExpression());
            cv = ((CompiledIndexOperation) cv).getReceiver();
          }
          else {
            throw new IndexInvalidException(LocalizedStrings.FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEFROM_CLAUSE_IS_NEITHER_A_COMPILEDPATH_NOR_COMPILEDOPERATION.toLocalizedString());
          }
          reconstruct.add(0, Integer.valueOf(prevCV.getType()));
        }
        CompiledValue v = cv;
        cv = new CompiledBindArgument(1);
        int len = reconstruct.size();
        for (int j = 0; j < len; ++j) {
          Object obj = reconstruct.get(j);
          if (obj instanceof Integer) {
            int tokenType = ((Integer) obj).intValue();
            if (tokenType == CompiledValue.PATH) {
              cv = new CompiledPath(cv, (String) reconstruct.get(++j));
            }
            else if (tokenType == OQLLexerTokenTypes.TOK_LBRACK) {
              cv = new CompiledIndexOperation(cv, (CompiledValue) reconstruct
                  .get(++j));
            }
            else if (tokenType == OQLLexerTokenTypes.METHOD_INV) {
              cv = new CompiledOperation(cv, (String) reconstruct.get(++j),
                  (List) reconstruct.get(++j));
            }
          }
        }
        CompiledIteratorDef newDef = new CompiledIteratorDef(iterDef.getName(),
            null, cv);
        retValues[0] = v.evaluate(context);
        retValues[1] = newDef;
        return retValues;
      }
    }
    catch (Exception e) {
      throw new IndexInvalidException(e);
    }
    return retValues;
  }

  /*
   * Asif : This function is used to correct the complied value's dependency ,
   * in case the compiledvalue is dependent on the 0th RuntimeIterator in some
   * way. Thus the dependent compiled value is prefixed with the missing link so
   * that it is derivable from the 0th iterator.
   */
  private CompiledValue getModifiedDependentCompiledValue(
      ExecutionContext context, int currItrID, CompiledValue cv,
      boolean isDependent) throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    if (cv instanceof CompiledIteratorDef) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) cv;
      RuntimeIterator rItr = (RuntimeIterator) context.getCurrentIterators()
          .get(currItrID);
      String canonFrmClause = rItr.getDefinition();
      if (canonFrmClause.startsWith(this.canonicalizedIteratorNames[0]))
        isDependent = true;
      else
        isDependent = false;
      return new CompiledIteratorDef(iterDef.getName(), rItr.getElementType(),
          getModifiedDependentCompiledValue(context, currItrID, iterDef
              .getCollectionExpr(), isDependent));
    }
    else if (cv instanceof CompiledPath) {
      CompiledPath path = (CompiledPath) cv;
      return new CompiledPath(getModifiedDependentCompiledValue(context,
          currItrID, path.getReceiver(), isDependent), path.getTailID());
    }
    else if (cv instanceof CompiledOperation) {
      CompiledOperation oper = (CompiledOperation) cv;
      List list = oper.getArguments();
      int len = list.size();
      List newList = new ArrayList();
      for (int i = 0; i < len; ++i) {
        CompiledValue cv1 = (CompiledValue) list.get(i);
        StringBuffer sbuff = new StringBuffer();
        cv1.generateCanonicalizedExpression(sbuff, context);
        if (sbuff.toString().startsWith(this.canonicalizedIteratorNames[0])) {
          newList.add(getModifiedDependentCompiledValue(context, currItrID,
              cv1, true));
        }
        else {
          newList.add(getModifiedDependentCompiledValue(context, currItrID,
              cv1, false));
        }
      }
      //Asif: What if the receiver is null?
      CompiledValue rec = oper.getReceiver(context);
      if (rec == null) {
        if (isDependent) {
          rec = this.missingLink;
        }
        return new CompiledOperation(rec, oper.getMethodName(), newList);
      }
      else {
        return new CompiledOperation(getModifiedDependentCompiledValue(context,
            currItrID, rec, isDependent), oper.getMethodName(), newList);
      }
    }
    else if (cv instanceof CompiledFunction) {
      CompiledFunction cf = (CompiledFunction) cv;
      CompiledValue[] cvArray = cf.getArguments();
      int function = cf.getFunction();
      int len = cvArray.length;
      CompiledValue[] newCvArray = new CompiledValue[len];
      for (int i = 0; i < len; ++i) {
        CompiledValue cv1 = cvArray[i];
        StringBuffer sbuff = new StringBuffer();
        cv1.generateCanonicalizedExpression(sbuff, context);
        if (sbuff.toString().startsWith(this.canonicalizedIteratorNames[0])) {
          newCvArray[i] = getModifiedDependentCompiledValue(context, currItrID,
              cv1, true);
        }
        else {
          newCvArray[i] = getModifiedDependentCompiledValue(context, currItrID,
              cv1, false);
        }
      }
      return new CompiledFunction(newCvArray, function);
    }
    else if (cv instanceof CompiledID) {
      CompiledID id = (CompiledID) cv;
      RuntimeIterator rItr0 = (RuntimeIterator) context.getCurrentIterators()
          .get(0);
      if (isDependent) {
        String name = null;
        if ((name = rItr0.getName()) != null && name.equals(id.getId())) {
          //Asif: The CompiledID is a RuneTimeIterator & so it needs to be
          //replaced by the missing link
          return this.missingLink;
        }
        else {
          //Asif: The compiledID is a compiledpath
          return new CompiledPath(missingLink, id.getId());
        }
      }
      else {
        return cv;
      }
    }
    else if (cv instanceof CompiledIndexOperation) {
      CompiledIndexOperation co = (CompiledIndexOperation) cv;
      CompiledValue cv1 = co.getExpression();
      StringBuffer sbuff = new StringBuffer();
      cv1.generateCanonicalizedExpression(sbuff, context);
      if (sbuff.toString().startsWith(this.canonicalizedIteratorNames[0])) {
        cv1 = getModifiedDependentCompiledValue(context, currItrID, cv1, true);
      }
      else {
        cv1 = getModifiedDependentCompiledValue(context, currItrID, cv1, false);
      }
      return new CompiledIndexOperation(getModifiedDependentCompiledValue(
          context, currItrID, co.getReceiver(), isDependent), cv1);
    }
    else {
      return cv;
    }
  }

  private void remove(List list, int count, int index) {
    for (int j = 0; j < count; ++j) {
      list.remove(index);
    }
  }
}
