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

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.NameNotFoundException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;


/**
 * Class Description
 *
 * @version $Revision: 1.1 $
 */

public class PathUtils {

  public static String[] tokenizePath(String path) {
    ArrayList alist = new ArrayList();
    StringTokenizer tokenizer = new StringTokenizer(path, ".");
    while (tokenizer.hasMoreTokens()) {
      alist.add(tokenizer.nextToken());
    }
    return (String[]) alist.toArray(new String[0]);
  }

  public static String buildPathString(String[] path) {
    Support.assertArg(path != null && path.length > 0, "path should not be null or empty");
    StringBuilder buf = new StringBuilder();
    buf.append(path[0]);
    for (int i = 1; i < path.length; i++) {
      buf.append('.');
      buf.append(path[i]);
    }
    return buf.toString();
  }

  public static Object evaluateAttribute(ExecutionContext context, Object target, String attribute)
      throws NameNotFoundException, QueryInvocationTargetException {
    if (target instanceof Struct) {
      Struct struct = (Struct) target;
      try {
        return struct.get(attribute);
      } catch (Exception e) {
        throw new NameNotFoundException(attribute);
      }
    }

    try {
      return new AttributeDescriptor(context.getCache().getPdxRegistry(), attribute)
          .read(target, context);
    } catch (NameNotFoundException nfe) {
      if (DefaultQueryService.QUERY_HETEROGENEOUS_OBJECTS
          || DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS) {
        return QueryService.UNDEFINED;
      } else {
        throw nfe;
      }
    }
  }

  /*
   * This was added as part of CQ performance changes. The change is done to re-use the
   * AttributeDescriptor object instead of creating it each time.
   */
  /*
   * public static Object evaluateAttribute(Object target, String attribute, AttributeDescriptor
   * attributeDescriptor) throws NameNotFoundException, QueryInvocationTargetException { if(target
   * instanceof Struct){ Struct struct = (Struct)target; try{ return struct.get(attribute);
   * }catch(Exception e){ throw new NameNotFoundException(attribute); } } return
   * attributeDescriptor.read(target); }
   */

  /**
   * @param pathArray the path starting with an attribute on the initial type.
   * @return array of types starting with the initialType and ending with the type of the last
   *         attribute in the path.
   * @throws NameNotFoundException if could not find an attribute along path
   *
   */
  public static ObjectType[] calculateTypesAlongPath(ExecutionContext context,
      ObjectType initialType, String[] pathArray) throws NameNotFoundException {
    ObjectType[] types = new ObjectType[pathArray.length + 1];
    // initialClass goes in front
    types[0] = initialType;

    for (int i = 1; i < types.length; i++) {
      ObjectType currentType = types[i - 1];
      Member member = new AttributeDescriptor(context.getCache().getPdxRegistry(), pathArray[i - 1])
          .getReadMember(currentType.resolveClass());

      if (member instanceof Field) {
        types[i] = TypeUtils.getObjectType(((Field) member).getType());
      } else if (member instanceof Method) {
        types[i] = TypeUtils.getObjectType(((Method) member).getReturnType());
      }
    }
    return types;
  }


  public static ObjectType computeElementTypeOfExpression(ExecutionContext context,
      CompiledValue expr) throws AmbiguousNameException {
    try {

      ObjectType type = TypeUtils.OBJECT_TYPE;
      List exprSteps = new ArrayList();
      while (true) {
        if (expr instanceof CompiledPath) {
          CompiledPath path = (CompiledPath) expr;
          exprSteps.add(0, path.getTailID());
          expr = path.getReceiver();
        } else if (expr instanceof CompiledOperation) {
          CompiledOperation operation = (CompiledOperation) expr;
          if (operation.getArguments().size() > 0) {
            return TypeUtils.OBJECT_TYPE;
          }
          exprSteps.add(0, operation.getMethodName() + "()");
          expr = operation.getReceiver(context);
          if (expr == null) {
            expr = context.resolveImplicitOperationName(operation.getMethodName(),
                operation.getArguments().size(), true);
          }
        } else if (expr instanceof CompiledID) {
          expr = context.resolve(((CompiledID) expr).getId());
        } else if (expr instanceof CompiledRegion) {
          QRegion qrgn = (QRegion) ((CompiledRegion) expr).evaluate(context);
          type = qrgn.getCollectionType();
          break;
        } else if (expr instanceof RuntimeIterator) {
          type = ((RuntimeIterator) expr).getElementType();
          break;
        } else {
          return TypeUtils.OBJECT_TYPE;
        }
      }
      if (!TypeUtils.OBJECT_TYPE.equals(type)) {
        Class clazz = type.resolveClass();
        for (Object exprStep : exprSteps) {
          Member member;
          String stepStr = (String) exprStep;
          // System.out.println("step = "+step);
          if (stepStr.endsWith("()")) {
            stepStr = stepStr.substring(0, stepStr.length() - 2);
            member = clazz.getMethod(stepStr, (Class[]) null);
          } else {
            member = new AttributeDescriptor(context.getCache().getPdxRegistry(), stepStr)
                .getReadMember(clazz);
          }

          if (member instanceof Field) {
            clazz = ((Field) member).getType();
          } else if (member instanceof Method) {
            clazz = ((Method) member).getReturnType();
          }
          type = TypeUtils.getObjectType(clazz);
        }
        return type;
      }
    } catch (NoSuchMethodException ignored) {
    } catch (NameResolutionException ignored) {
    } catch (TypeMismatchException ignored) {
    }
    return TypeUtils.OBJECT_TYPE;
  }

  /**
   * Collects all the compiled values in the path , starting from the self at position 0 in the
   * returned List
   *
   * @return List of CompiledValues ( includes the RuntimeIterator)
   */
  public static List<CompiledValue> collectCompiledValuesInThePath(CompiledValue expr,
      ExecutionContext context) throws AmbiguousNameException, TypeMismatchException {
    boolean toContinue = true;
    List<CompiledValue> retList = new ArrayList<>();

    int exprType = expr.getType();
    while (toContinue) {
      switch (exprType) {
        case OQLLexerTokenTypes.RegionPath:
          retList.add(expr);
          toContinue = false;
          break;
        case OQLLexerTokenTypes.METHOD_INV:
          retList.add(expr);
          CompiledOperation operation = (CompiledOperation) expr;
          expr = operation.getReceiver(null/*
                                            * pass the ExecutionContext as null, thus never
                                            * implicitly resolving to RuntimeIterator
                                            */);
          if (expr == null) {
            expr = operation;
            toContinue = false;
          }
          break;
        case CompiledValue.PATH:
          retList.add(expr);
          expr = expr.getReceiver();
          break;
        case OQLLexerTokenTypes.ITERATOR_DEF:
          retList.add(expr);
          toContinue = false;
          break;
        case OQLLexerTokenTypes.TOK_LBRACK:
          retList.add(expr);
          expr = expr.getReceiver();
          break;
        case OQLLexerTokenTypes.Identifier:
          CompiledID cid = (CompiledID) expr;
          expr = context.resolve(cid.getId());
          break;
        default:
          toContinue = false;
          break;
      }

      if (toContinue) {
        exprType = expr.getType();
      }
    }
    return retList;
  }

}
