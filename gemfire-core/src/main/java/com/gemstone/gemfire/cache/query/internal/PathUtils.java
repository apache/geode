/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: PathUtils.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.lang.reflect.*;
import java.util.*;

import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.NameNotFoundException;
import com.gemstone.gemfire.cache.query.QueryService; 
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.cache.query.internal.types.*;


/**
 * Class Description
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */

public class PathUtils {
  
  public static String[] tokenizePath(String path) {
    ArrayList alist = new ArrayList();
    StringTokenizer tokenizer = new StringTokenizer(path, ".");
    while (tokenizer.hasMoreTokens()) {
      alist.add(tokenizer.nextToken());
    }
    return (String[])alist.toArray(new String[alist.size()]);
  }
  
  public static String buildPathString(String[] path) {
    Support.assertArg(path != null && path.length > 0, "path should not be null or empty");
    StringBuffer buf = new StringBuffer();
    buf.append(path[0]);
    for (int i = 1 ; i < path.length; i++) {
      buf.append('.');
      buf.append(path[i]);
    }
    return buf.toString();
  }
  
  public static Object evaluateAttribute(Object target, String attribute)
  throws NameNotFoundException, QueryInvocationTargetException {
    if(target instanceof Struct){
      Struct struct = (Struct)target;
      try{
        return struct.get(attribute);
      }catch(Exception e){
        throw new NameNotFoundException(attribute);
      }
    }
    try {
      return new AttributeDescriptor(attribute).read(target);
    } catch (NameNotFoundException nfe) {
      if (DefaultQueryService.QUERY_HETEROGENEOUS_OBJECTS ||
          DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS) {
        return QueryService.UNDEFINED;
      } else {
        throw nfe;
      }
    }
  }

  /*
   * This was added as part of CQ performance changes.
   * The change is done to re-use the AttributeDescriptor object instead of creating
   * it each time. 
   */
  /*
  public static Object evaluateAttribute(Object target, String attribute, AttributeDescriptor attributeDescriptor)
  throws NameNotFoundException, QueryInvocationTargetException {
    if(target instanceof Struct){
      Struct struct = (Struct)target;
      try{
        return struct.get(attribute);
      }catch(Exception e){
        throw new NameNotFoundException(attribute);
      }
    }
    return attributeDescriptor.read(target);
  }
  */
  
  /**
   * @param pathArray the path starting with an attribute on
   * the initial type.
   * @return array of types starting with the initialType and
   * ending with the type of the last attribute in the path.
   * @throws NameNotFoundException if could not find an attribute along path
   *
   */
  public static ObjectType[] calculateTypesAlongPath(ObjectType initialType,
          String[] pathArray)
          throws NameNotFoundException {
    ObjectType[] types = new ObjectType[pathArray.length + 1];
    // initialClass goes in front
    types[0] = initialType;
    
    for (int i = 1; i < types.length; i++) {
      ObjectType currentType = types[i - 1];
      Member member = new AttributeDescriptor(pathArray[i-1]).getReadMember(currentType);
      if (member instanceof Field)
        types[i] = TypeUtils.getObjectType(((Field)member).getType());
      else if (member instanceof Method)
        types[i] = TypeUtils.getObjectType(((Method)member).getReturnType());
    }
    return types;
  }
  
  
  public static ObjectType computeElementTypeOfExpression(
      ExecutionContext context, CompiledValue expr)
      throws AmbiguousNameException
  {
    try {

      ObjectType type = TypeUtils.OBJECT_TYPE;
      List exprSteps = new ArrayList();
      while (true) {
        if (expr instanceof CompiledPath) {
          CompiledPath path = (CompiledPath)expr;
          exprSteps.add(0, path.getTailID());
          expr = path.getReceiver();
        }
        else if (expr instanceof CompiledOperation) {
          CompiledOperation operation = (CompiledOperation)expr;
          if (operation.getArguments().size() > 0) {
            return TypeUtils.OBJECT_TYPE;
          }
          exprSteps.add(0, operation.getMethodName() + "()");
          expr = operation.getReceiver(context);
          if (expr == null) {
            expr = context.resolveImplicitOperationName(operation
                .getMethodName(), operation.getArguments().size(), true);
          }
        }
        else if (expr instanceof CompiledID) {
          expr = context.resolve(((CompiledID)expr).getId());
        }
        else if (expr instanceof CompiledRegion) {
          QRegion qrgn = (QRegion)((CompiledRegion)expr).evaluate(context);
          type = qrgn.getCollectionType();
          break;
        }
        else if (expr instanceof RuntimeIterator) {
          type = ((RuntimeIterator)expr).getElementType();
          break;
        }
        else {
          return TypeUtils.OBJECT_TYPE;
        }
      }
      if (!TypeUtils.OBJECT_TYPE.equals(type)) {
        Class clazz = type.resolveClass();
        for (int i = 0; i < exprSteps.size(); i++) {
          Member member;
          String stepStr = (String)exprSteps.get(i);
          // System.out.println("step = "+step);
          if (stepStr.endsWith("()")) {
            stepStr = stepStr.substring(0, stepStr.length() - 2);
            member = clazz.getMethod(stepStr, (Class[])null);
          }
          else {
            member = new AttributeDescriptor(stepStr).getReadMember(clazz);
          }
          if (member instanceof Field) {
            clazz = ((Field)member).getType();
          }
          else if (member instanceof Method) {
            clazz = ((Method)member).getReturnType();
          }
          type = TypeUtils.getObjectType(clazz);
        }
        return type;
      }
    }
    catch (NoSuchMethodException e) {
    }
    catch (NameResolutionException e) {
    }
    catch (TypeMismatchException e) {
    }
    return TypeUtils.OBJECT_TYPE;
  }

}
