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
package com.gemstone.gemfire.cache.query.internal.types;


import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.Support;
import com.gemstone.gemfire.cache.query.internal.Undefined;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.types.MapType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.EnumInfo.PdxInstanceEnumInfo;
import com.gemstone.gemfire.pdx.internal.PdxInstanceEnum;


/**
 * Utilities for casting and comparing values of possibly differing types,
 * testing and cloning query literals.
 *
 * @version     $Revision: 1.1 $
 * @author      ericz
 */

public class TypeUtils implements OQLLexerTokenTypes
{
    private static List _numericPrimitiveClasses = Arrays.asList(new Class[]
                                                                {Byte.TYPE,
                                                                 Short.TYPE,
                                                                 Integer.TYPE,
                                                                 Long.TYPE,
                                                                 Float.TYPE,
                                                                 Double.TYPE});
    
    private static List _numericWrapperClasses = Arrays.asList(new Class[]
                                                              {Byte.class,
                                                               Short.class,
                                                               Integer.class,
                                                               Long.class,
                                                               Float.class,
                                                               Double.class});
                                                               
    /* Common Types */
    /** ObjectType for Object.class */                                                               
    public static final ObjectType OBJECT_TYPE = new ObjectTypeImpl(Object.class);                                                               
    
                                                               
    /** prevent instantiation */
    private TypeUtils() {}    
    

    /** Verify that type-cast will work, or else throw an informative
     *  exception.
     *  @return the castTarget
     *  @throws InternalGemFireError if cast will fail
     */
    public static Object checkCast(Object castTarget, Class castClass) {
      if (castTarget == null) return null; // null can be cast to anything
      if (!castClass.isInstance(castTarget)) {
        throw new InternalGemFireError(LocalizedStrings.TypeUtils_EXPECTED_INSTANCE_OF_0_BUT_WAS_1.toLocalizedString(new Object[] {castClass.getName(), castTarget.getClass().getName()})); 
      }
      return castTarget;
    }
        

        // implicit conversion of numeric types:
        // floating point type to other floating point type, or
        // any integral type to any other integral type if possible
        // if an integral conversion cannot be done then a TypeMismatchException
        // is thrown.
/*    public static Object convert(Object obj, Class toType)
        throws TypeMismatchException
    {
            // if obj is null, just leave it alone
        if (obj == null)
            return null;
        

        if (toType.isInstance(obj))
            return obj;

        if (Float.class.isAssignableFrom(toType) && obj instanceof Double)
            return new Float(((Double)obj).floatValue());
        
        if (Double.class.isAssignableFrom(toType) && obj instanceof Float)
            return Double.valueOf(((Float)obj).doubleValue());

        if (Long.class.isAssignableFrom(toType) &&
            obj instanceof Byte || obj instanceof Short || obj instanceof Integer)
            return Long.valueOf(((Number)obj).longValue());

        if (Integer.class.isAssignableFrom(toType) &&
            obj instanceof Byte || obj instanceof Short || obj instanceof Long)
        {
            int newInt = ((Number)obj).intValue();
            if (!(obj instanceof Long) || (long)newInt == ((Long)obj).longValue())
                return Integer.valueOf(newInt);
        }

        if (Short.class.isAssignableFrom(toType) &&
            obj instanceof Byte || obj instanceof Integer || obj instanceof Long)
        {
            short newShort = ((Number)obj).shortValue();
            if (obj instanceof Byte
                || (obj instanceof Long && (long)newShort == ((Long)obj).longValue())
                || (obj instanceof Integer && (int)newShort == ((Integer)obj).intValue()))
                return new Short(newShort);
        }

        throw new TypeMismatchException(LocalizedStrings.TypeUtils_UNABLE_TO_CONVERT_0_TO_1.toLocalizedString(new Object[] {obj, toType}));
    }
*/
   /**
    * Compares two objects using the operator
    * 
    * @param obj1
    * @param obj2
    * @param compOp
    * @return boolean;<br> 
    *         {@link Undefined} if either of the operands is {@link Undefined} or
    *         if either of the operands is Null and operator is other than == or
    *         !=
    * @throws TypeMismatchException
    */ 
    public static Object compare(Object obj1, Object obj2, int compOp)
    throws TypeMismatchException {
      if (obj1 == null || obj2 == null) {
        Boolean result = nullCompare(obj1, obj2, compOp);
        if (result == null)
          return QueryService.UNDEFINED;
        return result;
      }

      // if either object is UNDEFINED, result is UNDEFINED
      if (obj1 == QueryService.UNDEFINED || obj2 == QueryService.UNDEFINED) {
        if (compOp == TOK_NE && !(obj1 == QueryService.UNDEFINED && obj2 == QueryService.UNDEFINED)) {
          return true;
        } 
        else if (compOp == TOK_EQ && obj1.equals(obj2)) {
          return true;
        }
        else {
          return QueryService.UNDEFINED;
        }
      }
      
    if (obj1 instanceof PdxInstanceEnumInfo && obj2 instanceof Enum) {
      obj2 = new PdxInstanceEnum((Enum<?>) obj2);
    } else if (obj1 instanceof Enum && obj2 instanceof PdxInstanceEnumInfo) {
      obj1 = new PdxInstanceEnum((Enum<?>) obj1);
    }
      
      try {
        int r;
        
        if (obj1 instanceof java.util.Date && obj2 instanceof java.util.Date)
          r = getTemporalComparator().compare(obj1, obj2);
        
        else if (obj1.getClass() != obj2.getClass()
                 && (obj1 instanceof Number && obj2 instanceof Number)) {
          /* @todo check for NaN, in which case we should not call compareTo
             Must also handle this in the index lookup code to be consistent
             See bug 37716
          NumericComparator cmprtr = getNumericComparator();
          boolean b;
          if (obj1.equals(Float.NaN) || obj1.equals(Double.NaN)) {
            return new Boolean(cmprtr.compareWithNaN(obj2));
          }
          else if (obj2.equals(Float.NaN) || obj2.equals(Float.NaN)) {
            return new Boolean(cmprtr.compareWithNaN(obj1));
          }
          */
          r = getNumericComparator().compare(obj1, obj2);
        }
        
        else if (obj1 instanceof Boolean || obj2 instanceof Boolean)
          return Boolean.valueOf(booleanCompare(obj1, obj2, compOp));
        
        
        else if (obj1 instanceof Comparable && obj2 instanceof Comparable)
          r = ((Comparable)obj1).compareTo(obj2);
        // comparison of two arbitrary objects: use equals()
        else if (compOp == TOK_EQ)
          return Boolean.valueOf(obj1.equals(obj2));
        else if (compOp == TOK_NE)
          return Boolean.valueOf(!obj1.equals(obj2));
        else
          throw new TypeMismatchException(LocalizedStrings.TypeUtils_UNABLE_TO_USE_A_RELATIONAL_COMPARISON_OPERATOR_TO_COMPARE_AN_INSTANCE_OF_CLASS_0_WITH_AN_INSTANCE_OF_1.toLocalizedString(new Object[] {obj1.getClass().getName(), obj2.getClass().getName()}));
        
        
        switch(compOp) {
          case TOK_EQ:
            return Boolean.valueOf(r == 0);
          case TOK_LT:
            return Boolean.valueOf(r < 0);
          case TOK_LE:
            return Boolean.valueOf(r <= 0);
          case TOK_GT:
            return Boolean.valueOf(r > 0);
          case TOK_GE:
            return Boolean.valueOf(r >= 0);
          case TOK_NE:
            return Boolean.valueOf(r != 0);
          default:
            throw new IllegalArgumentException(LocalizedStrings.TypeUtils_UNKNOWN_OPERATOR_0.toLocalizedString(Integer.valueOf(compOp)));
        }
      } catch (ClassCastException e) {
        // if a ClassCastException was thrown and the operator is equals or not equals,
        // then override and return true or false
        if (compOp == TOK_EQ)
          return Boolean.FALSE;
        if (compOp == TOK_NE)
          return Boolean.TRUE;
        
        throw new TypeMismatchException(LocalizedStrings.TypeUtils_UNABLE_TO_COMPARE_OBJECT_OF_TYPE_0_WITH_OBJECT_OF_TYPE_1.toLocalizedString(new Object[] {obj1.getClass().getName(), obj2.getClass().getName()}), e);
      } catch (TypeMismatchException e) {
        // same for TypeMismatchException
        if (compOp == TOK_EQ)
          return Boolean.FALSE;
        if (compOp == TOK_NE)
          return Boolean.TRUE;
        throw e;
      }
    }
    
    public static Comparator getTemporalComparator() {
      return new TemporalComparator();
    }
    
    public static Comparator getNumericComparator() {
      return new NumericComparator();
    }
    
    public static Comparator getExtendedNumericComparator() {
      return new ExtendedNumericComparator();
    }
    
    
    public static Object indexKeyFor(Object obj)
    throws TypeMismatchException {
      if (obj == null)
        return null;
      if (obj instanceof Byte)
        return Integer.valueOf(((Byte)obj).intValue());
      if (obj instanceof Short)
        return Integer.valueOf(((Short)obj).intValue());
      // Ketan : Added later. Indexes should be created 
      // if the IndexedExpr implements Comparable interface.
      if (obj instanceof Comparable) {
        if (obj instanceof Enum) {
          obj = new PdxInstanceEnum((Enum<?>)obj);
        }
        return obj;
      }
      throw new TypeMismatchException(LocalizedStrings.TypeUtils_INDEXES_ARE_NOT_SUPPORTED_FOR_TYPE_0.toLocalizedString(obj.getClass().getName()));
    }
    
    // returns null if this type doesn't need a Comparator
    public static Comparator comparatorFor(Class pathType) {
      Iterator i = _numericWrapperClasses.iterator();
      while(i.hasNext())
        if (((Class)i.next()).isAssignableFrom(pathType))
          return getNumericComparator();
      i = _numericPrimitiveClasses.iterator();
      while(i.hasNext())
        if (((Class)i.next()).isAssignableFrom(pathType))
          return getNumericComparator();
      if (java.util.Date.class.isAssignableFrom(pathType))
        return getTemporalComparator();
      return null;
    }
    
    
    // indexes are allowed on primitive types except char,
    // plus Strings, and temporals
    /**
     * Return the type of the keys for a given path type.
     * @param pathType the Class of the last attribute in the path
     * @return the Class of the index keys
     * @throws TypeMismatchException if indexes are not allowed on this type
     */
    
    public static Class indexTypeFor(Class pathType)
    throws TypeMismatchException {
      if (Character.class.isAssignableFrom(pathType) || Character.TYPE == pathType)
        return pathType;
      
      if (Byte.class.isAssignableFrom(pathType) || Short.class.isAssignableFrom(pathType)
      || Byte.TYPE == pathType || Short.TYPE == pathType)
        return Integer.class;
      Iterator i = _numericWrapperClasses.iterator();
      while (i.hasNext()) {
        Class cls = (Class)i.next();
        if (cls.isAssignableFrom(pathType))
          return pathType;
      }
      i = _numericPrimitiveClasses.iterator();
      while (i.hasNext()) {
        Class cls = (Class)i.next();
        if (cls == pathType)
          return pathType;
      }
      
      if (java.util.Date.class.isAssignableFrom(pathType) || pathType == String.class)
        return pathType;
      throw new TypeMismatchException(LocalizedStrings.TypeUtils_INDEXES_ARE_NOT_SUPPORTED_ON_PATHS_OF_TYPE_0.toLocalizedString(pathType.getName()));
    }
    
    
    
    public static boolean areTypesConvertible(Class[] srcTypes, Class[] destTypes) {
      Support.assertArg(srcTypes.length == destTypes.length,
              "Arguments 'srcTypes' and 'destTypes' must be of same length");
      
      for (int i = 0; i < srcTypes.length; i++)
        if (!isTypeConvertible(srcTypes[i], destTypes[i]))
          return false;
      return true;
    }
    
    public static boolean isTypeConvertible(Class srcType, Class destType) {
      // handle null: if srcType is null, then it represents
      // a null runtime value, and for our purposes it is type
      // convertible to any type that is assignable from Object.class
      if (srcType == null)
        return (Object.class.isAssignableFrom(destType));
      
      // check to see if the classes are assignable
      if (destType.isAssignableFrom(srcType))
        return true;
      
      // handle booleans: are we going from a wrapper Boolean
      // to a primitive boolean or vice-versa?
      if ((srcType == Boolean.TYPE || srcType == Boolean.class)
      &&
              (destType == Boolean.TYPE || destType == Boolean.class))
        return true;
      
      
      // a numeric primitive or wrapper can be converted to
      // the same wrapper or a same or wider primitive.
      // handle chars specially
      int i = _numericPrimitiveClasses.indexOf(srcType);
      if (i < 0)
        i = _numericWrapperClasses.indexOf(srcType);
      
      int destP = _numericPrimitiveClasses.indexOf(destType);
      int destW = -1;
      if (destP < 0)
        destW = _numericWrapperClasses.indexOf(destType);
      
      // same size wrapper
      if (i >= 0 && destW == i)
        return true;
      
      // same or wider primitive
      if (i >= 0 && destP >= i)
        return true;
      
      // chars
      if (srcType == Character.class || srcType == Character.TYPE) {
        // chars: same size wrapper/primitive
        if (destType == Character.class || destType == Character.TYPE)
          return true;
      }
      
      // no other possibilities
      return false;
    }
    
    
    private static boolean booleanCompare(Object obj1, Object obj2, int compOp)
    throws TypeMismatchException {
      if (!(obj1 instanceof Boolean) || !(obj2 instanceof Boolean))
        throw new TypeMismatchException(LocalizedStrings.TypeUtils_BOOLEANS_CAN_ONLY_BE_COMPARED_WITH_BOOLEANS.toLocalizedString());
      
      if (compOp == TOK_EQ)
        return obj1.equals(obj2);
      else if (compOp == TOK_NE)
        return !obj1.equals(obj2);
      else
        throw new TypeMismatchException(LocalizedStrings.TypeUtils_BOOLEAN_VALUES_CAN_ONLY_BE_COMPARED_WITH_OR.toLocalizedString());
    }
    
    
    // returns a Boolean or null if UNDEFINED
    private static Boolean nullCompare(Object obj1, Object obj2, int compOp) {
      switch(compOp) {
        case TOK_EQ:
          if (obj1 == null)
            return Boolean.valueOf(obj2 == null);
          else // obj1 is not null obj2 must be
            return Boolean.FALSE;
        case TOK_NE:
          if (obj1 == null)
            return Boolean.valueOf(obj2 != null);
          else // obj1 is not null so obj2 must be
            return Boolean.TRUE;
        default:
          return null;
      }
    }
    
    //    public static boolean isList(Class clazz) {
    //      if( clazz == java.util.ArrayList.class ||
    //              clazz == java.util.LinkedList.class ||
    //              clazz == java.util.Vector.class)
    //        return true;
    //      return false;
    //    }
    
    public static boolean isMap(ObjectType objType) {
      return objType instanceof MapType;
    }
    
    //    public static boolean isSet(Class clazz){
    //      if( clazz == java.util.HashSet.class ||
    //              clazz == java.util.LinkedHashSet.class ||
    //              clazz == java.util.TreeSet.class)
    //        return true;
    //      return false;
    //    }
    
    public static ObjectType getObjectType(Class cls) {
      if (cls == Object.class) {
        return OBJECT_TYPE;
      }      
      if (Collection.class.isAssignableFrom(cls)) {
        // we don't have element type info here
        return new CollectionTypeImpl(cls, OBJECT_TYPE);
      }
      if (cls.isArray()) {
        return new CollectionTypeImpl(cls, getObjectType(cls.getComponentType()));
      }
      if (Region.class.isAssignableFrom(cls)) {
        // we don't have access to the region itself for element type
        return new CollectionTypeImpl(cls, OBJECT_TYPE);
      }
      if (Map.class.isAssignableFrom(cls)) {
        return new MapTypeImpl(cls, OBJECT_TYPE, OBJECT_TYPE);
      }
      // if it's a struct we have no field info, so just return an ObjectTypeImpl
      return new ObjectTypeImpl(cls);
    }
    
    public static ObjectType getRegionEntryType(Region rgn) {
      // just use an ObjectType for now
      return new ObjectTypeImpl(Region.Entry.class);
    }
    
}

