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
package org.apache.geode.cache.query.internal.types;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.Support;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.pdx.internal.EnumInfo.PdxInstanceEnumInfo;
import org.apache.geode.pdx.internal.PdxInstanceEnum;
import org.apache.geode.pdx.internal.PdxString;


/**
 * Utilities for casting and comparing values of possibly differing types, testing and cloning query
 * literals.
 *
 * @version $Revision: 1.1 $
 */

public class TypeUtils implements OQLLexerTokenTypes {
  protected static List<Class> _numericPrimitiveClasses = Arrays.asList(
      new Class[] {Byte.TYPE, Short.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE, Double.TYPE});

  protected static List<Class> _numericWrapperClasses = Arrays.asList(
      new Class[] {Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class});

  /**
   * Enum to execute comparisons based on types.
   */
  private enum ComparisonStrategy {
    TEMPORAL {
      public Boolean execute(Object object1, Object object2, int comparator)
          throws ClassCastException {
        return applyComparator(getTemporalComparator().compare(object1, object2), comparator);
      }
    },

    NUMERIC {
      public Boolean execute(Object object1, Object object2, int comparator)
          throws ClassCastException {
        return applyComparator(getNumericComparator().compare(object1, object2), comparator);
      }
    },

    BOOLEAN {
      public Boolean execute(Object object1, Object object2, int comparator)
          throws TypeMismatchException {
        return booleanCompare(object1, object2, comparator);
      }
    },

    COMPARABLE {
      public Boolean execute(Object object1, Object object2, int comparator)
          throws ClassCastException {
        return applyComparator(((Comparable) object1).compareTo(object2), comparator);
      }
    },

    ARBITRARY {
      public Boolean execute(Object object1, Object object2, int comparator) {
        if (comparator == TOK_EQ) {
          return object1.equals(object2);
        } else {
          return !object1.equals(object2);
        }
      }
    };

    protected Boolean applyComparator(int temporalResult, int comparator)
        throws IllegalArgumentException {
      switch (comparator) {
        case TOK_EQ:
          return temporalResult == 0;
        case TOK_LT:
          return temporalResult < 0;
        case TOK_LE:
          return temporalResult <= 0;
        case TOK_GT:
          return temporalResult > 0;
        case TOK_GE:
          return temporalResult >= 0;
        case TOK_NE:
          return temporalResult != 0;
        default:
          throw new IllegalArgumentException(String.format("Unknown operator: %s",
              Integer.valueOf(comparator)));
      }
    }

    public static ComparisonStrategy get(Class object1Class, Class object2Class, int comparator)
        throws TypeMismatchException {
      if (isAssignableFrom(object1Class, Date.class)
          && isAssignableFrom(object1Class, Date.class)) {
        return TEMPORAL;
      } else if (object1Class != object2Class && (isAssignableFrom(object1Class, Number.class)
          && isAssignableFrom(object2Class, Number.class))) {
        return NUMERIC;
      } else if (isAssignableFrom(object1Class, Boolean.class)
          || isAssignableFrom(object2Class, Boolean.class)) {
        return BOOLEAN;
      } else if (isAssignableFrom(object1Class, Comparable.class)
          && isAssignableFrom(object2Class, Comparable.class)) {
        return COMPARABLE;
      } else if ((comparator == TOK_EQ) || (comparator == TOK_NE)) {
        return ARBITRARY;
      } else {
        throw new TypeMismatchException(
            String.format(
                "Unable to use a relational comparison operator to compare an instance of class ' %s ' with an instance of ' %s '",
                new Object[] {object1Class.getName(), object2Class.getName()}));
      }
    }

    /**
     * Executes the comparison strategy.
     *
     */
    public abstract Boolean execute(Object object1, Object object2, int comparator)
        throws TypeMismatchException, ClassCastException;
  }

  /* Common Types */
  /** ObjectType for Object.class */
  public static final ObjectType OBJECT_TYPE = new ObjectTypeImpl(Object.class);

  /** prevent instantiation */
  private TypeUtils() {}

  /**
   * Creates an instance of {@link org.apache.geode.cache.query.internal.types.TemporalComparator}.
   *
   * @return Comparator for mixed comparisons between instances of java.util.Date, java.sql.Date,
   *         java.sql.Time, and java.sql.Timestamp.
   * @see org.apache.geode.cache.query.internal.types.TemporalComparator
   */
  public static Comparator getTemporalComparator() {
    return new TemporalComparator();
  }

  /**
   * Creates an instance of {@link org.apache.geode.cache.query.internal.types.NumericComparator}.
   *
   * @return Comparator for mixed comparisons between numbers.
   * @see org.apache.geode.cache.query.internal.types.NumericComparator
   */
  public static Comparator getNumericComparator() {
    return new NumericComparator();
  }

  /**
   * Creates an instance of
   * {@link org.apache.geode.cache.query.internal.types.ExtendedNumericComparator}.
   *
   * @return A general comparator that compares different numeric types for equality.
   * @see org.apache.geode.cache.query.internal.types.ExtendedNumericComparator
   */
  public static Comparator getExtendedNumericComparator() {
    return new ExtendedNumericComparator();
  }

  /**
   * Verify that type-cast will work, or else throw an informative exception.
   *
   * @return the castTarget
   * @throws InternalGemFireError if cast will fail
   */
  public static Object checkCast(Object castTarget, Class castClass) {
    if (castTarget == null) {
      return null; // null can be cast to anything
    }

    if (!castClass.isInstance(castTarget)) {
      throw new InternalGemFireError(String.format("expected instance of %s but was %s",
          new Object[] {castClass.getName(), castTarget.getClass().getName()}));
    }

    return castTarget;
  }

  /**
   * Given an arbitrary object, return the value that should be used as the key for a particular
   * index.
   *
   * @param obj the object to evaluate.
   * @return the value that should be used as the key for an index.
   * @throws TypeMismatchException if the object type is not supported to be used as an index.
   */
  public static Object indexKeyFor(Object obj) throws TypeMismatchException {
    if (obj == null) {
      return null;
    }

    if (obj instanceof Byte) {
      return Integer.valueOf(((Byte) obj).intValue());
    }

    if (obj instanceof Short) {
      return Integer.valueOf(((Short) obj).intValue());
    }

    // Ketan : Added later. Indexes should be created
    // if the IndexedExpr implements Comparable interface.
    if (obj instanceof Comparable) {
      if (obj instanceof Enum) {
        obj = new PdxInstanceEnum((Enum<?>) obj);
      }

      return obj;
    }

    throw new TypeMismatchException(String.format("Indexes are not supported for type ' %s '",
        obj.getClass().getName()));
  }

  /**
   * Determines if the Class represented by the first parameter is either the same as, or is a
   * superclass or superinterface of, the class or interface represented by the second parameter.
   *
   * @param srcType the class to be checked.
   * @param destType the class to be checked against.
   * @return the {@code boolean} value indicating whether objects of the type {@code srcType} can be
   *         assigned to objects of type {@code destType} .
   */
  protected static boolean isAssignableFrom(Class srcType, Class destType) {
    return destType.isAssignableFrom(srcType);
  }

  /**
   * Determines if the Class represented by the first parameter can be safely converted to the Class
   * represented by the second parameter.
   *
   * @param srcType the source class to be converted.
   * @param destType the target class to be converted to.
   * @return the {@code boolean} value indicating whether objects of the type {@code srcType} can be
   *         converted to objects of type {@code destType} .
   */
  public static boolean isTypeConvertible(Class srcType, Class destType) {
    // handle null: if srcType is null, then it represents
    // a null runtime value, and for our purposes it is type
    // convertible to any type that is assignable from Object.class
    if (srcType == null) {
      // TODO: This should always be true as Object is assignable from everything, is the root type
      // of all Java classes. Object.class is always assignable from every other Java class.
      return isAssignableFrom(destType, Object.class);
    }

    // check to see if the classes are assignable
    if (isAssignableFrom(srcType, destType)) {
      return true;
    }

    // handle booleans: are we going from a wrapper Boolean
    // to a primitive boolean or vice-versa?
    if ((srcType == Boolean.TYPE || srcType == Boolean.class)
        && (destType == Boolean.TYPE || destType == Boolean.class)) {
      return true;
    }

    // a numeric primitive or wrapper can be converted to
    // the same wrapper or a same or wider primitive.
    // handle chars specially
    int i = _numericPrimitiveClasses.indexOf(srcType);
    if (i < 0) {
      i = _numericWrapperClasses.indexOf(srcType);
    }

    int destP = _numericPrimitiveClasses.indexOf(destType);
    int destW = -1;
    if (destP < 0) {
      destW = _numericWrapperClasses.indexOf(destType);
    }

    // same size wrapper
    if (i >= 0 && destW == i) {
      return true;
    }

    // same or wider primitive
    if (i >= 0 && destP >= i) {
      return true;
    }

    // chars
    if (srcType == Character.class || srcType == Character.TYPE) {
      // chars: same size wrapper/primitive
      if (destType == Character.class || destType == Character.TYPE) {
        return true;
      }
    }

    // no other possibilities
    return false;
  }

  /**
   * Determines if the all classes represented by the first parameter can be safely converted to the
   * classes represented by the second parameter.
   *
   * @param srcTypes the source classes to be converted.
   * @param destTypes the target classes to be converted to.
   * @return the {@code boolean} value indicating whether all objects of the first array can be
   *         converted to objects of the second array.
   */
  public static boolean areTypesConvertible(Class[] srcTypes, Class[] destTypes) {
    Support.assertArg(srcTypes.length == destTypes.length,
        "Arguments 'srcTypes' and 'destTypes' must be of same length");

    for (int i = 0; i < srcTypes.length; i++) {
      if (!isTypeConvertible(srcTypes[i], destTypes[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * Return the {@link org.apache.geode.cache.query.types.ObjectType} corresponding to the specified
   * Class.
   *
   * @return the internal ObjectType implementation that represents the specified class.
   */
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

  /**
   * Return a fixed {@link org.apache.geode.cache.query.types.ObjectType} representing a Region
   * Entry.
   *
   * @return the internal ObjectType implementation that represents a the region entry class.
   */
  public static ObjectType getRegionEntryType(Region rgn) {
    // just use an ObjectType for now
    return new ObjectTypeImpl(Region.Entry.class);
  }

  /**
   * Compare two booleans.
   *
   * @return a boolean indicating the result of applying the comparison operator on the arguments.
   * @throws TypeMismatchException When either one of the arguments is not a Boolean, or the
   *         comparison operator is not supported.
   */
  protected static boolean booleanCompare(Object obj1, Object obj2, int compOp)
      throws TypeMismatchException {
    if (!(obj1 instanceof Boolean) || !(obj2 instanceof Boolean)) {
      throw new TypeMismatchException(
          "Booleans can only be compared with booleans");
    }

    if (compOp == TOK_EQ) {
      return obj1.equals(obj2);
    } else if (compOp == TOK_NE) {
      return !obj1.equals(obj2);
    } else {
      throw new TypeMismatchException(
          "Boolean values can only be compared with = or <>");
    }
  }

  /**
   *
   * @return a Boolean, or null if UNDEFINED.
   */
  private static Boolean nullCompare(Object obj1, Object obj2, int compOp) {
    Boolean result = null;

    switch (compOp) {
      case TOK_EQ: {
        if (obj1 == null) {
          result = Boolean.valueOf(obj2 == null);
        } else { // obj1 is not null obj2 must be
          result = Boolean.FALSE;
        }

        break;
      }

      case TOK_NE: {
        if (obj1 == null) {
          result = Boolean.valueOf(obj2 != null);
        } else { // obj1 is not null so obj2 must be
          result = Boolean.TRUE;
        }

        break;
      }
    }

    return result;
  }

  /**
   * Compares two objects using the operator
   *
   * @return boolean;<br>
   *         {@link Undefined} if either of the operands is {@link Undefined} or if either of the
   *         operands is Null and operator is other than == or !=
   */
  public static Object compare(Object obj1, Object obj2, int compOp) throws TypeMismatchException {
    // Check for nulls first.
    if (obj1 == null || obj2 == null) {
      Boolean result = nullCompare(obj1, obj2, compOp);

      if (result == null) {
        return QueryService.UNDEFINED;
      }

      return result;
    }

    // if either object is UNDEFINED, result is UNDEFINED
    if (obj1 == QueryService.UNDEFINED || obj2 == QueryService.UNDEFINED) {
      if (compOp == TOK_NE && !(obj1 == QueryService.UNDEFINED && obj2 == QueryService.UNDEFINED)) {
        return true;
      } else if (compOp == TOK_EQ && obj1.equals(obj2)) {
        return true;
      } else {
        return QueryService.UNDEFINED;
      }
    }

    // Prepare pdx instances if needed.
    if (obj1 instanceof PdxInstanceEnumInfo && obj2 instanceof Enum) {
      obj2 = new PdxInstanceEnum((Enum<?>) obj2);
    } else if (obj1 instanceof Enum && obj2 instanceof PdxInstanceEnumInfo) {
      obj1 = new PdxInstanceEnum((Enum<?>) obj1);
    }

    if (obj1 instanceof PdxString && obj2 instanceof String) {
      obj2 = new PdxString((String) obj2);
    } else if (obj1 instanceof String && obj2 instanceof PdxString) {
      obj1 = new PdxString((String) obj1);
    }

    try {
      ComparisonStrategy strategy =
          ComparisonStrategy.get(obj1.getClass(), obj2.getClass(), compOp);
      return strategy.execute(obj1, obj2, compOp);
    } catch (ClassCastException | TypeMismatchException e) {
      // if a ClassCastException or TypeMismatchException was thrown and the operator is equals
      // or not equals, then override and return true or false.
      if (compOp == TOK_EQ) {
        return Boolean.FALSE;
      }

      if (compOp == TOK_NE) {
        return Boolean.TRUE;
      }

      if (isAssignableFrom(e.getClass(), ClassCastException.class)) {
        throw new TypeMismatchException(
            String.format("Unable to compare object of type ' %s ' with object of type ' %s '",

                new Object[] {obj1.getClass().getName(), obj2.getClass().getName()}),
            e);
      } else {
        throw e;
      }
    }
  }
}
