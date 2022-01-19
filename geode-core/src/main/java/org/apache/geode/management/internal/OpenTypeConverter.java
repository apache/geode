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
package org.apache.geode.management.internal;

import static javax.management.openmbean.SimpleType.BIGDECIMAL;
import static javax.management.openmbean.SimpleType.BIGINTEGER;
import static javax.management.openmbean.SimpleType.BOOLEAN;
import static javax.management.openmbean.SimpleType.BYTE;
import static javax.management.openmbean.SimpleType.CHARACTER;
import static javax.management.openmbean.SimpleType.DATE;
import static javax.management.openmbean.SimpleType.DOUBLE;
import static javax.management.openmbean.SimpleType.FLOAT;
import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.LONG;
import static javax.management.openmbean.SimpleType.OBJECTNAME;
import static javax.management.openmbean.SimpleType.SHORT;
import static javax.management.openmbean.SimpleType.STRING;
import static javax.management.openmbean.SimpleType.VOID;

import java.beans.ConstructorProperties;
import java.io.InvalidObjectException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.WeakHashMap;

import javax.management.JMX;
import javax.management.ObjectName;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataInvocationHandler;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularType;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.management.ManagementException;

/**
 * It takes care of converting a Java type to an open types
 *
 * A Java type is an instance of java.lang.reflect.Type representing all types in Java.
 *
 * Each Type is associated with an OpenTypeConverter. The OpenTypeConverter defines an OpenType
 * corresponding to the Type, plus a Java class corresponding to the OpenType. For example:
 *
 *
 * Java Type : Integer Open Class :Integer Open Type :SimpleType.INTEGER
 *
 * Apart from simple types, arrays, and collections, Java types are converted through introspection
 * into CompositeType
 */
public abstract class OpenTypeConverter {

  private final Type targetType;

  /**
   * The Java class corresponding to getOpenType(). This is the class named by
   * getOpenType().getClassName(), except that it may be a primitive type or an array of primitive
   * type.
   */
  private final OpenType openType;

  private final Class openClass;

  private static class ConverterMap extends WeakHashMap<Type, WeakReference<OpenTypeConverter>> {
  }

  @MakeNotStatic
  private static final ConverterMap converterMap = new ConverterMap();

  @MakeNotStatic
  private static final Map<Type, Type> inProgress = OpenTypeUtil.newIdentityHashMap();

  /**
   * Following List simply serves to keep a reference to predefined OpenConverters so they don't get
   * garbage collected.
   */
  @MakeNotStatic
  private static final List<OpenTypeConverter> preDefinedConverters = OpenTypeUtil.newList();

  protected OpenTypeConverter(Type targetType, OpenType openType, Class openClass) {
    this.targetType = targetType;
    this.openType = openType;
    this.openClass = openClass;
  }

  /**
   * Convert an instance of openClass into an instance of targetType.
   *
   * @return the java type object
   */
  public Object fromOpenValue(Object value) throws InvalidObjectException {
    if (value == null) {
      return null;
    } else {
      return fromNonNullOpenValue(value);
    }
  }

  abstract Object fromNonNullOpenValue(Object value) throws InvalidObjectException;

  /**
   * Throw an appropriate InvalidObjectException if we will not be able to convert back from the
   * open data to the original Java object.
   */
  void checkReconstructible() throws InvalidObjectException {
    // subclasses can override
  }

  /**
   * Convert an instance of targetType into an instance of openClass.
   *
   * @return open class object
   */
  Object toOpenValue(Object value) throws OpenDataException {
    if (value == null) {
      return null;
    } else {
      return toNonNullOpenValue(value);
    }
  }

  abstract Object toNonNullOpenValue(Object value) throws OpenDataException;

  /**
   * @return True if and only if this OpenTypeConverter's toOpenValue and fromOpenValue methods are
   *         the identity function.
   */
  boolean isIdentity() {
    return false;
  }

  Type getTargetType() {
    return targetType;
  }

  OpenType getOpenType() {
    return openType;
  }

  Class getOpenClass() {
    return openClass;
  }

  /**
   * @return a converter corresponding to a type
   */
  private static synchronized OpenTypeConverter getConverter(Type type) {

    if (type instanceof GenericArrayType) {
      Type component = ((GenericArrayType) type).getGenericComponentType();
      if (component instanceof Class) {
        type = Array.newInstance((Class<?>) component, 0).getClass();
      }
    }

    WeakReference<OpenTypeConverter> wr = converterMap.get(type);
    return (wr == null) ? null : wr.get();
  }

  /**
   * Put the converter in the map to avoid future creation
   */
  private static synchronized void putConverter(Type type, OpenTypeConverter conv) {
    WeakReference<OpenTypeConverter> wr = new WeakReference<>(conv);
    converterMap.put(type, wr);
  }

  private static synchronized void putPreDefinedConverter(Type type, OpenTypeConverter conv) {
    putConverter(type, conv);
    preDefinedConverters.add(conv);
  }

  /*
   * Static block to initialize pre defined convertor
   */
  static {

    final OpenType[] simpleTypes = {BIGDECIMAL, BIGINTEGER, BOOLEAN, BYTE, CHARACTER, DATE, DOUBLE,
        FLOAT, INTEGER, LONG, OBJECTNAME, SHORT, STRING, VOID,};

    for (final OpenType t : simpleTypes) {
      Class c;
      try {
        c = Class.forName(t.getClassName(), false, ObjectName.class.getClassLoader());
      } catch (ClassNotFoundException e) {
        throw new Error(e);
      }
      final OpenTypeConverter conv = new IdentityConverter(c, t, c);
      putPreDefinedConverter(c, conv);

      if (c.getName().startsWith("java.lang.")) {
        try {
          final Field typeField = c.getField("TYPE");
          final Class primitiveType = (Class) typeField.get(null);
          final OpenTypeConverter primitiveConv =
              new IdentityConverter(primitiveType, t, primitiveType);
          putPreDefinedConverter(primitiveType, primitiveConv);
          if (primitiveType != void.class) {
            final Class primitiveArrayType = Array.newInstance(primitiveType, 0).getClass();
            final OpenType primitiveArrayOpenType =
                ArrayType.getPrimitiveArrayType(primitiveArrayType);
            final OpenTypeConverter primitiveArrayConv = new IdentityConverter(primitiveArrayType,
                primitiveArrayOpenType, primitiveArrayType);
            putPreDefinedConverter(primitiveArrayType, primitiveArrayConv);
          }
        } catch (NoSuchFieldException ignored) {

        } catch (IllegalAccessException e) {
          assert (false);
        }
      }
    }
  }

  /**
   * @return the converter for the given Java type, creating it if necessary
   */
  public static synchronized OpenTypeConverter toConverter(Type objType) throws OpenDataException {

    if (inProgress.containsKey(objType)) {
      throw new OpenDataException("Recursive data structure, including " + typeName(objType));
    }

    OpenTypeConverter conv;

    conv = getConverter(objType);
    if (conv != null) {
      return conv;
    }

    inProgress.put(objType, objType);
    try {
      conv = makeConverter(objType);
    } catch (OpenDataException e) {
      throw openDataException("Cannot convert type: " + objType, e);
    } finally {
      inProgress.remove(objType);
    }

    putConverter(objType, conv);
    return conv;
  }

  /**
   * @return the open type converter for a given type
   */
  private static OpenTypeConverter makeConverter(Type objType) throws OpenDataException {

    if (objType instanceof GenericArrayType) {
      Type componentType = ((GenericArrayType) objType).getGenericComponentType();
      return makeArrayOrCollectionConverter(objType, componentType);
    } else if (objType instanceof Class) {
      Class objClass = (Class<?>) objType;
      if (objClass.isEnum()) {
        return makeEnumConverter(objClass);
      } else if (objClass.isArray()) {
        Type componentType = objClass.getComponentType();
        return makeArrayOrCollectionConverter(objClass, componentType);
      } else if (JMX.isMXBeanInterface(objClass)) {
        throw openDataException("Cannot obtain array class",
            new ManagementException(" MXBean as an Return Type is not supported"));
      } else {
        return makeCompositeConverter(objClass);
      }
    } else if (objType instanceof ParameterizedType) {
      return makeParameterizedConverter((ParameterizedType) objType);
    } else {
      throw new OpenDataException("Cannot map type: " + objType);
    }
  }

  private static <T extends Enum<T>> OpenTypeConverter makeEnumConverter(Class<T> enumClass) {
    return new EnumConverter<>(enumClass);
  }

  private static OpenTypeConverter makeArrayOrCollectionConverter(Type collectionType,
      Type elementType) throws OpenDataException {

    final OpenTypeConverter elementConverter = toConverter(elementType);
    final OpenType elementOpenType = elementConverter.getOpenType();
    final ArrayType openType = new ArrayType(1, elementOpenType);
    final Class elementOpenClass = elementConverter.getOpenClass();

    final Class openArrayClass;
    final String openArrayClassName;
    if (elementOpenClass.isArray()) {
      openArrayClassName = "[" + elementOpenClass.getName();
    } else {
      openArrayClassName = "[L" + elementOpenClass.getName() + ";";
    }
    try {
      openArrayClass = Class.forName(openArrayClassName);
    } catch (ClassNotFoundException e) {
      throw openDataException("Cannot obtain array class", e);
    }

    if (collectionType instanceof ParameterizedType) {
      return new CollectionConverter(collectionType, openType, openArrayClass, elementConverter);
    } else {
      if (elementConverter.isIdentity()) {
        return new IdentityConverter(collectionType, openType, openArrayClass);
      } else {
        return new ArrayConverter(collectionType, openType, openArrayClass, elementConverter);
      }
    }
  }

  @Immutable
  protected static final String[] keyArray = {"key"};

  @Immutable
  protected static final String[] keyValueArray = {"key", "value"};

  private static OpenTypeConverter makeTabularConverter(Type objType, boolean sortedMap,
      Type keyType, Type valueType) throws OpenDataException {

    final String objTypeName = objType.toString();
    final OpenTypeConverter keyConverter = toConverter(keyType);
    final OpenTypeConverter valueConverter = toConverter(valueType);
    final OpenType keyOpenType = keyConverter.getOpenType();
    final OpenType valueOpenType = valueConverter.getOpenType();
    final CompositeType rowType = new CompositeType(objTypeName, objTypeName, keyValueArray,
        keyValueArray, new OpenType[] {keyOpenType, valueOpenType});
    final TabularType tabularType = new TabularType(objTypeName, objTypeName, rowType, keyArray);
    return new TableConverter(objType, sortedMap, tabularType, keyConverter, valueConverter);
  }

  /**
   * Supported types are List<E>, Set<E>, SortedSet<E>, Map<K,V>, SortedMap<K,V>.
   *
   * Subclasses of the above types wont be supported as deserialize info wont be there.
   *
   * Queue<E> won't be supported as Queue is more of a functional data structure rather than a data
   * holder
   *
   * @return the open type converter for a given type
   */
  private static OpenTypeConverter makeParameterizedConverter(ParameterizedType objType)
      throws OpenDataException {

    final Type rawType = objType.getRawType();

    if (rawType instanceof Class) {
      Class c = (Class<?>) rawType;
      if (c == List.class || c == Set.class || c == SortedSet.class) {
        Type[] actuals = objType.getActualTypeArguments();
        assert (actuals.length == 1);
        if (c == SortedSet.class) {
          mustBeComparable(c, actuals[0]);
        }
        return makeArrayOrCollectionConverter(objType, actuals[0]);
      } else {
        boolean sortedMap = (c == SortedMap.class);
        if (c == Map.class || sortedMap) {
          Type[] actuals = objType.getActualTypeArguments();
          assert (actuals.length == 2);
          if (sortedMap) {
            mustBeComparable(c, actuals[0]);
          }
          return makeTabularConverter(objType, sortedMap, actuals[0], actuals[1]);
        }
      }
    }
    throw new OpenDataException("Cannot convert type: " + objType);
  }

  /**
   * @return the open type converrter for a given type
   */
  private static OpenTypeConverter makeCompositeConverter(Class c) throws OpenDataException {

    final Method[] methods = c.getMethods();
    final SortedMap<String, Method> getterMap = OpenTypeUtil.newSortedMap();

    for (Method method : methods) {
      final String propertyName = propertyName(method);

      if (propertyName == null) {
        continue;
      }

      Method old = getterMap.put(OpenTypeUtil.toCamelCase(propertyName), method);
      if (old != null) {
        final String msg = "Class " + c.getName() + " has method name clash: " + old.getName()
            + ", " + method.getName();
        throw new OpenDataException(msg);
      }
    }

    final int nitems = getterMap.size();

    if (nitems == 0) {
      throw new OpenDataException("Can't map " + c.getName() + " to an open data type");
    }

    final Method[] getters = new Method[nitems];
    final String[] itemNames = new String[nitems];
    final OpenType[] openTypes = new OpenType[nitems];
    int i = 0;
    for (Map.Entry<String, Method> entry : getterMap.entrySet()) {
      itemNames[i] = entry.getKey();
      final Method getter = entry.getValue();
      getters[i] = getter;
      final Type retType = getter.getGenericReturnType();
      openTypes[i] = toConverter(retType).getOpenType();
      i++;
    }

    CompositeType compositeType = new CompositeType(c.getName(), c.getName(), itemNames, // field
                                                                                         // names
        itemNames, // field descriptions
        openTypes);

    return new CompositeConverter(c, compositeType, itemNames, getters);
  }

  /**
   * Converts from a CompositeData to an instance of the targetClass Various subclasses override its
   * functionality.
   */
  protected abstract static class CompositeBuilder {
    CompositeBuilder(Class targetClass, String[] itemNames) {
      this.targetClass = targetClass;
      this.itemNames = itemNames;
    }

    Class getTargetClass() {
      return targetClass;
    }

    String[] getItemNames() {
      return itemNames;
    }

    /**
     * If the subclass should be appropriate but there is a problem, then the method throws
     * InvalidObjectException.
     *
     * @return If the subclass is appropriate for targetClass, then the method returns null. If the
     *         subclass is not appropriate, then the method returns an explanation of why not.
     */
    abstract String applicable(Method[] getters) throws InvalidObjectException;

    /**
     *
     * @return possible cause if target class is not applicable
     */
    Throwable possibleCause() {
      return null;
    }

    /**
     * @return Actual java types from the composite type
     */
    abstract Object fromCompositeData(CompositeData cd, String[] itemNames,
        OpenTypeConverter[] converters) throws InvalidObjectException;

    private final Class targetClass;
    private final String[] itemNames;
  }

  /**
   * Builder if the target class has a method "public static from(CompositeData)"
   */
  protected static class CompositeBuilderViaFrom extends CompositeBuilder {

    CompositeBuilderViaFrom(Class targetClass, String[] itemNames) {
      super(targetClass, itemNames);
    }

    @Override
    String applicable(Method[] getters) throws InvalidObjectException {
      // See if it has a method "T from(CompositeData)"
      // as is conventional for a CompositeDataView
      Class targetClass = getTargetClass();
      try {
        Method fromMethod = targetClass.getMethod("from", CompositeData.class);

        if (!Modifier.isStatic(fromMethod.getModifiers())) {
          final String msg = "Method from(CompositeData) is not static";
          throw new InvalidObjectException(msg);
        }

        if (fromMethod.getReturnType() != getTargetClass()) {
          final String msg = "Method from(CompositeData) returns "
              + typeName(fromMethod.getReturnType()) + " not " + typeName(targetClass);
          throw new InvalidObjectException(msg);
        }

        this.fromMethod = fromMethod;
        return null;
      } catch (InvalidObjectException e) {
        throw e;
      } catch (Exception e) {
        return "no method from(CompositeData)";
      }
    }

    @Override
    Object fromCompositeData(CompositeData cd, String[] itemNames, OpenTypeConverter[] converters)
        throws InvalidObjectException {
      try {
        return fromMethod.invoke(null, cd);
      } catch (Exception e) {
        final String msg = "Failed to invoke from(CompositeData)";
        throw invalidObjectException(msg, e);
      }
    }

    private Method fromMethod;
  }

  /**
   * This builder never actually returns success. It simply serves to check whether the other
   * builders in the same group have any chance of success. If any getter in the targetClass returns
   * a type that we don't know how to reconstruct, then we will not be able to make a builder, and
   * there is no point in repeating the error about the problematic getter as many times as there
   * are candidate builders. Instead, the "applicable" method will return an explanatory string, and
   * the other builders will be skipped. If all the getters are OK, then the "applicable" method
   * will return an empty string and the other builders will be tried.
   */
  protected static class CompositeBuilderCheckGetters extends CompositeBuilder {
    CompositeBuilderCheckGetters(Class targetClass, String[] itemNames,
        OpenTypeConverter[] getterConverters) {
      super(targetClass, itemNames);
      this.getterConverters = getterConverters;
    }

    @Override
    String applicable(Method[] getters) {
      for (int i = 0; i < getters.length; i++) {
        try {
          getterConverters[i].checkReconstructible();
        } catch (InvalidObjectException e) {
          possibleCause = e;
          return "method " + getters[i].getName() + " returns type "
              + "that cannot be mapped back from OpenData";
        }
      }
      return "";
    }

    @Override
    Throwable possibleCause() {
      return possibleCause;
    }

    @Override
    Object fromCompositeData(CompositeData cd, String[] itemNames, OpenTypeConverter[] converters) {
      throw new Error();
    }

    private final OpenTypeConverter[] getterConverters;
    private Throwable possibleCause;
  }

  /**
   * Builder if the target class has a setter for every getter
   */
  protected static class CompositeBuilderViaSetters extends CompositeBuilder {

    CompositeBuilderViaSetters(Class targetClass, String[] itemNames) {
      super(targetClass, itemNames);
    }

    @Override
    String applicable(Method[] getters) {
      try {
        Constructor c = getTargetClass().getConstructor((Class[]) null);
      } catch (Exception e) {
        return "does not have a public no-arg constructor";
      }

      Method[] setters = new Method[getters.length];
      for (int i = 0; i < getters.length; i++) {
        Method getter = getters[i];
        Class returnType = getter.getReturnType();
        String name = propertyName(getter);
        String setterName = "set" + name;
        Method setter;
        try {
          setter = getTargetClass().getMethod(setterName, returnType);
          if (setter.getReturnType() != void.class) {
            throw new Exception();
          }
        } catch (Exception e) {
          return "not all getters have corresponding setters " + "(" + getter + ")";
        }
        setters[i] = setter;
      }
      this.setters = setters;
      return null;
    }

    @Override
    Object fromCompositeData(CompositeData cd, String[] itemNames, OpenTypeConverter[] converters)
        throws InvalidObjectException {
      Object o;
      try {
        o = getTargetClass().newInstance();
        for (int i = 0; i < itemNames.length; i++) {
          if (cd.containsKey(itemNames[i])) {
            Object openItem = cd.get(itemNames[i]);
            Object javaItem = converters[i].fromOpenValue(openItem);
            setters[i].invoke(o, javaItem);
          }
        }
      } catch (Exception e) {
        throw invalidObjectException(e);
      }
      return o;
    }

    private Method[] setters;
  }

  /**
   * Builder if the target class has a constructor that is annotated with @ConstructorProperties so
   * we can derive the corresponding getters.
   */
  protected static class CompositeBuilderViaConstructor extends CompositeBuilder {

    CompositeBuilderViaConstructor(Class targetClass, String[] itemNames) {
      super(targetClass, itemNames);
    }

    @Override
    String applicable(Method[] getters) throws InvalidObjectException {

      final Class<ConstructorProperties> propertyNamesClass = ConstructorProperties.class;

      Class targetClass = getTargetClass();
      Constructor[] constrs = targetClass.getConstructors();

      List<Constructor> annotatedConstrList = OpenTypeUtil.newList();
      for (Constructor constr : constrs) {
        if (Modifier.isPublic(constr.getModifiers())
            && constr.getAnnotation(propertyNamesClass) != null) {
          annotatedConstrList.add(constr);
        }
      }

      if (annotatedConstrList.isEmpty()) {
        return "no constructor has @ConstructorProperties annotation";
      }

      annotatedConstructors = OpenTypeUtil.newList();

      Map<String, Integer> getterMap = OpenTypeUtil.newMap();
      String[] itemNames = getItemNames();
      for (int i = 0; i < itemNames.length; i++) {
        getterMap.put(itemNames[i], i);
      }

      Set<BitSet> getterIndexSets = OpenTypeUtil.newSet();
      for (Constructor constr : annotatedConstrList) {
        String[] propertyNames =
            ((ConstructorProperties) constr.getAnnotation(propertyNamesClass)).value();

        Type[] paramTypes = constr.getGenericParameterTypes();
        if (paramTypes.length != propertyNames.length) {
          final String msg = "Number of constructor params does not match "
              + "@ConstructorProperties annotation: " + constr;
          throw new InvalidObjectException(msg);
        }

        for (int i = 0; i < paramTypes.length; i++) {
          paramTypes[i] = fixType(paramTypes[i]);
        }

        int[] paramIndexes = new int[getters.length];
        for (int i = 0; i < getters.length; i++) {
          paramIndexes[i] = -1;
        }
        BitSet present = new BitSet();

        for (int i = 0; i < propertyNames.length; i++) {
          String propertyName = propertyNames[i];
          if (!getterMap.containsKey(propertyName)) {
            String msg = "@ConstructorProperties includes name " + propertyName
                + " which does not correspond to a property";
            for (String getterName : getterMap.keySet()) {
              if (getterName.equalsIgnoreCase(propertyName)) {
                msg += " (differs only in case from property " + getterName + ")";
              }
            }
            msg += ": " + constr;
            throw new InvalidObjectException(msg);
          }
          int getterIndex = getterMap.get(propertyName);
          paramIndexes[getterIndex] = i;
          if (present.get(getterIndex)) {
            final String msg = "@ConstructorProperties contains property " + propertyName
                + " more than once: " + constr;
            throw new InvalidObjectException(msg);
          }
          present.set(getterIndex);
          Method getter = getters[getterIndex];
          Type propertyType = getter.getGenericReturnType();
          if (!propertyType.equals(paramTypes[i])) {
            final String msg = "@ConstructorProperties gives property " + propertyName + " of type "
                + propertyType + " for parameter " + " of type " + paramTypes[i] + ": " + constr;
            throw new InvalidObjectException(msg);
          }
        }

        if (!getterIndexSets.add(present)) {
          final String msg = "More than one constructor has a @ConstructorProperties "
              + "annotation with this set of names: " + Arrays.toString(propertyNames);
          throw new InvalidObjectException(msg);
        }

        Constr c = new Constr(constr, paramIndexes, present);
        annotatedConstructors.add(c);
      }

      for (BitSet a : getterIndexSets) {
        boolean seen = false;
        for (BitSet b : getterIndexSets) {
          if (a == b) {
            seen = true;
          } else if (seen) {
            BitSet u = new BitSet();
            u.or(a);
            u.or(b);
            if (!getterIndexSets.contains(u)) {
              Set<String> names = new TreeSet<>();
              for (int i = u.nextSetBit(0); i >= 0; i = u.nextSetBit(i + 1)) {
                names.add(itemNames[i]);
              }
              final String msg = "Constructors with @ConstructorProperties annotation "
                  + " would be ambiguous for these items: " + names;
              throw new InvalidObjectException(msg);
            }
          }
        }
      }

      return null;
    }

    @Override
    Object fromCompositeData(CompositeData cd, String[] itemNames, OpenTypeConverter[] converters)
        throws InvalidObjectException {

      CompositeType ct = cd.getCompositeType();
      BitSet present = new BitSet();
      for (int i = 0; i < itemNames.length; i++) {
        if (ct.getType(itemNames[i]) != null) {
          present.set(i);
        }
      }

      Constr max = null;
      for (Constr constr : annotatedConstructors) {
        if (subset(constr.presentParams, present)
            && (max == null || subset(max.presentParams, constr.presentParams))) {
          max = constr;
        }
      }

      if (max == null) {
        final String msg = "No constructor has a @ConstructorProperties for this set of "
            + "items: " + ct.keySet();
        throw new InvalidObjectException(msg);
      }

      Object[] params = new Object[max.presentParams.cardinality()];
      for (int i = 0; i < itemNames.length; i++) {
        if (!max.presentParams.get(i)) {
          continue;
        }
        Object openItem = cd.get(itemNames[i]);
        Object javaItem = converters[i].fromOpenValue(openItem);
        int index = max.paramIndexes[i];
        if (index >= 0) {
          params[index] = javaItem;
        }
      }

      try {
        return max.constructor.newInstance(params);
      } catch (Exception e) {
        final String msg = "Exception constructing " + getTargetClass().getName();
        throw invalidObjectException(msg, e);
      }
    }

    private static boolean subset(BitSet sub, BitSet sup) {
      BitSet subcopy = (BitSet) sub.clone();
      subcopy.andNot(sup);
      return subcopy.isEmpty();
    }

    private static class Constr {
      final Constructor constructor;
      final int[] paramIndexes;
      final BitSet presentParams;

      Constr(Constructor constructor, int[] paramIndexes, BitSet presentParams) {
        this.constructor = constructor;
        this.paramIndexes = paramIndexes;
        this.presentParams = presentParams;
      }
    }

    private List<Constr> annotatedConstructors;
  }

  /**
   * Builder if the target class is an interface and contains no methods other than getters. Then we
   * can make an instance using a dynamic proxy that forwards the getters to the source
   * CompositeData
   */
  protected static class CompositeBuilderViaProxy extends CompositeBuilder {

    CompositeBuilderViaProxy(Class targetClass, String[] itemNames) {
      super(targetClass, itemNames);
    }

    @Override
    String applicable(Method[] getters) {
      Class targetClass = getTargetClass();
      if (!targetClass.isInterface()) {
        return "not an interface";
      }
      Set<Method> methods = OpenTypeUtil.newSet(Arrays.asList(targetClass.getMethods()));
      methods.removeAll(Arrays.asList(getters));

      String bad = null;
      for (Method m : methods) {
        String mname = m.getName();
        Class[] mparams = m.getParameterTypes();
        try {
          Method om = Object.class.getMethod(mname, mparams);
          if (!Modifier.isPublic(om.getModifiers())) {
            bad = mname;
          }
        } catch (NoSuchMethodException e) {
          bad = mname;
        }

      }
      if (bad != null) {
        return "contains methods other than getters (" + bad + ")";
      }
      return null;
    }

    @Override
    Object fromCompositeData(CompositeData cd, String[] itemNames, OpenTypeConverter[] converters) {
      final Class targetClass = getTargetClass();
      return Proxy.newProxyInstance(targetClass.getClassLoader(), new Class[] {targetClass},
          new CompositeDataInvocationHandler(cd));
    }
  }

  static InvalidObjectException invalidObjectException(String msg, Throwable cause) {
    return new InvalidObjectException(msg);
  }

  static InvalidObjectException invalidObjectException(Throwable cause) {
    return invalidObjectException(cause.getMessage(), cause);
  }

  static OpenDataException openDataException(String msg, Throwable cause) {
    return new OpenDataException(msg);
  }

  static OpenDataException openDataException(Throwable cause) {
    return openDataException(cause.getMessage(), cause);
  }

  static void mustBeComparable(Class collection, Type element) throws OpenDataException {
    if (!(element instanceof Class) || !Comparable.class.isAssignableFrom((Class<?>) element)) {
      final String msg = "Parameter class " + element + " of " + collection.getName()
          + " does not implement " + Comparable.class.getName();
      throw new OpenDataException(msg);
    }
  }

  public static String propertyName(Method m) {
    String rest = null;
    String name = m.getName();
    if (name.startsWith("get")) {
      rest = name.substring(3);
    } else if (name.startsWith("is") && m.getReturnType() == boolean.class) {
      rest = name.substring(2);
    }
    if (rest == null || rest.length() == 0 || m.getParameterTypes().length > 0
        || m.getReturnType() == void.class || name.equals("getClass")) {
      return null;
    }
    return rest;
  }

  protected static String typeName(Type t) {
    if (t instanceof Class<?>) {
      Class<?> c = (Class<?>) t;
      if (c.isArray()) {
        return typeName(c.getComponentType()) + "[]";
      } else {
        return c.getName();
      }
    }
    return t.toString();
  }

  private static Type fixType(Type t) {
    if (!(t instanceof GenericArrayType)) {
      return t;
    }
    GenericArrayType gat = (GenericArrayType) t;
    Type ultimate = ultimateComponentType(gat);
    if (!(ultimate instanceof Class<?>)) {
      return t;
    }
    Class<?> component = (Class<?>) fixType(gat.getGenericComponentType());
    return Array.newInstance(component, 0).getClass();
  }

  private static Type ultimateComponentType(GenericArrayType gat) {
    Type component = gat.getGenericComponentType();
    if (component instanceof GenericArrayType) {
      return ultimateComponentType((GenericArrayType) component);
    } else {
      return component;
    }
  }

}
