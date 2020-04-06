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
package org.apache.geode.pdx.internal;

import java.io.Externalizable;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.RegionService;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.PdxSerializerObject;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.NonPortableClassException;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.unsafe.internal.sun.misc.Unsafe;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * The core of auto serialization which is used in both aspect and reflection-based
 * auto-serialization. This simple manager class is a singleton which tracks the relevant fields for
 * each class which is to be auto-serialized. This class used to be a singleton. But now every
 * instance of ReflectionBasedAutoSerializer will have its own instance of this class. We allow
 * instances of this class to be found so that tests can access internal apis that are not exposed
 * on the public ReflectionBasedAutoSerializer.
 *
 * @since GemFire 6.6
 */

public class AutoSerializableManager {
  private static final Logger logger = LogService.getLogger();

  private static final String INIT_CLASSES_PARAM = "classes";
  private static final String INIT_CHECK_PORTABILITY_PARAM = "check-portability";
  private static final String OPT_IDENTITY = "identity";
  private static final String OPT_EXCLUDE = "exclude";

  /*
   * Map of class and list of fields, we're interested in, for that class.
   */
  private final Map<Class<?>, AutoClassInfo> classMap =
      new CopyOnWriteWeakHashMap<Class<?>, AutoClassInfo>();

  /*
   * Mapping between class patterns and identity field patterns.
   */
  private final List<String[]> identityPatterns = new CopyOnWriteArrayList<String[]>();

  /*
   * Mapping between class patterns and patterns of fields to exclude
   */
  private final List<String[]> excludePatterns = new CopyOnWriteArrayList<String[]>();

  /*
   * This is an internal parameter which, when set either as a system property or via cache.xml will
   * not evaluate any hardcoded excludes. This helps with testing as well as possibly debugging
   * future customer issues.
   */
  public static final String NO_HARDCODED_EXCLUDES_PARAM =
      GeodeGlossary.GEMFIRE_PREFIX + "auto.serialization.no.hardcoded.excludes";

  private boolean noHardcodedExcludes = Boolean.getBoolean(NO_HARDCODED_EXCLUDES_PARAM);


  /*
   * Holds a set of regex patterns which match the list of classes we're interested in.
   */
  private final Set<Pattern> classPatterns = new LinkedHashSet<Pattern>();

  /*
   * Hardcoded set of patterns which we always exclude.
   */
  private Set<Pattern> hardcodedExclusions = new HashSet<Pattern>() {
    {
      add(Pattern.compile("org\\.apache\\.geode\\..*"));
      add(Pattern.compile("com\\.gemstone\\..*"));
      add(Pattern.compile("java\\..*"));
      add(Pattern.compile("javax\\..*"));
    }
  };

  /*
   * Cache of class names which have been determined to be excluded from serialization. Built up
   * within isRelevant().
   */
  private final Set<String> cachedExcludedClasses = new CopyOnWriteHashSet<String>();

  /*
   * Cache of class names which have been determined to be included for serialization. Built up
   * within isRelevant().
   */
  private final Set<String> cachedIncludedClasses = new CopyOnWriteHashSet<String>();

  /*
   * Used to hold the class names which have triggered a warning because they have been determined
   * that they should be auto serialized based on a pattern much but were not because that either do
   * not have a public no-arg constructor or have explicit java serialization code.
   */
  private final Set<String> loggedNoAutoSerializeMsg = new CopyOnWriteHashSet<String>();


  private final ReflectionBasedAutoSerializer owner;

  public ReflectionBasedAutoSerializer getOwner() {
    return this.owner;
  }

  public static AutoSerializableManager create(ReflectionBasedAutoSerializer owner,
      boolean checkPortability, String... patterns) {
    AutoSerializableManager result = new AutoSerializableManager(owner);
    result.reconfigure(checkPortability, patterns);
    return result;
  }

  private AutoSerializableManager(ReflectionBasedAutoSerializer owner) {
    this.owner = owner;
  }

  public Map<Class<?>, AutoClassInfo> getClassMap() {
    return classMap;
  }

  private boolean checkPortability;

  public void setCheckPortability(boolean b) {
    this.checkPortability = b;
  }

  public boolean getCheckPortability() {
    return this.checkPortability;
  }

  public void resetCachedTypes() {
    classMap.clear();
  }

  public void resetCaches() {
    identityPatterns.clear();
    excludePatterns.clear();
    resetCachedTypes();
  }

  public void resetAll() {
    resetCaches();
    this.checkPortability = false;
    classPatterns.clear();
    cachedIncludedClasses.clear();
    cachedExcludedClasses.clear();
    loggedNoAutoSerializeMsg.clear();
    this.noHardcodedExcludes = Boolean.getBoolean(NO_HARDCODED_EXCLUDES_PARAM);
  }

  /*
   * Helper method to determine whether the class of a given object is a class which we are
   * interested in (de)serializing.
   *
   *
   * @return true if the object should be considered for serialization or false otherwise
   */
  private boolean isRelevant(Class<?> clazz) {
    String className = clazz.getName();

    // Do some short-circuiting if possible
    if (cachedIncludedClasses.contains(className)) {
      return true;
    } else if (cachedExcludedClasses.contains(className)) {
      return false;
    }

    boolean result = getOwner().isClassAutoSerialized(clazz);
    if (result) {
      cachedIncludedClasses.add(className);
    } else {
      cachedExcludedClasses.add(className);
    }
    return result;
  }

  /**
   * Determines whether objects of the named class are excluded from auto-serialization. The classes
   * that are excluded are Geode-internal classes and Java classes.
   *
   * @param className Class name including packages.
   * @return <code>true</code> if the named class is excluded.
   */
  public boolean isExcluded(String className) {
    if (!noHardcodedExcludes) {
      for (Pattern p : hardcodedExclusions) {
        if (p.matcher(className).matches()) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean defaultIsClassAutoSerialized(Class<?> clazz) {
    if (clazz.isEnum()) {
      return false;
    }
    String className = clazz.getName();
    if (isExcluded(className)) {
      if (!PdxSerializerObject.class.isAssignableFrom(clazz)) {
        return false;
      }
    }

    for (Pattern p : classPatterns) {
      if (p.matcher(className).matches()) {
        if (hasValidConstructor(clazz, p) && !needsStandardSerialization(clazz, p)) {
          return true;
        } else {
          return false;
        }
      }
    }
    return false;
  }

  /*
   * Helper method to determine whether a class has a default constructor. That's needed so that it
   * can be re-instantiated by PDX on de-serialization.
   *
   */
  private boolean hasValidConstructor(Class<?> clazz, Pattern matchedPattern) {
    if (unsafe != null && !USE_CONSTRUCTOR) {
      // unsafe allows us to create instances without a constructor
      return true;
    }
    try {
      clazz.getConstructor();
      return true;
    } catch (NoSuchMethodException nex) {
      String className = clazz.getName();
      if (!loggedNoAutoSerializeMsg.contains(className)) {
        loggedNoAutoSerializeMsg.add(className);
        logger.warn(
            "Class {} matched with '{}' cannot be auto-serialized due to missing public no-arg constructor. Will attempt using Java serialization.",
            className, matchedPattern.pattern());
      }
      return false;
    }
  }

  private boolean needsStandardSerialization(Class<?> clazz, Pattern matchedPattern) {
    if (Serializable.class.isAssignableFrom(clazz)) {
      if (Externalizable.class.isAssignableFrom(clazz)) {
        String className = clazz.getName();
        if (!loggedNoAutoSerializeMsg.contains(className)) {
          loggedNoAutoSerializeMsg.add(className);
          logger.warn(
              "Class {} matched with '{}' cannot be auto-serialized because it is Externalizable. Java serialization will be used instead of auto-serialization.",
              className, matchedPattern.pattern());
        }
        return true;
      } else {
        if (getPrivateMethod(clazz, "writeObject", new Class[] {ObjectOutputStream.class},
            Void.TYPE)) {
          String className = clazz.getName();
          if (!loggedNoAutoSerializeMsg.contains(className)) {
            loggedNoAutoSerializeMsg.add(className);
            logger.warn(
                "Class {} matched with '{}' cannot be auto-serialized because it has a writeObject(ObjectOutputStream) method. Java serialization will be used instead of auto-serialization.",
                className, matchedPattern.pattern());
          }
          return true;
        } else if (getInheritableMethod(clazz, "writeReplace", null, Object.class)) {
          String className = clazz.getName();
          if (!loggedNoAutoSerializeMsg.contains(className)) {
            loggedNoAutoSerializeMsg.add(className);
            logger.warn(
                "Class {} matched with '{}' cannot be auto-serialized because it has a writeReplace() method. Java serialization will be used instead of auto-serialization.",
                className, matchedPattern.pattern());
          }
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns true if a non-private static method with given signature defined by given class, or
   * false if none found.
   */
  private static boolean getPrivateMethod(Class cl, String name, Class[] argTypes,
      Class returnType) {
    try {
      Method meth = cl.getDeclaredMethod(name, argTypes);
      int mods = meth.getModifiers();
      return ((meth.getReturnType() == returnType) && ((mods & Modifier.STATIC) == 0)
          && ((mods & Modifier.PRIVATE) != 0));
    } catch (NoSuchMethodException ex) {
      return false;
    }
  }

  /**
   * Returns true if a non-static, non-abstract method with given signature provided it is defined
   * by or accessible (via inheritance) by the given class, or false if no match found.
   */
  private static boolean getInheritableMethod(Class cl, String name, Class[] argTypes,
      Class returnType) {
    Method meth = null;
    Class defCl = cl;
    while (defCl != null) {
      try {
        meth = defCl.getDeclaredMethod(name, argTypes);
        break;
      } catch (NoSuchMethodException ex) {
        defCl = defCl.getSuperclass();
      }
    }

    if ((meth == null) || (meth.getReturnType() != returnType)) {
      return false;
    }
    int mods = meth.getModifiers();
    if ((mods & (Modifier.STATIC | Modifier.ABSTRACT)) != 0) {
      return false;
    } else if ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0) {
      return true;
    } else if ((mods & Modifier.PRIVATE) != 0) {
      return (cl == defCl);
    } else {
      return packageEquals(cl, defCl);
    }
  }

  /**
   * Returns true if classes are defined in the same runtime package, false otherwise.
   */
  private static boolean packageEquals(Class cl1, Class cl2) {
    return (cl1.getClassLoader() == cl2.getClassLoader()
        && getPackageName(cl1).equals(getPackageName(cl2)));
  }

  /**
   * Returns package name of given class.
   */
  private static String getPackageName(Class cl) {
    String s = cl.getName();
    int i = s.lastIndexOf('[');
    if (i >= 0) {
      s = s.substring(i + 2);
    }
    i = s.lastIndexOf('.');
    return (i >= 0) ? s.substring(0, i) : "";
  }

  /**
   * Given a class, figure out which fields we're interested in serializing. The class' entire
   * hierarchy will be traversed and used. Transients and statics will be ignored.
   *
   * @param clazz the <code>Class</code> we're interested in
   * @return a list of fields to be used when this class is (de)serialized
   */
  public List<PdxFieldWrapper> getFields(Class<?> clazz) {
    return getClassInfo(clazz).getFields();

  }

  public AutoClassInfo getExistingClassInfo(Class<?> clazz) {
    return classMap.get(clazz);
  }

  public AutoClassInfo getClassInfo(Class<?> clazz) {
    Class<?> tmpClass = clazz;
    AutoClassInfo classInfo = getExistingClassInfo(tmpClass);
    if (classInfo == null) {
      synchronized (classMap) {
        classInfo = classMap.get(tmpClass);
        if (classInfo != null)
          return classInfo;

        List<PdxFieldWrapper> fieldList = new ArrayList<PdxFieldWrapper>();
        List<PdxFieldWrapper> variableLenFields = new ArrayList<PdxFieldWrapper>();

        while (tmpClass != Object.class) {
          Field[] fields = tmpClass.getDeclaredFields();
          for (Field f : fields) {
            if (getOwner().isFieldIncluded(f, clazz)) {
              // Should this be reset at some point?
              f.setAccessible(true);
              FieldType ft = getOwner().getFieldType(f, clazz);
              PdxFieldWrapper fw = PdxFieldWrapper.create(this, f, ft,
                  getOwner().getFieldName(f, clazz), getOwner().transformFieldValue(f, clazz),
                  getOwner().isIdentityField(f, clazz));
              if (ft.isFixedWidth()) {
                fieldList.add(fw);
              } else {
                variableLenFields.add(fw);
              }
            }
          }
          tmpClass = tmpClass.getSuperclass();
        }

        fieldList.addAll(variableLenFields);
        classInfo = new AutoClassInfo(clazz, fieldList);
        logger.info("Auto serializer generating type for {} for fields: {}", clazz,
            classInfo.toFormattedString());
        classMap.put(clazz, classInfo);
      } // end sync
    }
    return classInfo;
  }

  public boolean defaultIsIdentityField(Field f, Class<?> clazz) {
    return fieldMatches(f, clazz.getName(), identityPatterns);
  }

  public boolean defaultIsFieldIncluded(Field f, Class<?> clazz) {
    return !Modifier.isTransient(f.getModifiers()) && !Modifier.isStatic(f.getModifiers())
        && !fieldMatches(f, clazz.getName(), excludePatterns);
  }

  public FieldType defaultGetFieldType(Field f, Class<?> clazz) {
    return FieldType.get(f.getType());
  }

  private static class FieldWrapper {
    private final Field field;

    public FieldWrapper(Field f) {
      this.field = f;
    }

    public Field getField() {
      return this.field;
    }

    public int getInt(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getInt(o);
    }

    public void setInt(Object o, int v) throws IllegalArgumentException, IllegalAccessException {
      this.field.setInt(o, v);
    }

    public boolean getBoolean(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getBoolean(o);
    }

    public void setBoolean(Object o, boolean v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setBoolean(o, v);
    }

    public byte getByte(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getByte(o);
    }

    public void setByte(Object o, byte v) throws IllegalArgumentException, IllegalAccessException {
      this.field.setByte(o, v);
    }

    public short getShort(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getShort(o);
    }

    public void setShort(Object o, short v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setShort(o, v);
    }

    public char getChar(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getChar(o);
    }

    public void setChar(Object o, char v) throws IllegalArgumentException, IllegalAccessException {
      this.field.setChar(o, v);
    }

    public long getLong(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getLong(o);
    }

    public void setLong(Object o, long v) throws IllegalArgumentException, IllegalAccessException {
      this.field.setLong(o, v);
    }

    public float getFloat(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getFloat(o);
    }

    public void setFloat(Object o, float v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setFloat(o, v);
    }

    public double getDouble(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getDouble(o);
    }

    public void setDouble(Object o, double v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setDouble(o, v);
    }

    public Object getObject(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.get(o);
    }

    public void setObject(Object o, Object v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.set(o, v);
    }

    @Override
    public String toString() {
      return field.toString();
    }
  }

  private static class UnsafeFieldWrapper extends FieldWrapper {
    private final long offset;

    public UnsafeFieldWrapper(Field f) {
      super(f);
      this.offset = unsafe.objectFieldOffset(f);
    }

    @Override
    public int getInt(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getInt(o, this.offset);
    }

    @Override
    public void setInt(Object o, int v) throws IllegalArgumentException, IllegalAccessException {
      unsafe.putInt(o, this.offset, v);
    }

    @Override
    public boolean getBoolean(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getBoolean(o, this.offset);
    }

    @Override
    public void setBoolean(Object o, boolean v)
        throws IllegalArgumentException, IllegalAccessException {
      unsafe.putBoolean(o, this.offset, v);
    }

    @Override
    public byte getByte(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getByte(o, this.offset);
    }

    @Override
    public void setByte(Object o, byte v) throws IllegalArgumentException, IllegalAccessException {
      unsafe.putByte(o, this.offset, v);
    }

    @Override
    public short getShort(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getShort(o, this.offset);
    }

    @Override
    public void setShort(Object o, short v)
        throws IllegalArgumentException, IllegalAccessException {
      unsafe.putShort(o, this.offset, v);
    }

    @Override
    public char getChar(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getChar(o, this.offset);
    }

    @Override
    public void setChar(Object o, char v) throws IllegalArgumentException, IllegalAccessException {
      unsafe.putChar(o, this.offset, v);
    }

    @Override
    public long getLong(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getLong(o, this.offset);
    }

    @Override
    public void setLong(Object o, long v) throws IllegalArgumentException, IllegalAccessException {
      unsafe.putLong(o, this.offset, v);
    }

    @Override
    public float getFloat(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getFloat(o, this.offset);
    }

    @Override
    public void setFloat(Object o, float v)
        throws IllegalArgumentException, IllegalAccessException {
      unsafe.putFloat(o, this.offset, v);
    }

    @Override
    public double getDouble(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getDouble(o, this.offset);
    }

    @Override
    public void setDouble(Object o, double v)
        throws IllegalArgumentException, IllegalAccessException {
      unsafe.putDouble(o, this.offset, v);
    }

    @Override
    public Object getObject(Object o) throws IllegalArgumentException, IllegalAccessException {
      return unsafe.getObject(o, this.offset);
    }

    @Override
    public void setObject(Object o, Object v)
        throws IllegalArgumentException, IllegalAccessException {
      unsafe.putObject(o, this.offset, v);
    }
  }

  // unsafe will be null if the Unsafe class is not available or SAFE was requested.
  // We attempt to use Unsafe by default for best performance.
  @Immutable
  private static final Unsafe unsafe;
  static {
    Unsafe tmp = null;
    // only use Unsafe if SAFE was not explicitly requested
    if (!Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "AutoSerializer.SAFE")) {
      try {
        tmp = new Unsafe();
        // only throw an exception if UNSAFE was explicitly requested
      } catch (RuntimeException ex) {
        if (Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "AutoSerializer.UNSAFE")) {
          throw ex;
        }
      } catch (Error ex) {
        if (Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "AutoSerializer.UNSAFE")) {
          throw ex;
        }
      }
    }
    unsafe = tmp;
  }

  public abstract static class PdxFieldWrapper {
    private final FieldWrapper field;
    private final String fieldName;
    private final boolean transformValue;
    private final AutoSerializableManager owner;
    private final boolean isIdentityField;

    protected PdxFieldWrapper(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      FieldWrapper tmp;
      if (unsafe != null) {
        tmp = new UnsafeFieldWrapper(f);
      } else {
        tmp = new FieldWrapper(f);
      }
      this.field = tmp;
      this.fieldName = name;
      this.transformValue = transformValue;
      this.owner = owner;
      this.isIdentityField = isIdentityField;
    }

    public static PdxFieldWrapper create(AutoSerializableManager owner, Field f, FieldType ft,
        String name, boolean transformValue, boolean isIdentityField) {
      switch (ft) {
        case INT:
          return new IntField(owner, f, name, transformValue, isIdentityField);
        case BYTE:
          return new ByteField(owner, f, name, transformValue, isIdentityField);
        case LONG:
          return new LongField(owner, f, name, transformValue, isIdentityField);
        case BOOLEAN:
          return new BooleanField(owner, f, name, transformValue, isIdentityField);
        case CHAR:
          return new CharField(owner, f, name, transformValue, isIdentityField);
        case SHORT:
          return new ShortField(owner, f, name, transformValue, isIdentityField);
        case DOUBLE:
          return new DoubleField(owner, f, name, transformValue, isIdentityField);
        case FLOAT:
          return new FloatField(owner, f, name, transformValue, isIdentityField);
        case STRING:
          return new StringField(owner, f, name, transformValue, isIdentityField);
        case DATE:
          return new DateField(owner, f, name, transformValue, isIdentityField);
        case BYTE_ARRAY:
          return new ByteArrayField(owner, f, name, transformValue, isIdentityField);
        case STRING_ARRAY:
          return new StringArrayField(owner, f, name, transformValue, isIdentityField);
        case ARRAY_OF_BYTE_ARRAYS:
          return new ByteArrayArrayField(owner, f, name, transformValue, isIdentityField);
        case BOOLEAN_ARRAY:
          return new BooleanArrayField(owner, f, name, transformValue, isIdentityField);
        case CHAR_ARRAY:
          return new CharArrayField(owner, f, name, transformValue, isIdentityField);
        case SHORT_ARRAY:
          return new ShortArrayField(owner, f, name, transformValue, isIdentityField);
        case INT_ARRAY:
          return new IntArrayField(owner, f, name, transformValue, isIdentityField);
        case LONG_ARRAY:
          return new LongArrayField(owner, f, name, transformValue, isIdentityField);
        case FLOAT_ARRAY:
          return new FloatArrayField(owner, f, name, transformValue, isIdentityField);
        case DOUBLE_ARRAY:
          return new DoubleArrayField(owner, f, name, transformValue, isIdentityField);
        case OBJECT_ARRAY:
          return new ObjectArrayField(owner, f, name, transformValue, isIdentityField);
        case OBJECT:
          return new ObjectField(owner, f, name, transformValue, isIdentityField);
        default:
          throw new IllegalStateException("unhandled field type " + ft);
      }
    }

    public boolean getCheckPortability() {
      return this.owner.getCheckPortability();
    }

    public Field getField() {
      return this.field.getField();
    }

    public String getName() {
      return this.fieldName;
    }

    public boolean transform() {
      return this.transformValue;
    }

    public abstract void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite);

    public abstract void serializeValue(PdxWriterImpl writer, Object newValue,
        boolean optimizeWrite);

    public abstract void deserialize(InternalPdxReader reader, Object obj);

    public abstract void orderedDeserialize(InternalPdxReader reader, Object obj);

    protected Object readTransformIf(Object o, Object serializedValue)
        throws IllegalArgumentException, IllegalAccessException {
      if (!transform())
        return serializedValue;
      return readTransform(o, serializedValue);
    }

    protected Object readTransform(Object o, Object serializedValue)
        throws IllegalArgumentException, IllegalAccessException {
      return this.owner.getOwner().readTransform(getField(), o.getClass(), serializedValue);
    }

    protected void handleException(boolean serialization, Object obj, Exception ex) {
      AutoSerializableManager.handleException(ex, serialization, getName(), obj);
    }

    protected int getInt(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getInt(o);
    }

    protected void setInt(Object o, int v) throws IllegalArgumentException, IllegalAccessException {
      this.field.setInt(o, v);
    }

    protected boolean getBoolean(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getBoolean(o);
    }

    protected void setBoolean(Object o, boolean v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setBoolean(o, v);
    }

    protected byte getByte(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getByte(o);
    }

    protected void setByte(Object o, byte v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setByte(o, v);
    }

    protected short getShort(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getShort(o);
    }

    protected void setShort(Object o, short v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setShort(o, v);
    }

    protected char getChar(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getChar(o);
    }

    protected void setChar(Object o, char v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setChar(o, v);
    }

    protected long getLong(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getLong(o);
    }

    protected void setLong(Object o, long v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setLong(o, v);
    }

    protected float getFloat(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getFloat(o);
    }

    protected void setFloat(Object o, float v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setFloat(o, v);
    }

    protected double getDouble(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getDouble(o);
    }

    protected void setDouble(Object o, double v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setDouble(o, v);
    }

    protected Object getObject(Object o) throws IllegalArgumentException, IllegalAccessException {
      return this.field.getObject(o);
    }

    protected void setObject(Object o, Object v)
        throws IllegalArgumentException, IllegalAccessException {
      this.field.setObject(o, v);
    }

    public boolean isIdentityField() {
      return this.isIdentityField;
    }

    @Override
    public String toString() {
      return this.fieldName + ": " + field.toString();
    }
  }

  public static class IntField extends PdxFieldWrapper {
    public IntField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeInt(getInt(obj));
        } else {
          writer.writeInt(getName(), getInt(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeInt((Integer) newValue);
      } else {
        writer.writeInt(getName(), (Integer) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readInt(pf)));
          } else {
            setInt(obj, reader.readInt(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readInt()));
        } else {
          setInt(obj, reader.readInt());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ByteField extends PdxFieldWrapper {
    public ByteField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeByte(getByte(obj));
        } else {
          writer.writeByte(getName(), getByte(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeByte((Byte) newValue);
      } else {
        writer.writeByte(getName(), (Byte) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readByte(pf)));
          } else {
            setByte(obj, reader.readByte(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readByte()));
        } else {
          setByte(obj, reader.readByte());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class LongField extends PdxFieldWrapper {
    public LongField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeLong(getLong(obj));
        } else {
          writer.writeLong(getName(), getLong(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeLong((Long) newValue);
      } else {
        writer.writeLong(getName(), (Long) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readLong(pf)));
          } else {
            setLong(obj, reader.readLong(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readLong()));
        } else {
          setLong(obj, reader.readLong());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class BooleanField extends PdxFieldWrapper {
    public BooleanField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeBoolean(getBoolean(obj));
        } else {
          writer.writeBoolean(getName(), getBoolean(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeBoolean((Boolean) newValue);
      } else {
        writer.writeBoolean(getName(), (Boolean) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readBoolean(pf)));
          } else {
            setBoolean(obj, reader.readBoolean(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readBoolean()));
        } else {
          setBoolean(obj, reader.readBoolean());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class CharField extends PdxFieldWrapper {
    public CharField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeChar(getChar(obj));
        } else {
          writer.writeChar(getName(), getChar(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeChar((Character) newValue);
      } else {
        writer.writeChar(getName(), (Character) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readChar(pf)));
          } else {
            setChar(obj, reader.readChar(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readChar()));
        } else {
          setChar(obj, reader.readChar());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ShortField extends PdxFieldWrapper {
    public ShortField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeShort(getShort(obj));
        } else {
          writer.writeShort(getName(), getShort(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeShort((Short) newValue);
      } else {
        writer.writeShort(getName(), (Short) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readShort(pf)));
          } else {
            setShort(obj, reader.readShort(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readShort()));
        } else {
          setShort(obj, reader.readShort());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class FloatField extends PdxFieldWrapper {
    public FloatField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeFloat(getFloat(obj));
        } else {
          writer.writeFloat(getName(), getFloat(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeFloat((Float) newValue);
      } else {
        writer.writeFloat(getName(), (Float) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readFloat(pf)));
          } else {
            setFloat(obj, reader.readFloat(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readFloat()));
        } else {
          setFloat(obj, reader.readFloat());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class DoubleField extends PdxFieldWrapper {
    public DoubleField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        if (optimizeWrite) {
          writer.writeDouble(getDouble(obj));
        } else {
          writer.writeDouble(getName(), getDouble(obj));
        }
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeDouble((Double) newValue);
      } else {
        writer.writeDouble(getName(), (Double) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          if (transform()) {
            setObject(obj, readTransform(obj, reader.readDouble(pf)));
          } else {
            setDouble(obj, reader.readDouble(pf));
          }
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        if (transform()) {
          setObject(obj, readTransform(obj, reader.readDouble()));
        } else {
          setDouble(obj, reader.readDouble());
        }
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ObjectField extends PdxFieldWrapper {
    public ObjectField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeObject(newValue, getCheckPortability());
      } else {
        writer.writeObject(getName(), newValue, getCheckPortability());
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readObject(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readObject()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class StringField extends PdxFieldWrapper {
    public StringField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeString((String) newValue);
      } else {
        writer.writeString(getName(), (String) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readString(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readString()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class DateField extends PdxFieldWrapper {
    public DateField(AutoSerializableManager owner, Field f, String name, boolean transformValue,
        boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeDate((Date) newValue);
      } else {
        writer.writeDate(getName(), (Date) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readDate(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readDate()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ByteArrayField extends PdxFieldWrapper {
    public ByteArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeByteArray((byte[]) newValue);
      } else {
        writer.writeByteArray(getName(), (byte[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readByteArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readByteArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class BooleanArrayField extends PdxFieldWrapper {
    public BooleanArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeBooleanArray((boolean[]) newValue);
      } else {
        writer.writeBooleanArray(getName(), (boolean[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readBooleanArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readBooleanArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ShortArrayField extends PdxFieldWrapper {
    public ShortArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeShortArray((short[]) newValue);
      } else {
        writer.writeShortArray(getName(), (short[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readShortArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readShortArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class CharArrayField extends PdxFieldWrapper {
    public CharArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeCharArray((char[]) newValue);
      } else {
        writer.writeCharArray(getName(), (char[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readCharArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readCharArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class IntArrayField extends PdxFieldWrapper {
    public IntArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeIntArray((int[]) newValue);
      } else {
        writer.writeIntArray(getName(), (int[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readIntArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readIntArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class LongArrayField extends PdxFieldWrapper {
    public LongArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeLongArray((long[]) newValue);
      } else {
        writer.writeLongArray(getName(), (long[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readLongArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readLongArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class FloatArrayField extends PdxFieldWrapper {
    public FloatArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeFloatArray((float[]) newValue);
      } else {
        writer.writeFloatArray(getName(), (float[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readFloatArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readFloatArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class DoubleArrayField extends PdxFieldWrapper {
    public DoubleArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeDoubleArray((double[]) newValue);
      } else {
        writer.writeDoubleArray(getName(), (double[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readDoubleArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readDoubleArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class StringArrayField extends PdxFieldWrapper {
    public StringArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeStringArray((String[]) newValue);
      } else {
        writer.writeStringArray(getName(), (String[]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readStringArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readStringArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ByteArrayArrayField extends PdxFieldWrapper {
    public ByteArrayArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeArrayOfByteArrays((byte[][]) newValue);
      } else {
        writer.writeArrayOfByteArrays(getName(), (byte[][]) newValue);
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readArrayOfByteArrays(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readArrayOfByteArrays()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }
  public static class ObjectArrayField extends PdxFieldWrapper {
    public ObjectArrayField(AutoSerializableManager owner, Field f, String name,
        boolean transformValue, boolean isIdentityField) {
      super(owner, f, name, transformValue, isIdentityField);
    }

    @Override
    public void serialize(PdxWriterImpl writer, Object obj, boolean optimizeWrite) {
      try {
        serializeValue(writer, getObject(obj), optimizeWrite);
      } catch (Exception ex) {
        handleException(true, obj, ex);
      }
    }

    @Override
    public void serializeValue(PdxWriterImpl writer, Object newValue, boolean optimizeWrite) {
      if (optimizeWrite) {
        writer.writeObjectArray((Object[]) newValue, getCheckPortability());
      } else {
        writer.writeObjectArray(getName(), (Object[]) newValue, getCheckPortability());
      }
    }

    @Override
    public void deserialize(InternalPdxReader reader, Object obj) {
      PdxField pf = reader.getPdxField(getName());
      if (pf != null) {
        try {
          setObject(obj, readTransformIf(obj, reader.readObjectArray(pf)));
        } catch (Exception ex) {
          handleException(false, obj, ex);
        }
      }
    }

    @Override
    public void orderedDeserialize(InternalPdxReader reader, Object obj) {
      try {
        setObject(obj, readTransformIf(obj, reader.readObjectArray()));
      } catch (Exception ex) {
        handleException(false, obj, ex);
      }
    }
  }

  /**
   * Given an object, use its class to determine which fields are to be used when (de)serializing.
   *
   * @param obj the object whose class we're interested in
   * @return a list of fields to be used when this object's class is (de)serialized
   */
  public List<PdxFieldWrapper> getFields(Object obj) {
    return getFields(obj.getClass());
  }

  /**
   * Using the given PdxWriter, write out the relevant fields for the object instance passed in.
   *
   * @param writer the <code>PdxWriter</code> to use when writing the object
   * @param obj the object to serialize
   * @return <code>true</code> if the object was serialized, <code>false</code> otherwise
   */
  public boolean writeData(PdxWriter writer, Object obj) {
    if (isRelevant(obj.getClass())) {
      writeData(writer, obj, getClassInfo(obj.getClass()));
      return true;
    }
    return false;
  }

  private static void handleException(Exception ex, boolean serialization, String fieldName,
      Object obj) {
    if (ex instanceof CancelException) {
      // fix for bug 43936
      throw (CancelException) ex;
    } else if (ex instanceof NonPortableClassException) {
      throw (NonPortableClassException) ex;
    } else {
      throw new PdxSerializationException((serialization ? "Serialization" : "Deserialization")
          + " error on field " + fieldName + " for class " + obj.getClass().getName(), ex);
    }
  }

  /**
   * Using the given PdxWriter, write out the fields which have been passed in.
   *
   * @param writer the <code>PdxWriter</code> to use when writing the object
   * @param obj the object to serialize
   * @param autoClassInfo a <code>List</code> of <code>Field</code>s which are to be written out
   */
  public void writeData(PdxWriter writer, Object obj, AutoClassInfo autoClassInfo) {
    PdxWriterImpl w = (PdxWriterImpl) writer;
    boolean optimizeFieldWrites = false;
    if (autoClassInfo.getSerializedType() != null) {
      // check to see if we have unread data for this instance
      if (w.initUnreadData() == null) {
        // we don't so we can optimize the field writes since
        // we will write them in the correct order
        optimizeFieldWrites = true;
      }
    }
    for (PdxFieldWrapper f : autoClassInfo.getFields()) {
      // System.out.println("DEBUG writing field=" + f.getField().getName() + " offset=" +
      // ((PdxWriterImpl)writer).position());
      if (f.transform()) {
        try {
          Object newValue =
              getOwner().writeTransform(f.getField(), obj.getClass(), f.getObject(obj));
          f.serializeValue(w, newValue, optimizeFieldWrites);
        } catch (Exception ex) {
          f.handleException(true, obj, ex);
        }
      } else {
        f.serialize(w, obj, optimizeFieldWrites);
      }
      if (f.isIdentityField() && w.definingNewPdxType()) {
        try {
          w.markIdentityField(f.getName());
        } catch (Exception ex) {
          handleException(ex, true, f.getName(), obj);
        }
      }
    }
    if (autoClassInfo.getSerializedType() == null) {
      autoClassInfo.setSerializedType(w.getAutoPdxType());
    }
  }

  private static final boolean USE_CONSTRUCTOR =
      !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "autopdx.ignoreConstructor");

  /**
   * Using the given PdxReader, recreate the given object.
   *
   * @param reader the <code>PdxReader</code> to use when reading the object
   * @param clazz the class of the object to re-create
   */
  public Object readData(PdxReader reader, Class<?> clazz) {
    Object result = null;
    if (isRelevant(clazz)) {
      AutoClassInfo ci = getClassInfo(clazz);
      result = ci.newInstance(clazz);
      InternalPdxReader ri = (InternalPdxReader) reader;
      PdxType pt = ri.getPdxType();
      if (ci.matchesPdxType(pt)) {
        pt.setAutoInfo(ci);
        ri.orderedDeserialize(result, ci);
      } else {
        for (PdxFieldWrapper f : ci.getFields()) {
          f.deserialize(ri, result);
        }
      }
    }
    return result;
  }

  /**
   * Add a new class pattern / identity-field pattern tuple
   *
   * @param classPattern the class pattern
   * @param fieldPattern the pattern to identify a field as an identity field within the given class
   *        pattern
   */
  public void addIdentityPattern(String classPattern, String fieldPattern) {
    identityPatterns.add(new String[] {classPattern, fieldPattern});
  }

  /**
   * Return the identity patterns. The patterns are returned as a <code>List</code> of
   * <code>String</code> arrays of size 2 - essentially a tuple of the form
   *
   * <pre>
   *   (classPattern, identityPattern)
   * </pre>
   *
   * @return the identity patterns
   */
  public List<String[]> getIdentityPatterns() {
    return identityPatterns;
  }

  /**
   * Add a new class pattern / exclude-field pattern tuple
   *
   * @param classPattern the class pattern
   * @param fieldPattern the pattern to exclude a field from serialization within the given class
   *        pattern
   */
  public void addExcludePattern(String classPattern, String fieldPattern) {
    excludePatterns.add(new String[] {classPattern, fieldPattern});
  }

  /**
   * Return the exclude patterns. The patterns are returned as a <code>List</code> of
   * <code>String</code> arrays of size 2 - essentially a tuple of the form
   *
   * <pre>
   *   (classPattern, excludePattern)
   * </pre>
   *
   * @return the exclude patterns
   */
  public List<String[]> getExcludePatterns() {
    return excludePatterns;
  }

  /*
   * Helper method which determines whether a given field matches a set of class/field patterns.
   *
   * @param field the <code>Field</code> to consider
   *
   * @param field the className which references this field
   *
   * @param matches a map containing the
   *
   */
  private boolean fieldMatches(Field field, String className, List<String[]> matches) {
    String fieldName = field.getName();

    for (String[] e : matches) {
      if (className.matches(e[0]) && fieldName.matches(e[1])) {
        return true;
      }
    }

    return false;
  }

  /**
   * Holds meta information about a class that we have auto serialized.
   *
   */
  public static class AutoClassInfo {
    private final WeakReference<Class<?>> clazzRef;
    /**
     * The fields that describe the class
     */
    private final List<PdxFieldWrapper> fields;
    /**
     * The pdxType ids that we are known to exactly match.
     */
    private final Set<Integer> matchingPdxIds = new CopyOnWriteArraySet<Integer>();
    /**
     * The pdxType ids that do not exactly match our class. Either their field order differs it they
     * have extra or missing fields.
     */
    private final Set<Integer> mismatchingPdxIds = new CopyOnWriteArraySet<Integer>();

    /**
     * The PdxType created by the first serialization by the auto serializer.
     */
    private PdxType serializedType = null;

    public AutoClassInfo(Class<?> clazz, List<PdxFieldWrapper> fields) {
      this.clazzRef = new WeakReference<Class<?>>(clazz);
      this.fields = fields;
    }

    public String toFormattedString() {
      StringBuffer sb = new StringBuffer();
      boolean first = true;
      for (Object o : this.fields) {
        if (first) {
          first = false;
          sb.append('\n');
        }
        sb.append("    ").append(o).append('\n');
      }
      return sb.toString();
    }

    public Object newInstance(Class<?> clazz) {
      Object result;
      try {
        if (unsafe != null && !USE_CONSTRUCTOR) {
          result = unsafe.allocateInstance(clazz);
        } else {
          result = clazz.newInstance();
        }
      } catch (Exception ex) {
        throw new PdxSerializationException(
            String.format("Could not create an instance of a class %s",
                clazz.getName()),
            ex);
      }
      return result;
    }

    public void setSerializedType(PdxType v) {
      this.serializedType = v;
    }

    public PdxType getSerializedType() {
      return this.serializedType;
    }

    public Class<?> getInfoClass() {
      return this.clazzRef.get();
    }

    public List<PdxFieldWrapper> getFields() {
      return this.fields;
    }

    public boolean matchesPdxType(PdxType t) {
      Integer pdxTypeId = Integer.valueOf(t.getTypeId());
      if (this.matchingPdxIds.contains(pdxTypeId)) {
        return true;
      } else if (this.mismatchingPdxIds.contains(pdxTypeId)) {
        return false;
      } else if (checkForMatch(t)) {
        this.matchingPdxIds.add(pdxTypeId);
        return true;
      } else {
        this.mismatchingPdxIds.add(pdxTypeId);
        return false;
      }
    }

    private boolean checkForMatch(PdxType t) {
      if (this.fields.size() != t.getUndeletedFieldCount()) {
        return false;
      }
      Iterator<PdxField> pdxIt = t.getFields().iterator();
      for (PdxFieldWrapper f : this.fields) {
        PdxField pdxF = pdxIt.next();
        if (pdxF.isDeleted()) {
          return false; // If the type has a deleted field then we can't do ordered deserialization
                        // because we need to skip over the deleted field's bytes.
        }
        if (!f.getName().equals(pdxF.getFieldName())) {
          return false;
        }
        if (!FieldType.get(f.getField().getType()).equals(pdxF.getFieldType())) {
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return "AutoClassInfo [fields=" + fields + "]";
    }
  }

  public void init(Properties props) {
    resetAll();
    if (props != null) {
      Enumeration<?> it = props.propertyNames();
      while (it.hasMoreElements()) {
        Object o = it.nextElement();
        if (o instanceof String) {
          String key = (String) o;
          if (INIT_CLASSES_PARAM.equals(key)) {
            String propValue = props.getProperty(INIT_CLASSES_PARAM);
            if (propValue != null) {
              processInitParams(propValue);
            }
          } else if (INIT_CHECK_PORTABILITY_PARAM.equals(key)) {
            String propValue = props.getProperty(INIT_CHECK_PORTABILITY_PARAM);
            if (propValue != null) {
              setCheckPortability(Boolean.parseBoolean(propValue));
            }
          } else if (NO_HARDCODED_EXCLUDES_PARAM.equals(key)) {
            if (props.getProperty(NO_HARDCODED_EXCLUDES_PARAM) != null) {
              noHardcodedExcludes = true;
            }
          } else {
            throw new IllegalArgumentException(
                "ReflectionBasedAutoSerializer: unknown init property \"" + key + "\"");
          }
        } else {
          throw new IllegalArgumentException(
              "ReflectionBasedAutoSerializer: unknown non-String init property \"" + o + "\"");
        }
      }
    }
  }

  public Properties getConfig() {
    Properties props = new Properties();
    if (classPatterns.isEmpty()) {
      return props;
    }

    StringBuilder sb = new StringBuilder();
    // This is so that we can exclude duplicates
    // LinkedHashSet is used to preserve the order of classPatterns. See bug 52286.
    Set<String> tmp = new LinkedHashSet<String>();
    for (Pattern p : classPatterns) {
      tmp.add(p.pattern());
    }
    for (Iterator<String> i = tmp.iterator(); i.hasNext();) {
      String s = i.next();
      sb.append(s);
      if (i.hasNext()) {
        sb.append(", ");
      }
    }

    if (getIdentityPatterns().size() > 0) {
      sb.append(", ");
      for (Iterator<String[]> i = getIdentityPatterns().iterator(); i.hasNext();) {
        String[] s = i.next();
        sb.append(s[0]).append("#" + OPT_IDENTITY + "=").append(s[1]);
        if (i.hasNext()) {
          sb.append(", ");
        }
      }
    }

    if (getExcludePatterns().size() > 0) {
      sb.append(", ");
      for (Iterator<String[]> i = getExcludePatterns().iterator(); i.hasNext();) {
        String[] s = i.next();
        sb.append(s[0]).append("#" + OPT_EXCLUDE + "=").append(s[1]);
        if (i.hasNext()) {
          sb.append(", ");
        }
      }
    }

    props.put(INIT_CLASSES_PARAM, sb.toString());

    if (getCheckPortability()) {
      props.put(INIT_CHECK_PORTABILITY_PARAM, "true");
    }

    return props;
  }

  public void reconfigure(boolean b, String... patterns) {
    resetAll();
    setCheckPortability(b);
    for (String c : patterns) {
      processInitParams(c);
    }
  }

  private void processInitParams(String value) {
    String identityPattern;
    String excludePattern;

    for (String s : value.split("[, ]+")) {
      if (s.length() > 0) {
        // Let's check for any additional embedded params...
        String[] split = s.split("#");
        for (int i = 1; i < split.length; i++) {
          identityPattern = null;
          excludePattern = null;

          String[] paramVals = split[i].split("=");
          if (paramVals.length != 2) {
            throw new IllegalArgumentException(
                "Unable to correctly process auto serialization init value: " + value);
          }
          if (OPT_IDENTITY.equalsIgnoreCase(paramVals[0])) {
            identityPattern = paramVals[1];
          } else if (OPT_EXCLUDE.equalsIgnoreCase(paramVals[0])) {
            excludePattern = paramVals[1];
          } else {
            throw new IllegalArgumentException(
                "Unable to correctly process auto serialization init value: " + value);
          }
          if (identityPattern != null) {
            addIdentityPattern(split[0], identityPattern);
          }
          if (excludePattern != null) {
            addExcludePattern(split[0], excludePattern);
          }
        }
        classPatterns.add(Pattern.compile(split[0]));
      }
    }
  }

  private RegionService cache;

  public RegionService getRegionService() {
    return this.cache;
  }

  public void setRegionService(RegionService rs) {
    this.cache = rs;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (checkPortability ? 1231 : 1237);
    result = prime * result + classPatterns.hashCode();
    result = prime * result + excludePatterns.hashCode();
    result = prime * result + identityPatterns.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    AutoSerializableManager other = (AutoSerializableManager) obj;
    if (checkPortability != other.checkPortability)
      return false;
    if (!classPatterns.equals(other.classPatterns))
      return false;
    if (!excludePatterns.equals(other.excludePatterns))
      return false;
    if (!identityPatterns.equals(other.identityPatterns))
      return false;
    return true;
  }
}
