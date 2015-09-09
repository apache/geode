/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.pdx.internal.AutoSerializableManager;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * This class uses Java reflection in conjunction with
 * {@link com.gemstone.gemfire.pdx.PdxSerializer PdxSerialzer} to perform
 * automatic serialization of domain objects. The implication is that the domain
 * classes do not need to implement the <code>PdxSerializable</code> interface.
 * <p>
 * This implementation will serialize all relevant fields
 * <p>
 * For example:
 * 
 * <pre>
 * Cache c = new CacheFactory().set(&quot;cache-xml-file&quot;, cacheXmlFileName)
 *     .setPdxSerializer(new ReflectionBasedAutoSerializer("com.foo.DomainObject"))
 *     .create();
 * </pre>
 * <p>
 * In this example <code>DomainObject</code> would not need to implement
 * <code>PdxSerializable</code> to be serialized.
 * <p>
 * The equivalent <code>cache.xml</code> entries might be as follows:
 * 
 * <pre>
 * &lt;pdx&gt;
 *   &lt;pdx-serializer&gt;
 *     &lt;class-name&gt;
 *       com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer
 *     &lt;/class-name&gt;
 *     &lt;parameter name="classes"&gt;
 *       &lt;string&gt; com.company.domain.DomainObject &lt;/string&gt;
 *     &lt;/parameter&gt;
 *   &lt;/pdx-serializer&gt;
 * &lt;/pdx&gt;
 * </pre>
 * See {@link ReflectionBasedAutoSerializer#reconfigure(String...) reconfigure}
 * for additional details on the format of the parameter string.
 * 
 * @since 6.6
 * @author jens
 * @author darrel
 */
public class ReflectionBasedAutoSerializer implements PdxSerializer, Declarable {

  private final AutoSerializableManager manager;

  /**
   * Default constructor primarily used during declarative configuration via the
   * cache.xml file.
   * Instances created with this constructor will not match any classes
   * so use {@link #ReflectionBasedAutoSerializer(String...)} instead.
   */
  public ReflectionBasedAutoSerializer() {
    this(new String[0]);
  }

  /**
   * Constructor which takes a list of class name patterns which are to be
   * auto-serialized. Portability of serialization will not be checked.
   * <p>
   * Each string in the list represents a definition in the following form:
   * <pre>
   *   &lt;class pattern&gt;#identity=&lt;identity field pattern&gt;#exclude=&lt;exclude field pattern&gt;
   * </pre>
   * The hash (#) characters are separators and are not part of the parameter
   * name. An example would be:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*#exclude=creationDate
   * </pre>
   * This would select all classes with a class name beginning with <code>com.company.DomainObject</code>
   * and would select as PDX identity fields any fields beginning with <code>id</code>
   * and would not serialize the field called <code>creationDate</code>.
   * <p>
   * There is no association between the the <i>identity</i> and <i>exclude</i> 
   * options, so the above example could also be expressed as:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*
   *   com.company.DomainObject.*#exclude=creationDate
   * </pre>
   * 
   * Note that <u>all</u> defined patterns are used when determining whether a field
   * should be considered as an identity field or should be excluded. Thus the
   * order of the patterns is not relevant.
   * 
   * @param classes the patterns which are matched against domain class
   *  names to determine whether they should be serialized
   *  @deprecated as of 6.6.2 use ReflectionBasedAutoSerializer(String...) instead.
   */
  public ReflectionBasedAutoSerializer(List<String> classes) {
    this(listToArray(classes));
  }
  
  private static String[] listToArray(List<String> l) {
    if (l == null) {
      l = Collections.emptyList();
    }
    return l.toArray(new String[l.size()]);
  }
  /**
   * Constructor which takes a list of class name patterns which are to be
   * auto-serialized. Portability of serialization will not be checked.
   * <p>
   * Each string in the list represents a definition in the following form:
   * <pre>
   *   &lt;class pattern&gt;#identity=&lt;identity field pattern&gt;#exclude=&lt;exclude field pattern&gt;
   * </pre>
   * The hash (#) characters are separators and are not part of the parameter
   * name. An example would be:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*#exclude=creationDate
   * </pre>
   * This would select all classes with a class name beginning with <code>com.company.DomainObject</code>
   * and would select as PDX identity fields any fields beginning with <code>id</code>
   * and would not serialize the field called <code>creationDate</code>.
   * <p>
   * There is no association between the the <i>identity</i> and <i>exclude</i> 
   * options, so the above example could also be expressed as:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*
   *   com.company.DomainObject.*#exclude=creationDate
   * </pre>
   * 
   * Note that <u>all</u> defined patterns are used when determining whether a field
   * should be considered as an identity field or should be excluded. Thus the
   * order of the patterns is not relevant.
   * 
   * @param patterns the patterns which are matched against domain class
   *  names to determine whether they should be serialized
   * @since 6.6.2
   */
  public ReflectionBasedAutoSerializer(String... patterns) {
    this(false, patterns);
  }
  /**
   * Constructor which takes a list of class name patterns which are to be
   * auto-serialized.
   * <p>
   * Each string in the list represents a definition in the following form:
   * <pre>
   *   &lt;class pattern&gt;#identity=&lt;identity field pattern&gt;#exclude=&lt;exclude field pattern&gt;
   * </pre>
   * The hash (#) characters are separators and are not part of the parameter
   * name. An example would be:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*#exclude=creationDate
   * </pre>
   * This would select all classes with a class name beginning with <code>com.company.DomainObject</code>
   * and would select as PDX identity fields any fields beginning with <code>id</code>
   * and would not serialize the field called <code>creationDate</code>.
   * <p>
   * There is no association between the the <i>identity</i> and <i>exclude</i> 
   * options, so the above example could also be expressed as:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*
   *   com.company.DomainObject.*#exclude=creationDate
   * </pre>
   * 
   * Note that <u>all</u> defined patterns are used when determining whether a field
   * should be considered as an identity field or should be excluded. Thus the
   * order of the patterns is not relevant.
   * 
   * @param checkPortability if <code>true</code> then an serialization done by
   *  this serializer will throw an exception if the object it not portable to
   *  non-java languages.
   * @param patterns the patterns which are matched against domain class
   *  names to determine whether they should be serialized
   * @since 6.6.2
   */
  public ReflectionBasedAutoSerializer(boolean checkPortability, String... patterns) {
    // We allow this class to escape its constructor so that our delegate can
    // call back to us when needed. Callbacks will not happen until this instance
    // is fully constructed and registered with the Cache.
    this.manager = AutoSerializableManager.create(this, checkPortability, patterns);
  }

  /**
   * Method to configure classes to consider for serialization, to set any
   * identity fields and to define any fields to exclude from serialization.
   * <p>
   * Each string in the list represents a definition in the following form:
   * <pre>
   *   &lt;class pattern&gt;#identity=&lt;identity field pattern&gt;#exclude=&lt;exclude field pattern&gt;
   * </pre>
   * The hash (#) characters are separators and are not part of the parameter
   * name. An example would be:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*#exclude=creationDate
   * </pre>
   * This would select all classes with a class name beginning with <code>com.company.DomainObject</code>
   * and would select as PDX identity fields any fields beginning with <code>id</code>
   * and would not serialize the field called <code>creationDate</code>.
   * <p>
   * There is no association between the the <i>identity</i> and <i>exclude</i> 
   * options, so the above example could also be expressed as:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*
   *   com.company.DomainObject.*#exclude=creationDate
   * </pre>
   * 
   * Note that <u>all</u> defined patterns are used when determining whether a field
   * should be considered as an identity field or should be excluded. Thus the
   * order of the patterns is not relevant.
   * 
   * @param patterns the list of definitions to apply
   * @deprecated as of 6.6.2 use {@link #reconfigure(String...)} instead.
   */
  public final void setSerializableClasses(List<String> patterns) {
    reconfigure(listToArray(patterns));
  }
  /**
   * Method to reconfigure this serializer. Any previous configuration is cleared.
   * The serializer will not check for portable serialization.
   * <p>
   * Each string in the list represents a definition in the following form:
   * <pre>
   *   &lt;class pattern&gt;#identity=&lt;identity field pattern&gt;#exclude=&lt;exclude field pattern&gt;
   * </pre>
   * The hash (#) characters are separators and are not part of the parameter
   * name. An example would be:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*#exclude=creationDate
   * </pre>
   * This would select all classes with a class name beginning with <code>com.company.DomainObject</code>
   * and would select as PDX identity fields any fields beginning with <code>id</code>
   * and would not serialize the field called <code>creationDate</code>.
   * <p>
   * There is no association between the the <i>identity</i> and <i>exclude</i> 
   * options, so the above example could also be expressed as:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*
   *   com.company.DomainObject.*#exclude=creationDate
   * </pre>
   * 
   * Note that <u>all</u> defined patterns are used when determining whether a field
   * should be considered as an identity field or should be excluded. Thus the
   * order of the patterns is not relevant.
   * 
   * @param patterns the definitions to apply
   * @since 6.6.2
   */
  public final void reconfigure(String... patterns) {
    reconfigure(false, patterns);
  }
  /**
   * Method to reconfigure this serializer. Any previous configuration is cleared.
   * <p>
   * Each string in the list represents a definition in the following form:
   * <pre>
   *   &lt;class pattern&gt;#identity=&lt;identity field pattern&gt;#exclude=&lt;exclude field pattern&gt;
   * </pre>
   * The hash (#) characters are separators and are not part of the parameter
   * name. An example would be:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*#exclude=creationDate
   * </pre>
   * This would select all classes with a class name beginning with <code>com.company.DomainObject</code>
   * and would select as PDX identity fields any fields beginning with <code>id</code>
   * and would not serialize the field called <code>creationDate</code>.
   * <p>
   * There is no association between the the <i>identity</i> and <i>exclude</i> 
   * options, so the above example could also be expressed as:
   * <pre>
   *   com.company.DomainObject.*#identity=id.*
   *   com.company.DomainObject.*#exclude=creationDate
   * </pre>
   * 
   * Note that <u>all</u> defined patterns are used when determining whether a field
   * should be considered as an identity field or should be excluded. Thus the
   * order of the patterns is not relevant.
   * 
   * @param patterns the definitions to apply
   * @param checkPortability if <code>true</code> then an serialization done by
   * this serializer will throw an exception if the object it not portable to
   * non-java languages.
   * @since 6.6.2
   */
  public final void reconfigure(boolean checkPortability, String... patterns) {
    this.manager.reconfigure(checkPortability, patterns);
  }

  /**
   * Method implemented from <code>PdxSerializer</code> which performs object
   * serialization.
   * 
   * @param obj
   *          the object to serialize
   * @param writer
   *          the <code>PdxWriter</code> to use when serializing this object
   * @return <code>true</code> if the object was serialized, <code>false</code>
   *         otherwise
   */
  public boolean toData(Object obj, PdxWriter writer) {
    return manager.writeData(writer, obj);
  }

  /**
   * Method implemented from <code>PdxSerializer</code> which performs object
   * de-serialization.
   * 
   * @param clazz
   *          the class of the object to re-create
   * @param reader
   *          the <code>PdxReader</code> to use when creating this object
   * @return the deserialized object if this serializer handles the given class,
   * null otherwise.
   */
  public Object fromData(Class<?> clazz, PdxReader reader) {
    return manager.readData(reader, clazz);
  }


  /**
   * Used for declarative class initialization from cache.xml. The following
   * property may be specified:
   * <ul>
   * <li><b>classes</b> - a comma-delimited list of strings which represent the
   * patterns used to select classes for serialization, patterns to select
   * identity fields and patterns to exclude fields. See {@link ReflectionBasedAutoSerializer#reconfigure(String...) reconfigure}
   * for specifics.
   * </li>
   * <li><b>check-portability</b> - if true then an exception will be thrown if
   * an attempt to serialize data that is not portable to .NET is made.
   * 
   * @param props
   *          properties used to configure the auto serializer
   */
  public void init(Properties props) {
    this.manager.init(props);
  }

  /**
   * Return a <code>Properties</code> object with a representation of the
   * current config. Depending on how this <code>ReflectionBasedAutoSerializer</code>
   * was configured, the returned property value will have the correct semantics
   * but may differ from the the original configuration string.
   * 
   * @return a <code>Properties</code> object
   */
  public Properties getConfig() {
    return this.manager.getConfig();
  }

  /**
   * Controls what classes will be auto serialized by this serializer.
   * Override this method to customize what classes will be auto serialized.
   * <p>
   * The default implementation:
   * <ul>
   * <li>only serializes classes whose name matches one of the patterns
   * <li>excludes classes whose package begins with "com.gemstone.", "java.", or "javax."
   * unless the system property "gemfire.auto.serialization.no.hardcoded.excludes"
   * is set to "true".
   * <li>excludes classes that do not have a public no-arg constructor
   * <li>excludes enum classes
   * <li>excludes classes that require standard java serialization. A class
   * requires standard java serialization if it extends Externalizable or
   * if it extends Serializable and has either a private writeObject method
   * or a writeReplace method as defined by the java serialization specification.
   * </ul>
   * <p>
   * This method is only called the first time it sees a new class. The result
   * will be remembered and used the next time the same class is seen.
   * @param clazz the class that is being considered for auto serialization.
   * @return true if instances of the class should be auto serialized; false if not.
   * @since 6.6.2
   */
  public boolean isClassAutoSerialized(Class<?> clazz) {
    return this.manager.defaultIsClassAutoSerialized(clazz);
  }

  /**
   * Controls what fields of a class will be auto serialized by this serializer.
   * Override this method to customize what fields of a class will be auto serialized.
   * <p>
   * The default implementation:
   * <ul>
   * <li> excludes transient fields
   * <li> excludes static fields
   * <li> excludes any fields that match an "#exclude=" pattern.
   * </ul>
   * All other fields are included.
   * <p>
   * This method is only called the first time it sees a new class. The result
   * will be remembered and used the next time the same class is seen.
   * @param f the field being considered for serialization
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @return true if the field should be serialized as a pdx field; false if it should be ignored.
   * @since 6.6.2
   */
  public boolean isFieldIncluded(Field f, Class<?> clazz) {
    return this.manager.defaultIsFieldIncluded(f, clazz);
  }
  /**
   * Controls the field name that will be used in pdx for a field being auto serialized.
   * Override this method to customize the field names that will be generated by auto serialization.
   * It allows you to convert a local, language dependent name, to a more portable name.
   * The returned name is the one that will show up in a {@link PdxInstance} and that
   * one that will need to be used to access the field when doing a query.
   * <p>
   * The default implementation returns the name obtained from <code>f</code>.
   * <p>
   * This method is only called the first time it sees a new class. The result
   * will be remembered and used the next time the same class is seen.
   * @param f the field whose name is returned.
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @return the name of the field
   * @since 6.6.2
   */
  public String getFieldName(Field f, Class<?> clazz) {
    return f.getName();
  }

  /**
   * Controls what fields of a class that is auto serialized will be marked
   * as pdx identity fields.
   * Override this method to customize what fields of an auto serialized class will be
   * identity fields.
   * Identity fields are used when a {@link PdxInstance} computes its hash code
   * and checks to see if it is equal to another object.
   * <p>
   * The default implementation only marks fields that match an "#identity=" pattern
   * as identity fields.
   * <p>
   * This method is only called the first time it sees a new class. The result
   * will be remembered and used the next time the same class is seen.
   * @param f the field to test to see if it is an identity field.
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @return true if the field should be marked as an identity field; false if not.
   * @since 6.6.2
   */
  public boolean isIdentityField(Field f, Class<?> clazz) {
    return this.manager.defaultIsIdentityField(f, clazz);
  }

  /**
   * Controls what pdx field type will be used when auto serializing.
   * Override this method to customize what pdx field type will be used
   * for a given domain class field.
   * <p>
   * The default implementation uses {@link FieldType#get(Class)}
   * by passing it {@link Field#getType()}.
   * <p>
   * This method is only called the first time it sees a new class. The result
   * will be remembered and used the next time the same class is seen.
   * @param f the field whose pdx field type needs to be determined
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @return the pdx field type of the given domain class field.
   * @since 6.6.2
   */
  public FieldType getFieldType(Field f, Class<?> clazz) {
    return this.manager.defaultGetFieldType(f, clazz);
  }

  /**
   * Controls if a pdx field's value can be transformed during serialization.
   * Override this method to customize what fields can have their values transformed.
   * If you return true then you need to also override {@link #writeTransform}
   * and {@link #readTransform}.
   * <p>
   * The default implementation returns false.
   * <p>
   * This method is only called the first time it sees a new class. The result
   * will be remembered and used the next time the same class is seen.
   * @param f the field in question
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @return true if the {@link #writeTransform} and {@link #readTransform} need to be called
   * when serializing and deserializing this field's value.
   * @since 6.6.2
   */
  public boolean transformFieldValue(Field f, Class<?> clazz) {
    return false;
  }
  /**
   * Controls what field value is written during auto serialization.
   * Override this method to customize the data that will be written
   * during auto serialization.
   * This method will only be called if {@link #transformFieldValue}
   * returned true.
   * @param f the field in question
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @param originalValue the value of the field that was read from the domain object. 
   * @return the actual value to write for this field. Return <code>originalValue</code>
   *   if you decide not to transform the value. 
   * @since 6.6.2
   */
  public Object writeTransform(Field f, Class<?> clazz, Object originalValue) {
    return originalValue;
  }
  /**
   * Controls what field value is read during auto deserialization.
   * Override this method to customize the data that will be read
   * during auto deserialization.
   * This method will only be called if {@link #transformFieldValue}
   * returned true.
   * @param f the field in question
   * @param clazz the original class being serialized that owns this field.
   *   Note that this field may have been inherited from a super class by this class.
   *   If you want to find the class that declared this field use {@link Field#getDeclaringClass()}.
   * @param serializedValue the value of the field that was serialized for this field.
   * @return the actual value to write for this field. Return <code>serializedValue</code>
   *   if you decide not to transform the value. 
   * @since 6.6.2
   */
  public Object readTransform(Field f, Class<?> clazz, Object serializedValue) {
    return serializedValue;
  }
  /**
   * Returns the cache that this serializer is installed on.
   * Returns null if it is not installed.
   * @since 6.6.2
   */
  public final RegionService getRegionService() {
    return this.manager.getRegionService();
  }
  /**
   * For internal use only.
   * @since 8.2
   */
  public final Object getManager() {
    // The result is not AutoSerializableManager because
    // that class is not part of our public APIs.
    return this.manager;
  }
}