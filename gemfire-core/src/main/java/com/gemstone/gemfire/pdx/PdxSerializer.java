/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

/**
 * The PdxSerializer interface allows domain classes to be 
 * serialized and deserialized as PDXs without modification 
 * of the domain class. It only requires that the domain class 
 * have a public zero-arg constructor and that it provides read and write access
 * to the PDX serialized fields.
 * <p>GemFire allows a single PdxSerializer to be configured on a cache
 * using {@link com.gemstone.gemfire.cache.CacheFactory#setPdxSerializer(PdxSerializer) setPdxSerializer}
 * or {@link com.gemstone.gemfire.cache.client.ClientCacheFactory#setPdxSerializer(PdxSerializer) client setPdxSerializer}.
 * It can also be configured in <code>cache.xml</code> using the <code>pdx-serializer</code> element.
 * The same PdxSerializer should be configured on each member of a distributed
 * system that can serialize or deserialize PDX data.
 * <p>The {@link #toData toData} method is used for serialization; {@link #fromData fromData} is used for deserialization.
 * The order in which fields are serialized and deserialized in these methods for the same class must match.
 * For the same class toData and fromData must write the same fields each time they are called.
 * <p>If you can modify your domain class then use {@link PdxSerializable} instead.
 * 
 *<p>Simple example:
 * <PRE>
public class User {
  final public String name;
  final public int userId;

  public User(String name, int userId) {
    this.name = name;
    this.userId = userId;
  }
}

public class MyPdxSerializer implements PdxSerializer {
  public boolean toData(Object o, PdxWriter out) {
    if (o instanceof User) {
      User u = (User)o;
      out.writeString("name", u.name);
      out.writeInt("userId", u.userId);
      return true;
    } else {
      return false;
    }
  }
  public Object fromData(Class<?> clazz, PdxReader in) {
    if (User.class.isAssignableFrom(clazz)) {
      return new User(in.readString("name"), in.readInt("userId"));
    } else {
      return null;
    }
  }
}
 * </PRE>
 * 
 * @author darrel
 * @since 6.6
 */
public interface PdxSerializer {
  /**
   * This method is given an object to serialize as a PDX using the given writer.
   * If it chooses to do the serialization then it must return <code>true</code>;
   * otherwise it must return <code>false</code> in which case it will be serialized using
   * standard serialization.
   * 
   * @param o the object to consider serializing as a PDX
   * @param out the {@link PdxWriter} to use to serialize the object
   * @return <code>true</code> if the method serialized the object; otherwise <code>false</code>
   */
  public boolean toData(Object o, PdxWriter out);

  /**
   * This method is given an class that should be
   * instantiated and deserialized using the given reader.
   * @param clazz the Class of the object that should be deserialized
   * @param in the reader to use to obtain the field data
   * @return the deserialized object. <code>null</code> indicates that this
   * PdxSerializer does not know how to deserialize the given class.
   */
  public Object fromData(Class<?> clazz, PdxReader in);
}
