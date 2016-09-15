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
package org.apache.geode;

import java.io.*;

/**
 * An interface for objects whose state can be written/read as
 * primitive types and strings ("data").  That is, instead of
 * serializing itself to an {@link java.io.ObjectOutputStream}, a
 * <code>DataSerializable</code> can serialize itself to a {@link
 * DataOutput}.  By implementing this interface, objects can be
 * serialized faster and in a more compact format than standard Java
 * serialization.  The {@link DataSerializer} class contains a number
 * of static methods that may be helpful to implementations of
 * <code>DataSerializable</code>.
 *
 * <P>
 *
 * When possible, GemFire respects the <code>DataSerializable</code>
 * contract to provide optimal object serialization.  For instance, if
 * a <code>DataSerializable</code> object is 
 * {@linkplain org.apache.geode.cache.Region#put(Object, Object) placed} into a distributed
 * cache region, its <code>toData</code> method will be used to
 * serialize it when it is sent to another member of the distributed
 * system.
 *
 * <P>
 *
 * To avoid the overhead of Java reflection,
 * <code>DataSerializable</code> classes may register an {@link
 * Instantiator} to be used during deserialization.  Alternatively,
 * classes that implement <code>DataSerializable</code> can provide a
 * zero-argument constructor that will be invoked when they are read
 * with {@link DataSerializer#readObject}.
 *
 * <P>
 *
 * Some classes (especially third-party classes that you may not have
 * the source code to) cannot be modified to implement
 * <code>DataSerializable</code>.  These classes can be data
 * serialized by an instance of {@link DataSerializer}.
 *
 * <P>
 *
 * <code>DataSerializable</code> offers improved performance over
 * standard Java serialization, but does not offer all of the features
 * of standard Java serialization.  In particular, data serialization
 * does not attempt to maintain referential integrity among the
 * objects it is writing or reading.  As a result, data serialization
 * should not be used with complex object graphs.  Attempting to data
 * serialize graphs that contain object cycles will result in infinite
 * recursion and a {@link StackOverflowError}.  Attempting to
 * deserialize an object graph that contains multiple reference
 * paths to the same object will result in multiple copies of the
 * objects that are referred to through multiple paths.
 *
 * <P>
 *
 * <CENTER>
 * <IMG src="{@docRoot}/javadoc-images/data-serialization-exceptions.gif"
 *      HEIGHT="219" WIDTH="698">
 * </CENTER>
 *
 * @see java.io.Serializable
 * @see DataSerializer
 * @see Instantiator
 *
 * @since GemFire 3.5 */
public interface DataSerializable extends Serializable {

  /**
   * Writes the state of this object as primitive data to the given
   * <code>DataOutput</code>.
   * <p>
   * Since 5.7 it is possible for any method call to the specified
   * <code>DataOutput</code> to throw {@link GemFireRethrowable}.
   * It should <em>not</em> be caught by user code.
   * If it is it <em>must</em> be rethrown.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   */
  public void toData(DataOutput out) throws IOException;

  /**
   * Reads the state of this object as primitive data from the given
   * <code>DataInput</code>. 
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         A class could not be loaded while reading from
   *         <code>in</code> 
   */
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException;

  ////////////////////////  Inner Classes  ////////////////////////

  /**
   * <code>Replaceable</code> allows an object to write an alternative
   * version of itself to a <code>DataOutput</code>.  It is similar to
   * the <code>writeReplace</code> method of standard Java
   * {@linkplain java.io.Serializable serialization}.  
   *
   * <P>
   *
   * Note that if a <code>Replaceable</code> is also
   * <code>DataSerializable</code>, its <code>toData</code> method
   * will <B>not</B> be invoked.  Instead, its replacement object will
   * be written to the stream using {@link DataSerializer#writeObject(Object, DataOutput)}. 
   *
   * @see DataSerializer#writeObject(Object, DataOutput)
   */
  public interface Replaceable {

    /**
     * Replaces this object with another in the "output stream"
     * written by {@link DataSerializer#writeObject(Object, DataOutput)}.
     */
    public Object replace() throws IOException;
  }

}
