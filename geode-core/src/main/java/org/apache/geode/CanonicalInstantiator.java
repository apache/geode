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
 * <code>CanonicalInstantiator</code> is much like its parent
 * <code>Instantiator</code> except that instead of
 * needing to implement <code>newInstance()</code>
 * you now must implement <code>newInstance(DataInput)</code>.
 * The addition of the <code>DataInput</code> parameter allows the instance
 * creator to optionally read data from the data input stream and use it to
 * decide the instance to create. This allows a value that represents a
 * canonical instance to be written by a class's {@link DataSerializer#toData}
 * and then be read by <code>newInstance(DataInput)</code>
 * and map it back to a canonical instance.
 * <p>
 * Note that {@link DataSerializer#fromData} is always called on the instance
 * returned from <code>newInstance(DataInput)</code>.
 *
 * @since GemFire 5.1
 */
public abstract class CanonicalInstantiator extends Instantiator {
  /**
   * Creates a new <code>CanonicalInstantiator</code> that instantiates a given
   * class.
   *
   * @param c
   *        The <code>DataSerializable</code> class to register.  This
   *        class must have a static initializer that registers this
   *        <code>Instantiator</code>. 
   * @param classId
   *        A unique id for class <code>c</code>.  The
   *        <code>classId</code> must not be zero.
   *        This has been an <code>int</code> since dsPhase1.
   *
   * @throws IllegalArgumentException
   *         If <code>c</code> does not implement
   *         <code>DataSerializable</code>, <code>classId</code> is
   *         less than or equal to zero.
   * @throws NullPointerException
   *         If <code>c</code> is <code>null</code>
   */
  public CanonicalInstantiator(Class<? extends DataSerializable> c, int classId) {
    super(c, classId);
  }
  
  /**
   * This method is not supported and if called will
   * throw UnsupportedOperationException.
   * Use {@link #newInstance(DataInput)} instead.
   * 
   * @throws UnsupportedOperationException in all cases
   */
  @Override
  public final DataSerializable newInstance() {
    throw new UnsupportedOperationException();
  }
  /**
   * Creates a new "empty" instance of a <Code>DataSerializable</code>
   * class whose state will be filled in by invoking its 
   * {@link DataSerializable#fromData fromData} method.
   * @param in the data input that can be read to decide what instance to create.
   * @return the new "empty" instance.
   * @throws IOException if a read from <code>in</code> fails.
   * @since GemFire 5.1
   */
  public abstract DataSerializable newInstance(DataInput in)
    throws IOException;
}
