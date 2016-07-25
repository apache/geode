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
package com.gemstone.gemfire;

import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * <code>Instantiator</code> allows classes that implement {@link
 * DataSerializable} to be registered with the data serialization
 * framework.  Knowledge of <code>DataSerializable</code> classes
 * allows the framework to optimize how instances of those classes are
 * data serialized.
 *
 * <P>
 *
 * Ordinarily, when a <code>DataSerializable</code> object is written
 * using {@link DataSerializer#writeObject(Object, java.io.DataOutput)}, a special marker class id
 * is written to the stream followed by the class name of the
 * <code>DataSerializable</code> object.  After the marker class id is
 * read by {@link DataSerializer#readObject} it performs the following
 * operations,
 *
 * <OL>
 *
 * <LI>The class name is read</LI>
 *
 * <LI>The class is loaded using {@link Class#forName(java.lang.String)}</LI>
 *
 * <LI>An instance of the class is created using reflection</LI>
 *
 * <LI>{@link DataSerializable#fromData} is invoked on the
 * newly-created object</LI>
 *
 * </OL>
 *
 * However, if a <code>DataSerializable</code> class is {@linkplain
 * #register(Instantiator) registered} with the data serialization framework and
 * assigned a unique class id, an important optimization can be
 * performed that avoid the expense of using reflection to instantiate
 * the <code>DataSerializable</code> class.  When the object is
 * written using {@link DataSerializer#writeObject(Object, java.io.DataOutput)}, the object's
 * registered class id is written to the stream.  Consequently, when
 * the data is read from the stream, the {@link #newInstance} method
 * of the appropriate <code>Instantiator</code> instance is invoked to
 * create an "empty" instance of the <code>DataSerializable</code>
 * instead of using reflection to create the new instance.
 *
 * <P>
 *
 * Commonly, a <code>DataSerializable</code> class will register
 * itself with the <code>Instantiator</code> in a static initializer
 * as shown in the below example code.
 *
 * <!-- 
 * The source code for the CompanySerializer class resides in 
 *         tests/com/examples/ds/User.java
 * Please keep the below code snippet in sync with that file.
 * -->
 *
 * <PRE>
public class User implements DataSerializable {
  private String name;
  private int userId;

  static {
    Instantiator.register(new Instantiator(User.class, 45) {
        public DataSerializable newInstance() {
          return new User();
        }
      });
  }

  public User(String name, int userId) {
    this.name = name;
    this.userId = userId;
  }

  &#47;**
   * Creates an "empty" User whose contents are filled in by
   * invoking its toData() method
   *&#47;
  private User() {

  }

  public void toData(DataOutput out) throws IOException {
    out.writeUTF(this.name);
    out.writeInt(this.userId);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this.name = in.readUTF();
    this.userId = in.readInt();
  }
}
 * </PRE>
 *
 * <code>Instantiator</code>s may be distributed to other members of
 * the distributed system when they are registered.  Consider the
 * following scenario in which VM1 and VM2 are members of the same
 * distributed system.  Both VMs define the sameRegion and VM2's
 * region replicates the contents of VM1's using replication.
 * VM1 puts an instance of the above <code>User</code> class into the
 * region.  The act of instantiating <code>User</code> will load the
 * <code>User</code> class and invoke its static initializer, thus
 * registering the <code>Instantiator</code> with the data
 * serialization framework.  Because the region is a replicate, the
 * <code>User</code> will be data serialized and sent to VM2.
 * However, when VM2 attempts to data deserialize the
 * <code>User</code>, its <code>Instantiator</code> will not
 * necessarily be registered because <code>User</code>'s static
 * initializer may not have been invoked yet.  As a result, an
 * exception would be logged while deserializing the <code>User</code>
 * and the replicate would not appear to have the new value.  So, in
 * order to ensure that the <code>Instantiator</code> is registered in
 * VM2, the data serialization framework distributes a message to each
 * member when an <code>Instantiator</code> is {@linkplain #register(Instantiator)
 * registered}.  <p>Note that the framework does not require that an
 * <code>Instantiator</code> be {@link java.io.Serializable}, but it
 * does require that it provide
 * a {@linkplain #Instantiator(Class, int)
 * two-argument constructor}.
 *
 * @see #register(Instantiator)
 * @see #newInstance
 *
 * @since GemFire 3.5 */
public abstract class Instantiator {

  /** The class associated with this instantiator.  Used mainly for
   * debugging purposes and error messages. */
  private Class<? extends DataSerializable> clazz;

  /** The id of this <code>Instantiator</code> */
  private int id;
  
  /** The eventId of this <code>Instantiator</code> */
  private EventID eventId;
  
  /** The originator of this <code>Instantiator</code> */
  private ClientProxyMembershipID context;


  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Registers a <code>DataSerializable</code> class with the data
   * serialization framework.  This method is usually invoked from the
   * static initializer of a class that implements
   * <code>DataSerializable</code>. 
   *
   * @param instantiator
   *        An <code>Instantiator</code> whose {@link #newInstance}
   *        method is invoked when an object is data deserialized.
   *
   * @throws IllegalStateException
   *         If class <code>c</code> is
   *         already registered with a different class id, or another
   *         class has already been registered with id
   *         <code>classId</code>
   * @throws NullPointerException
   *         If <code>instantiator</code> is <code>null</code>.
   */
  public static synchronized void register(Instantiator instantiator) {
    InternalInstantiator.register(instantiator, true);
  }

  /**
   * Registers a <code>DataSerializable</code> class with the data
   * serialization framework.  This method is usually invoked from the
   * static initializer of a class that implements
   * <code>DataSerializable</code>. 
   *
   * @param instantiator
   *        An <code>Instantiator</code> whose {@link #newInstance}
   *        method is invoked when an object is data deserialized.
   *
   * @param distribute
   *        True if the registered <code>Instantiator</code> has to be
   *        distributed to other members of the distributed system.
   *        Note that if distribute is set to false it may still be distributed
   *        in some cases.
   *
   * @throws IllegalArgumentException
   *         If class <code>c</code> is
   *         already registered with a different class id, or another
   *         class has already been registered with id
   *         <code>classId</code>
   * @throws NullPointerException
   *         If <code>instantiator</code> is <code>null</code>.
   * @deprecated as of 9.0 use {@link Instantiator#register(Instantiator)} instead
   */
  public static synchronized void register(Instantiator instantiator,
      boolean distribute) {
    InternalInstantiator.register(instantiator, distribute);
  }

  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>Instantiator</code> that instantiates a given
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
  public Instantiator(Class<? extends DataSerializable> c, int classId) {
    if (c == null) {
      throw new NullPointerException(LocalizedStrings.Instantiator_CANNOT_REGISTER_A_NULL_CLASS.toLocalizedString());
    }

    if (!DataSerializable.class.isAssignableFrom(c)) {
      throw new IllegalArgumentException(LocalizedStrings.Instantiator_CLASS_0_DOES_NOT_IMPLEMENT_DATASERIALIZABLE.toLocalizedString(c.getName()));
    }

    if (classId == 0) {
      throw new IllegalArgumentException(LocalizedStrings.Instantiator_CLASS_ID_0_MUST_NOT_BE_0.toLocalizedString(Integer.valueOf(classId)));
    }

    this.clazz = c;
    this.id = classId;
  }
  
  //////////////////////  Instance Methods  //////////////////////

  /**
   * Creates a new "empty" instance of a <Code>DataSerializable</code>
   * class whose state will be filled in by invoking its {@link
   * DataSerializable#fromData fromData} method.
   *
   * @see DataSerializer#readObject
   */
  public abstract DataSerializable newInstance();

  /**
   * Returns the <code>DataSerializable</code> class that is
   * instantiated by this <code>Instantiator</code>.
   */
  public final Class<? extends DataSerializable> getInstantiatedClass() {
    return this.clazz;
  }

  /**
   * Returns the unique <code>id</code> of this
   * <code>Instantiator</code>.
   */
  public final int getId() {
    return this.id;
  }
  /**
   * sets the unique <code>eventId</code> of this
   * <code>Instantiator</code>. For internal use only.
   */
  public final void setEventId(Object/*EventID*/ eventId) {
    this.eventId = (EventID)eventId;
  }
  
  /**
   * Returns the unique <code>eventId</code> of this
   * <code>Instantiator</code>. For internal use only.
   */
  public final Object/*EventID*/ getEventId() {
    return this.eventId;
  }
  
  /**
   * sets the context of this
   * <code>Instantiator</code>. For internal use only.
   */
  public final void setContext(Object/*ClientProxyMembershipID*/ context) {
    this.context = (ClientProxyMembershipID)context;
  }
  
  /**
   * Returns the context of this
   * <code>Instantiator</code>. For internal use only.
   */
  public final Object/*ClientProxyMembershipID*/ getContext() {
    return this.context;
  }
  
  
}
