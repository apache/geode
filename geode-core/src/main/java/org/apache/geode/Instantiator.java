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
package org.apache.geode;

import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * {@code Instantiator} allows classes that implement {@link DataSerializable} to be registered with
 * the data serialization framework. Knowledge of {@code DataSerializable} classes allows the
 * framework to optimize how instances of those classes are data serialized.
 *
 * <P>
 *
 * Ordinarily, when a {@code DataSerializable} object is written using
 * {@link DataSerializer#writeObject(Object, java.io.DataOutput)}, a special marker class id is
 * written to the stream followed by the class name of the {@code DataSerializable} object. After
 * the marker class id is read by {@link DataSerializer#readObject} it performs the following
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
 * <LI>{@link DataSerializable#fromData} is invoked on the newly-created object</LI>
 *
 * </OL>
 *
 * However, if a {@code DataSerializable} class is {@linkplain #register(Instantiator) registered}
 * with the data serialization framework and assigned a unique class id, an important optimization
 * can be performed that avoid the expense of using reflection to instantiate the
 * {@code DataSerializable} class. When the object is written using
 * {@link DataSerializer#writeObject(Object, java.io.DataOutput)}, the object's registered class id
 * is written to the stream. Consequently, when the data is read from the stream, the
 * {@link #newInstance} method of the appropriate {@code Instantiator} instance is invoked to create
 * an "empty" instance of the {@code DataSerializable} instead of using reflection to create the new
 * instance.
 *
 * <P>
 *
 * Commonly, a {@code DataSerializable} class will register itself with the {@code Instantiator} in
 * a static initializer as shown in the below example code.
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
 * {@code Instantiator}s may be distributed to other members of the distributed system when they are
 * registered. Consider the following scenario in which VM1 and VM2 are members of the same
 * distributed system. Both VMs define the sameRegion and VM2's region replicates the contents of
 * VM1's using replication. VM1 puts an instance of the above {@code User} class into the region.
 * The act of instantiating {@code User} will load the {@code User} class and invoke its static
 * initializer, thus registering the {@code Instantiator} with the data serialization framework.
 * Because the region is a replicate, the {@code User} will be data serialized and sent to VM2.
 * However, when VM2 attempts to data deserialize the {@code User}, its {@code Instantiator} will
 * not necessarily be registered because {@code User}'s static initializer may not have been invoked
 * yet. As a result, an exception would be logged while deserializing the {@code User} and the
 * replicate would not appear to have the new value. So, in order to ensure that the
 * {@code Instantiator} is registered in VM2, the data serialization framework distributes a message
 * to each member when an {@code Instantiator} is {@linkplain #register(Instantiator) registered}.
 * <p>
 * Note that the framework does not require that an {@code Instantiator} be
 * {@link java.io.Serializable}, but it does require that it provide a
 * {@linkplain #Instantiator(Class, int) two-argument constructor}.
 *
 * @see #register(Instantiator)
 * @see #newInstance
 *
 * @since GemFire 3.5
 */
public abstract class Instantiator {

  /**
   * The class associated with this instantiator. Used mainly for debugging purposes and error
   * messages.
   */
  private Class<? extends DataSerializable> clazz;

  /** The id of this {@code Instantiator} */
  private int id;

  /** The eventId of this {@code Instantiator} */
  private EventID eventId;

  /** The originator of this {@code Instantiator} */
  private ClientProxyMembershipID context;

  /**
   * Registers a {@code DataSerializable} class with the data serialization framework. This method
   * is usually invoked from the static initializer of a class that implements
   * {@code DataSerializable}.
   *
   * @param instantiator An {@code Instantiator} whose {@link #newInstance} method is invoked when
   *        an object is data deserialized.
   *
   * @throws IllegalStateException If class {@code c} is already registered with a different class
   *         id, or another class has already been registered with id {@code classId}
   * @throws NullPointerException If {@code instantiator} is {@code null}.
   */
  public static synchronized void register(Instantiator instantiator) {
    InternalInstantiator.register(instantiator, true);
  }

  /**
   * Registers a {@code DataSerializable} class with the data serialization framework. This method
   * is usually invoked from the static initializer of a class that implements
   * {@code DataSerializable}.
   *
   * @param instantiator An {@code Instantiator} whose {@link #newInstance} method is invoked when
   *        an object is data deserialized.
   *
   * @param distribute True if the registered {@code Instantiator} has to be distributed to other
   *        members of the distributed system. Note that if distribute is set to false it may still
   *        be distributed in some cases.
   *
   * @throws IllegalArgumentException If class {@code c} is already registered with a different
   *         class id, or another class has already been registered with id {@code classId}
   * @throws NullPointerException If {@code instantiator} is {@code null}.
   * @deprecated as of 9.0 use {@link Instantiator#register(Instantiator)} instead
   */
  @Deprecated
  public static synchronized void register(Instantiator instantiator, boolean distribute) {
    InternalInstantiator.register(instantiator, distribute);
  }

  /**
   * Creates a new {@code Instantiator} that instantiates a given class.
   *
   * @param c The {@code DataSerializable} class to register. This class must have a static
   *        initializer that registers this {@code Instantiator}.
   * @param classId A unique id for class {@code c}. The {@code classId} must not be zero. This has
   *        been an {@code int} since dsPhase1.
   *
   * @throws IllegalArgumentException If {@code c} does not implement {@code DataSerializable},
   *         {@code classId} is less than or equal to zero.
   * @throws NullPointerException If {@code c} is {@code null}
   */
  public Instantiator(Class<? extends DataSerializable> c, int classId) {
    if (c == null) {
      throw new NullPointerException(
          "Cannot register a null class.");
    }

    if (!DataSerializable.class.isAssignableFrom(c)) {
      throw new IllegalArgumentException(
          String.format("Class %s does not implement DataSerializable",
              c.getName()));
    }

    if (classId == 0) {
      throw new IllegalArgumentException(
          String.format("Class id %s must not be 0.", classId));
    }

    this.clazz = c;
    this.id = classId;
  }

  /**
   * Creates a new "empty" instance of a {@code DataSerializable} class whose state will be filled
   * in by invoking its {@link DataSerializable#fromData fromData} method.
   *
   * @see DataSerializer#readObject
   */
  public abstract DataSerializable newInstance();

  /**
   * Returns the {@code DataSerializable} class that is instantiated by this {@code Instantiator}.
   */
  public Class<? extends DataSerializable> getInstantiatedClass() {
    return this.clazz;
  }

  /**
   * Returns the unique {@code id} of this {@code Instantiator}.
   */
  public int getId() {
    return this.id;
  }

  /**
   * sets the unique {@code eventId} of this {@code Instantiator}. For internal use only.
   */
  public void setEventId(Object/* EventID */ eventId) {
    this.eventId = (EventID) eventId;
  }

  /**
   * Returns the unique {@code eventId} of this {@code Instantiator}. For internal use only.
   */
  public Object/* EventID */ getEventId() {
    return this.eventId;
  }

  /**
   * sets the context of this {@code Instantiator}. For internal use only.
   */
  public void setContext(Object/* ClientProxyMembershipID */ context) {
    this.context = (ClientProxyMembershipID) context;
  }

  /**
   * Returns the context of this {@code Instantiator}. For internal use only.
   */
  public Object/* ClientProxyMembershipID */ getContext() {
    return this.context;
  }

}
