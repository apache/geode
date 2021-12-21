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

package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.tier.sockets.ClientInstantiatorMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Contains the implementation of {@link org.apache.geode.Instantiator} registration and
 * distribution messaging (and shared memory management).
 *
 * @since GemFire 3.5
 */
public class InternalInstantiator {

  private static final Logger logger = LogService.getLogger();

  /**
   * Maps Class names to their Instantiator instance.
   */
  @MakeNotStatic
  private static final ConcurrentMap<String, Instantiator> dsMap = new ConcurrentHashMap<>();

  /** Maps the id of an instantiator to its Instantiator instance */
  @MakeNotStatic
  private static final ConcurrentMap<Integer, Object> idsToInstantiators =
      new ConcurrentHashMap<>();

  /**
   * Maps the name of the instantiated-class to an instance of InstantiatorAttributesHolder.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<String, InstantiatorAttributesHolder> classNamesToHolders =
      new ConcurrentHashMap<>();

  /**
   * Maps the id of an instantiator to an instance of InstantiatorAttributesHolder.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<Integer, InstantiatorAttributesHolder> idsToHolders;

  static {
    idsToHolders = new ConcurrentHashMap<>();
  }

  private static final String SERVER_CONNECTION_THREAD = "ServerConnection";
  /////////////////////// Static Methods ///////////////////////

  /**
   * Registers an {@code Instantiator} with the data serialization framework.
   */
  public static void register(Instantiator instantiator, boolean distribute) {
    // Skip the checkForThread() check if the instantiation is not
    // to be distributed. This allows instantiations from ServerConnection
    // thread in client security plugins, for example. This is particularly
    // useful for native clients that do not send a REGISTER_INSTANTIATORS
    // message rather rely on server side registration, so each server can
    // do the registration with distribute set to false.
    if (!distribute || checkForThread()) {
      _register(instantiator, distribute);
    }
  }

  /**
   * Actually registers an {@code Instantiator} with the data serialization framework.
   *
   * @throws IllegalArgumentException If the instantiator has an id of zero
   * @throws IllegalStateException The instantiator cannot be registered
   */
  private static void _register(Instantiator instantiator, boolean distribute) {
    if (instantiator == null) {
      throw new NullPointerException("Cannot register a null Instantiator.");
    }
    final int classId = instantiator.getId();
    if (classId == 0) {
      throw new IllegalArgumentException("Instantiator id cannot be zero");
    }
    Class c = instantiator.getInstantiatedClass();
    final String cName = c.getName();
    {
      int oldId = getClassId(c);
      if (oldId != 0 && oldId != classId) {
        throw new IllegalStateException(String.format(
            "Class %s is already registered with id %s so it can not be registered with id %s",
            c.getName(), oldId, classId));
      }
    }
    final Integer idx = classId;

    synchronized (InternalInstantiator.class) {
      boolean retry;
      do {
        retry = false;
        Object oldInst = idsToInstantiators.putIfAbsent(idx, instantiator);
        if (oldInst != null) {
          if (oldInst instanceof Marker) {
            retry = !idsToInstantiators.replace(idx, oldInst, instantiator);
            if (!retry) {
              dsMap.put(cName, instantiator);
              ((Marker) oldInst).setInstantiator(instantiator);
            }
          } else {
            Class oldClass = ((Instantiator) oldInst).getInstantiatedClass();
            if (!oldClass.getName().equals(cName)) {
              throw new IllegalStateException(String.format(
                  "Class id %s is already registered for class %s so it could not be registered for class %s",
                  classId, oldClass.getName(), cName));
            } else {
              // it was already registered
              return;
            }
          }
        } else {
          dsMap.put(cName, instantiator);
        }
      } while (retry);

      setEventIdIfNew(instantiator);

      logger.info("Instantiator registered with id {} class {}", classId, c.getName());
    }

    if (distribute) {
      // originated in this VM
      sendRegistrationMessage(instantiator);
      sendRegistrationMessageToServers(instantiator);
    }
    sendRegistrationMessageToClients(instantiator);

    InternalDataSerializer.fireNewInstantiator(instantiator);
  }

  /**
   * if instantiator is getting registered for first time its EventID will be null, so generate a
   * new event id the the distributed system is connected
   */
  private static void setEventIdIfNew(final Instantiator instantiator) {
    final InternalCache cache = getInternalCache();
    if (cache != null && instantiator.getEventId() == null) {
      instantiator.setEventId(new EventID(cache.getDistributedSystem()));
    }
  }

  @SuppressWarnings("deprecation")
  private static InternalCache getInternalCache() {
    return GemFireCacheImpl.getInstance();
  }

  /**
   * On receving on client, when the instantiator gets loaded, and if it has static initializer
   * which registers instantiators having distribute flag as true. And as distribute flag is true,
   * client sends instantiator registartion recursively to server. Similarly, on server side, when
   * the instantiator gets loaded, and if it has static initializer which registers instantiators
   * having distribute flag as true. But eventId is not set on this case so it generates itw own
   * event ID , to avoid this just return
   */
  private static boolean checkForThread() {
    String name = Thread.currentThread().getName();
    return !(name.startsWith(CacheClientUpdater.CLIENT_UPDATER_THREAD_NAME)
        || name.startsWith(SERVER_CONNECTION_THREAD));
  }

  /**
   * Sets the EventID to the instantiator if distributed system is created
   */
  public static EventID generateEventId() {
    if (isCacheCreated()) {
      return new EventID(InternalDistributedSystem.getAnyInstance());
    }
    return null;
  }

  /**
   * Sends Instantiator registration message to one of the servers
   *
   */
  private static void sendRegistrationMessageToServers(Instantiator instantiator) {
    PoolManagerImpl.allPoolsRegisterInstantiator(instantiator);
  }

  /**
   * Sends Instantiator registration message to one of the servers
   *
   */
  private static void sendRegistrationMessageToServers(InstantiatorAttributesHolder holder) {
    PoolManagerImpl.allPoolsRegisterInstantiator(holder);
  }

  /**
   * Sends Instantiator registration message to all cache clients
   */
  private static void sendRegistrationMessageToClients(Instantiator instantiator) {
    if (!isCacheCreated()) {
      return;
    }
    byte[][] serializedInstantiators = new byte[3][];
    try {
      serializedInstantiators[0] =
          CacheServerHelper.serialize(instantiator.getClass().toString().substring(6));
      serializedInstantiators[1] =
          CacheServerHelper.serialize(instantiator.getInstantiatedClass().toString().substring(6));
      {
        byte[] idBytes = new byte[4];
        Part.encodeInt(instantiator.getId(), idBytes);
        serializedInstantiators[2] = idBytes;
      }
    } catch (IOException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "IOException encountered while serializing instantiators using CacheServerHelper.serialize() method");
      }
    }
    ClientInstantiatorMessage clientInstantiatorMessage = new ClientInstantiatorMessage(
        EnumListenerEvent.AFTER_REGISTER_INSTANTIATOR, serializedInstantiators,
        (ClientProxyMembershipID) instantiator.getContext(), (EventID) instantiator.getEventId());
    // Deliver it to all the clients
    CacheClientNotifier.routeClientMessage(clientInstantiatorMessage);
  }

  private static boolean isCacheCreated() {
    return getInternalCache() != null;
  }

  /**
   * Creates a new {@code Instantiator} with the given class and id and
   * {@linkplain #register(Instantiator, boolean) registers} it with the data serialization
   * framework.
   *
   * @throws IllegalArgumentException The instantiator cannot be created
   * @throws IllegalStateException The instantiator cannot be registered
   */
  public static void register(Class instantiatorClass, Class instantiatedClass, int id,
      boolean distribute) {

    if (checkForThread()) {
      Instantiator inst = newInstance(instantiatorClass, instantiatedClass, id);
      _register(inst, distribute);
    }
  }

  /**
   * Creates a new {@code Instantiator} with the given class and id and
   * {@linkplain #register(Instantiator, boolean) registers} it with the data serialization
   * framework.
   *
   * This method is only called when server connection and CacheClientUpdaterThread
   *
   * @throws IllegalArgumentException The instantiator cannot be created
   * @throws IllegalStateException The instantiator cannot be registered
   */
  public static void register(Class instantiatorClass, Class instantiatedClass, int id,
      boolean distribute, EventID eventId, ClientProxyMembershipID context) {
    Instantiator inst = newInstance(instantiatorClass, instantiatedClass, id);
    // This method is only called when server connection and CacheClientUpdaterThread
    inst.setEventId(eventId);
    inst.setContext(context);
    _register(inst, distribute);
  }

  /**
   * Lazily creates a new {@code Instantiator} with the given class and id.
   *
   * @throws IllegalArgumentException The instantiator cannot be created
   * @throws IllegalStateException The instantiator cannot be registered
   */
  public static void register(String instantiatorClass, String instantiatedClass, int id,
      boolean distribute) {

    if (checkForThread()) {
      register(instantiatorClass,
          new InstantiatorAttributesHolder(instantiatorClass, instantiatedClass, id), distribute);
    }
  }

  /**
   * Lazily creates a new {@code Instantiator} with the given class and id.
   *
   * This method is only called when server connection and CacheClientUpdaterThread
   *
   * @throws IllegalArgumentException The instantiator cannot be created
   * @throws IllegalStateException The instantiator cannot be registered
   */
  public static void register(String instantiatorClass, String instantiatedClass, int id,
      boolean distribute, EventID eventId, ClientProxyMembershipID context) {
    register(instantiatorClass, new InstantiatorAttributesHolder(instantiatorClass,
        instantiatedClass, id, eventId, context), distribute);
  }

  private static void register(String instantiatorClassName, InstantiatorAttributesHolder holder,
      boolean distribute) {

    Object inst;
    synchronized (InternalInstantiator.class) {
      inst = idsToInstantiators.get(holder.getId());
      if (inst == null) {
        if (instantiatorClassName == null || instantiatorClassName.trim().equals("")) {
          throw new IllegalArgumentException("Instantiator class name cannot be null or empty.");
        }
        if (holder.getId() == 0) {
          throw new IllegalArgumentException("Instantiator id cannot be zero");
        }

        InstantiatorAttributesHolder iah =
            classNamesToHolders.putIfAbsent(holder.getInstantiatedClassName(), holder);

        if (iah != null && iah.getId() != holder.getId()) {
          throw new IllegalStateException(String.format(
              "Class %s is already registered with id %s so it can not be registered with id %s",
              instantiatorClassName, iah.getId(), holder.getId()));
        }

        idsToHolders.putIfAbsent(holder.getId(), holder);

        logger.info("Instantiator registered with holder id {} class {}", holder.getId(),
            holder.getInstantiatedClassName());

        if (distribute) {
          sendRegistrationMessageToServers(holder);
        }
        return;
      }
    }

    if (inst instanceof Marker) {
      Class instantiatorClass = null, instantiatedClass = null;
      try {
        instantiatorClass =
            InternalDataSerializer.getCachedClass(holder.getInstantiatorClassName());
        instantiatedClass =
            InternalDataSerializer.getCachedClass(holder.getInstantiatedClassName());
      } catch (ClassNotFoundException e) {
        logClassNotFoundException(e);
      }
      synchronized (InternalInstantiator.class) {
        Object inst2 = idsToInstantiators.get(holder.getId());
        if (inst2 == inst) {
          register(instantiatorClass, instantiatedClass, holder.getId(), distribute,
              holder.getEventId(), holder.getContext());
        } else {
          if (inst2 == null || inst2 instanceof Marker) {
            register(instantiatorClassName, holder, distribute);
          }
        }
      }
    }
  }

  private static void logClassNotFoundException(final ClassNotFoundException e) {
    final InternalCache cache = getInternalCache();
    if (cache != null && cache.getLogger() != null && cache.getLogger().infoEnabled()) {
      cache.getLogger()
          .info(String.format("Could not load instantiator class: %s", e.getMessage()));
    }
  }

  public static class InstantiatorAttributesHolder {
    private final String instantiatorName;
    private final String instantiatedName;
    private final int id;
    private EventID eventId;
    private ClientProxyMembershipID context;

    InstantiatorAttributesHolder(String instantiatorClass, String instantiatedClass,
        int id) {
      instantiatorName = instantiatorClass;
      instantiatedName = instantiatedClass;
      this.id = id;
    }

    InstantiatorAttributesHolder(String instantiatorClass, String instantiatedClass, int id,
        EventID eventId, ClientProxyMembershipID context) {
      instantiatorName = instantiatorClass;
      instantiatedName = instantiatedClass;
      this.id = id;
      this.eventId = eventId;
      this.context = context;
    }

    public String getInstantiatorClassName() {
      return instantiatorName;
    }

    public String getInstantiatedClassName() {
      return instantiatedName;
    }

    public int getId() {
      return id;
    }

    public EventID getEventId() {
      return eventId;
    }

    public ClientProxyMembershipID getContext() {
      return context;
    }

    public String toString() {
      return "InstantiatorAttributesHolder[irName=" + instantiatorName + ",idName="
          + instantiatedName + ",id=" + id
          + (eventId != null ? ",this.eventId=" + eventId : "")
          + (context != null ? ",this.context=" + context : "") + "]";
    }
  }

  /**
   * Unregisters the given class with the given class id with the {@code Instantiator}.
   *
   * @throws IllegalArgumentException If {@code c} was not previously registered with id
   *         {@code classId}.
   * @throws NullPointerException If {@code c} is {@code null}
   */
  public static synchronized void unregister(Class c, int classId) {
    if (c == null) {
      throw new NullPointerException(
          "Cannot unregister a null class");
    }
    final Integer idx = classId;
    final Instantiator i = (Instantiator) idsToInstantiators.remove(idx);
    if (i == null) {
      throw new IllegalArgumentException(
          String.format("Class %s was not registered with id %s", c.getName(), classId));
    } else {
      dsMap.remove(c.getName(), i);
    }
    idsToHolders.remove(idx);
    classNamesToHolders.remove(i.getInstantiatedClass().getName());
  }

  /**
   * testhook that removes all registed instantiators
   */
  @VisibleForTesting
  public static void reinitialize() {
    idsToInstantiators.clear();
    dsMap.clear();
    idsToHolders.clear();
    classNamesToHolders.clear();
  }

  /**
   * Returns the class id for the given class.
   *
   * @return {@code 0} if the class has not be registered
   *
   * @see DataSerializer#writeObject(Object, DataOutput)
   */
  public static int getClassId(Class c) {
    int result = 0;
    final Instantiator i = dsMap.get(c.getName());
    if (i != null) {
      result = i.getId();
    } else {
      InstantiatorAttributesHolder iah = classNamesToHolders.get(c.getName());
      if (iah != null) {
        result = iah.getId();
      }
    }
    return result;
  }

  /**
   * Returns the class with the given id
   *
   * @see DataSerializer#readObject
   */
  public static Instantiator getInstantiator(int classId) {
    final Integer idx = classId;
    Marker marker;
    boolean retry;
    Object o = idsToInstantiators.get(idx);
    do {
      retry = false;
      if (o == null) {
        marker = new Marker();
        o = idsToInstantiators.putIfAbsent(idx, marker);
        retry = o != null;
      } else if (o instanceof Marker) {
        marker = (Marker) o;
      } else {
        return (Instantiator) o;
      }
    } while (retry);

    Instantiator instantiator = null;
    if (idsToHolders.get(classId) == null) {
      instantiator = marker.getInstantiator();
    }

    if (instantiator != null) {
      return instantiator;
    } else {
      InstantiatorAttributesHolder holder = idsToHolders.get(classId);
      if (holder != null) {
        try {
          Class instantiatorClass =
              InternalDataSerializer.getCachedClass(holder.getInstantiatorClassName());
          Class instantiatedClass =
              InternalDataSerializer.getCachedClass(holder.getInstantiatedClassName());
          // 46355: move getCachedClass out of sync
          synchronized (InternalInstantiator.class) {
            register(instantiatorClass, instantiatedClass, holder.getId(), false,
                holder.getEventId(), holder.getContext());
            classNamesToHolders.remove(holder.getInstantiatedClassName());
            idsToHolders.remove(classId);
            instantiator = (Instantiator) idsToInstantiators.get(classId);
          }
        } catch (ClassNotFoundException e) {
          logClassNotFoundException(e);
        }
      }
      return instantiator;
    }
  }

  /**
   * If we are connected to a distributed system, send a message to other members telling them about
   * a newly-registered instantiator.
   */
  private static void sendRegistrationMessage(Instantiator s) {
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if (system != null) {
      RegistrationMessage m;
      if (s.getContext() == null) {
        m = new RegistrationMessage(s);
      } else {
        m = new RegistrationContextMessage(s);
      }
      system.getDistributionManager().putOutgoing(m);
    }
  }

  /**
   * Reflectively instantiates an instance of {@code Instantiator}.
   *
   * @param instantiatorClass The implementation of {@code Instantiator} to instantiate
   * @param instantiatedClass The implementation of {@code DataSerialization} that will be produced
   *        by the {@code Instantiator}
   *
   * @throws IllegalArgumentException If the class can't be instantiated
   */
  protected static Instantiator newInstance(Class<?> instantiatorClass, Class<?> instantiatedClass,
      int id) throws IllegalArgumentException {
    if (!Instantiator.class.isAssignableFrom(instantiatorClass)) {
      throw new IllegalArgumentException(
          String.format("%s does not extend Instantiator.",
              instantiatorClass.getName()));
    }

    Constructor init;
    boolean intConstructor = false;
    try {
      init = instantiatorClass.getDeclaredConstructor(Class.class, int.class);
      intConstructor = true;
    } catch (NoSuchMethodException ex) {
      // for backwards compat check for (Class, byte)
      try {
        init = instantiatorClass.getDeclaredConstructor(Class.class, byte.class);
      } catch (NoSuchMethodException ex2) {
        if (instantiatorClass.getDeclaringClass() != null) {
          throw new IllegalArgumentException(String.format(
              "Class %s does not have a two-argument (Class, int) constructor. It is an inner class of %s. Should it be a static inner class?",
              instantiatorClass.getName(), instantiatorClass.getDeclaringClass()));
        }
        throw new IllegalArgumentException(
            String.format("Class %s does not have a two-argument (Class, int) constructor.",
                instantiatorClass.getName()));
      }
    }

    Instantiator s;
    try {
      init.setAccessible(true);
      s = (Instantiator) init.newInstance(instantiatedClass, convertId(id, intConstructor));
    } catch (IllegalAccessException ex) {
      throw new IllegalArgumentException(String
          .format("Could not access zero-argument constructor of %s", instantiatorClass.getName()));

    } catch (InstantiationException ex) {
      throw new IllegalArgumentException(
          String.format("Could not instantiate an instance of %s", instantiatorClass.getName()),
          ex);

    } catch (InvocationTargetException ex) {
      throw new IllegalArgumentException(
          String.format("While instantiating an instance of %s", instantiatorClass.getName()), ex);
    }

    return s;
  }

  private static Object convertId(int id, boolean asInteger) {
    if (asInteger) {
      return id;
    }
    return (byte) id;
  }

  /**
   * Returns all of the currently registered instantiators
   */
  public static Instantiator[] getInstantiators() {
    if (!classNamesToHolders.isEmpty()) {
      for (InstantiatorAttributesHolder holder : classNamesToHolders.values()) {
        try {
          Class instantiatorClass =
              InternalDataSerializer.getCachedClass(holder.getInstantiatorClassName());
          Class instantiatedClass =
              InternalDataSerializer.getCachedClass(holder.getInstantiatedClassName());
          synchronized (InternalInstantiator.class) {
            if (!idsToInstantiators.containsKey(holder.getId())) {
              register(instantiatorClass, instantiatedClass, holder.getId(), false,
                  holder.getEventId(), holder.getContext());
            }
            classNamesToHolders.remove(holder.getInstantiatedClassName());
            idsToHolders.remove(holder.getId());
          }
        } catch (ClassNotFoundException e) {
          logClassNotFoundException(e);
        }
      }
    }
    return dsMap.values().toArray(new Instantiator[0]);
  }

  /**
   * Does not trigger loading of the instantiator/instantiated classes into the vm.
   *
   * @return array of InstantiatorAttributesArray instances.
   */
  public static Object[] getInstantiatorsForSerialization() {
    ArrayList<Object> instantiators = new ArrayList<>(dsMap.size() + idsToHolders.size());
    instantiators.addAll(dsMap.values());
    instantiators.addAll(classNamesToHolders.values());
    return instantiators.toArray();
  }

  /**
   * A marker object for {@code Instantiator}s that have not been registered. Using this marker
   * object allows us to asynchronously send {@code Instantiator} registration updates. If the
   * serialized bytes arrive at a VM before the registration message does, the deserializer will
   * wait an amount of time for the registration message to arrive.
   */
  static class Marker {

    /** The Instantiator that is filled in upon registration */
    private volatile Instantiator instantiator = null;

    /**
     * Creates a new {@code Marker} whose {@link #getInstantiator} method will wait for the
     * instantiator to be registered.
     */
    Marker() {

    }

    /**
     * Returns the instantiator associated with this marker. If the instantiator has not been
     * registered yet, then this method will wait until the instantiator is registered. If this
     * method has to wait for too long, then {@code null} is returned.
     */
    Instantiator getInstantiator() {
      synchronized (this) {
        if (instantiator == null) {
          try {
            wait(InternalDataSerializer.GetMarker.WAIT_MS);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // just return null, let it fail.
            return null;
          }
        }

        return instantiator;
      }
    }

    /**
     * Sets the instantiator associated with this marker. It will notify any threads that are
     * waiting for the instantiator to be registered.
     */
    void setInstantiator(Instantiator instantiator) {
      synchronized (this) {
        this.instantiator = instantiator;
        notifyAll();
      }
    }
  }

  /**
   * A distribution message that alerts other members of the distributed cache of a new
   * {@code Instantiator} being registered.
   */
  public static class RegistrationMessage extends SerialDistributionMessage {
    /**
     * The {@code Instantiator} class that was registered
     */
    Class instantiatorClass;

    /** The class that is instantiated by the instantiator */
    Class instantiatedClass;

    /**
     * The id of the {@code Instantiator} that was registered
     */
    protected int id;

    /**
     * The eventId of the {@code Instantiator} that was registered
     */
    protected EventID eventId;

    /**
     * Problems encountered while running fromData. See bug 31573.
     */
    transient StringBuffer fromDataProblems;

    /**
     * The name of the {@code Instantiator} class that was registered
     */
    String instantiatorClassName;

    /** Name of the class that is instantiated by the instantiator */
    String instantiatedClassName;

    /**
     * Constructor for {@code DataSerializable}
     */
    public RegistrationMessage() {

    }

    /**
     * Creates a new {@code RegistrationMessage} that broadcasts that the given {@code Instantiator}
     * was registered.
     */
    public RegistrationMessage(Instantiator s) {
      instantiatorClass = s.getClass();
      instantiatedClass = s.getInstantiatedClass();
      id = s.getId();
      eventId = (EventID) s.getEventId();
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      if (fromDataProblems != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(fromDataProblems);
        }
      }

      if (instantiatorClass != null && instantiatedClass != null) {
        Instantiator s = newInstance(instantiatorClass, instantiatedClass, id);
        s.setEventId(eventId);
        InternalInstantiator.register(s, false);
      } else if (instantiatorClassName != null && instantiatedClassName != null) {
        InternalInstantiator.register(instantiatorClassName, instantiatedClassName,
            id, false, eventId, null);
      }
    }

    @Override
    public int getDSFID() {
      return REGISTRATION_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeNonPrimitiveClassName(instantiatorClass.getName(), out);
      DataSerializer.writeNonPrimitiveClassName(instantiatedClass.getName(), out);
      out.writeInt(id);
      DataSerializer.writeObject(eventId, out);
    }

    private void recordFromDataProblem(String s) {
      if (fromDataProblems == null) {
        fromDataProblems = new StringBuffer();
      }

      fromDataProblems.append(s);
      fromDataProblems.append("\n\n");
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {

      super.fromData(in, context);
      instantiatorClassName = DataSerializer.readNonPrimitiveClassName(in);
      instantiatedClassName = DataSerializer.readNonPrimitiveClassName(in);
      if (CacheClientNotifier.getInstance() != null) {
        // This is a server so we need to send the instantiator to clients
        // right away. For that we need to load the class as the constructor of
        // ClientDataSerializerMessage requires instantiated class.
        try {
          instantiatorClass = InternalDataSerializer.getCachedClass(instantiatorClassName);
        } catch (ClassNotFoundException ex) {
          recordFromDataProblem(String.format("Could not load instantiator class: %s", ex));
          instantiatorClass = null;
        }
        try {
          instantiatedClass = InternalDataSerializer.getCachedClass(instantiatedClassName);
        } catch (ClassNotFoundException ex) {
          recordFromDataProblem(String.format("Could not load instantiated class: %s", ex));
          instantiatedClass = null;
        }
      }

      id = in.readInt();
      eventId = DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      String instatiatorName = (instantiatorClass == null) ? instantiatorClassName
          : instantiatorClass.getName();
      String instatiatedName = (instantiatedClass == null) ? instantiatedClassName
          : instantiatedClass.getName();
      return String.format("Register Instantiator %s of class %s that instantiates a %s", id,
          instatiatorName, instatiatedName);
    }

  }
  /**
   * A distribution message that alerts other members of the distributed cache of a new
   * {@code Instantiator} being registered.
   *
   *
   * @since GemFire 5.0
   */
  public static class RegistrationContextMessage extends RegistrationMessage {

    private transient ClientProxyMembershipID context;

    /**
     * Constructor for {@code RegistrationConetxtMessage}
     */
    public RegistrationContextMessage() {

    }

    /**
     * Creates a new {@code RegistrationContextMessage} that broadcasts that the given
     * {@code Instantiator} was registered.
     */
    public RegistrationContextMessage(Instantiator s) {
      instantiatorClass = s.getClass();
      instantiatedClass = s.getInstantiatedClass();
      id = s.getId();
      eventId = (EventID) s.getEventId();
      context = (ClientProxyMembershipID) s.getContext();
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      if (fromDataProblems != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(fromDataProblems);
        }
      }
      if (instantiatorClass != null && instantiatedClass != null) {
        Instantiator s = newInstance(instantiatorClass, instantiatedClass, id);
        s.setEventId(eventId);
        s.setContext(context);
        InternalInstantiator.register(s, false);
      } else if (instantiatorClassName != null && instantiatedClassName != null) {
        InternalInstantiator.register(instantiatorClassName, instantiatedClassName,
            id, false, eventId, context);
      }
    }

    @Override
    public int getDSFID() {
      return REGISTRATION_CONTEXT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.context = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.context, out);
    }
  }

  public static void logInstantiators() {
    for (Instantiator instantiator : dsMap.values()) {
      logger.info("Instantiator registered with id {} class {}", instantiator.getId(),
          instantiator.getInstantiatedClass().getName());
    }

    for (InstantiatorAttributesHolder holder : idsToHolders.values()) {
      logger.info("Instantiator registered with holder id {} class {}", holder.getId(),
          holder.getInstantiatedClassName());
    }

  }
}
