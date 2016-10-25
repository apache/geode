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
package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.tier.sockets.ClientInstantiatorMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.i18n.StringId;

/**
 * Contains the implementation of {@link org.apache.geode.Instantiator}
 * registration and distribution messaging (and shared memory management).
 *
 * @since GemFire 3.5
 */
public class InternalInstantiator {

  private static final Logger logger = LogService.getLogger();
  
  /** Maps Classes to their ids */
  /**
   * Maps Class names to their Instantiator instance.
   */
  private static final ConcurrentMap/*<String,Instantiator>*/ dsMap = new ConcurrentHashMap();

  /** Maps the id of an instantiator to its Instantiator instance */
  private static final ConcurrentMap/*<Integer,Instantiator|Marker>*/ idsToInstantiators = new ConcurrentHashMap();

  /**
   * Maps the name of the instantiated-class to an instance of
   * InstantiatorAttributesHolder.
   */
  private static final ConcurrentHashMap<String, InstantiatorAttributesHolder> classNamesToHolders = new ConcurrentHashMap<String, InstantiatorAttributesHolder>();

  /**
   * Maps the id of an instantiator to an instance of
   * InstantiatorAttributesHolder.
   */
  private static final ConcurrentHashMap<Integer, InstantiatorAttributesHolder> idsToHolders = new ConcurrentHashMap<Integer, InstantiatorAttributesHolder>();

  private static final String SERVER_CONNECTION_THREAD = "ServerConnection" ;
  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Registers an <code>Instantiator</code> with the data
   * serialization framework.
   */
  public static void register(Instantiator instantiator,
                                           boolean distribute) {
    // [sumedh] Skip the checkForThread() check if the instantiation has not
    // to be distributed. This allows instantiations from ServerConnection
    // thread in client security plugins, for example. This is particularly
    // useful for native clients that do not send a REGISTER_INSTANTIATORS
    // message rather rely on server side registration, so each server can
    // do the registration with distribute set to false.
    if(!distribute || checkForThread()) {
      _register(instantiator, distribute);
    }
  }

  /**
   * Actually registers an <code>Instantiator</code> with the data
   * serialization framework.
   *
   * @param instantiator
   * @param distribute
   * @throws IllegalArgumentException
   *         If the instantiator has an id of zero
   * @throws IllegalStateException
   *         The instantiator cannot be registered
   */
  private static void _register(Instantiator instantiator, boolean distribute)
  {
    if (instantiator == null) {
      throw new NullPointerException(LocalizedStrings.InternalInstantiator_CANNOT_REGISTER_A_NULL_INSTANTIATOR.toLocalizedString());
    }
    final int classId = instantiator.getId();
    if (classId == 0) {
      throw new IllegalArgumentException(LocalizedStrings.Instantiator_INSTANTIATOR_ID_CANNOT_BE_ZERO.toLocalizedString());
    }
    Class c = instantiator.getInstantiatedClass();
    final String cName = c.getName();
    {
      int oldId = getClassId(c);
      if (oldId != 0 && oldId != classId) {
        throw new IllegalStateException(LocalizedStrings.InternalInstantiator_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_IT_CANNOT_BE_REGISTERED_WTH_ID_2.toLocalizedString(new Object[] {c.getName(), Integer.valueOf(oldId), Integer.valueOf(classId)}));
      }
    }
    final Integer idx = Integer.valueOf(classId);

    synchronized(InternalInstantiator.class) {
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
          }
          else {
            Class oldClass =
              ((Instantiator) oldInst).getInstantiatedClass();
            if (!oldClass.getName().equals(cName)) {
              throw new IllegalStateException(
                LocalizedStrings.InternalInstantiator_CLASS_ID_0_IS_ALREADY_REGISTERED_FOR_CLASS_1_SO_IT_COULD_NOT_BE_REGISTED_FOR_CLASS_2
                  .toLocalizedString(new Object[] { Integer.valueOf(classId), oldClass.getName(), cName }));
            }
            else {
              return; // it was already registered
            }
          }
        }
        else {
          dsMap.put(cName, instantiator);
        }
      }
      while (retry);

      // if instantiator is getting registered for first time
      // its EventID will be null, so generate a new event id
      // the the distributed system is connected
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && instantiator.getEventId() == null) {
        instantiator.setEventId(new EventID(cache.getDistributedSystem()));
      }

      logger.info(LocalizedMessage.create(LocalizedStrings.InternalInstantiator_REGISTERED, new Object[] { Integer.valueOf(classId), c.getName()} ));
    }

    if (distribute) { // originated in this VM
      // send a message to other peers telling them about a newly-registered
      // instantiator, it also send event id of the originator along with the
      // instantiator
      sendRegistrationMessage(instantiator);
      // send it to cache servers if it is a client
      sendRegistrationMessageToServers(instantiator);
    }
    // send it to all cache clients irelevent of distribute
    // bridge servers send it all the clients irelevent of
    // originator VM
    sendRegistrationMessageToClients(instantiator);

    InternalDataSerializer.fireNewInstantiator(instantiator);
  }

  /**
   * On receving on client, when the instantiator gets loaded, and if it has
   * static initializer which registers instantiators having distribute
   * flag as true. And as distribute flag is true, client sends
   * instantiator registartion recursively to server.
   * Similarly, on server side, when the instantiator gets loaded, and if it has
   * static initializer which registers instantiators having distribute
   * flag as true. But eventId is not set on this case so it generates
   * itw own event ID , to avoid this just return
   */
  private static boolean checkForThread(){
    String name = Thread.currentThread().getName();
    return !(name.startsWith(CacheClientUpdater.CLIENT_UPDATER_THREAD_NAME)
        || name.startsWith(SERVER_CONNECTION_THREAD));
  }

  /**
   * Sets the EventID to the instantiator if distributed system is created
   */
  public static EventID generateEventId(){
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == null){
      // A cache has not yet created
      return null;
    }
    return new EventID(InternalDistributedSystem.getAnyInstance());
  }

  /**
   * Sends Instantiator registration message to one of the servers
   *
   * @param instantiator
   */
  private static void sendRegistrationMessageToServers(Instantiator instantiator)
  {
    PoolManagerImpl.allPoolsRegisterInstantiator(instantiator);
  }
  
  /**
   * Sends Instantiator registration message to one of the servers
   *
   * @param holder
   */
  private static void sendRegistrationMessageToServers(InstantiatorAttributesHolder holder)
  {
    PoolManagerImpl.allPoolsRegisterInstantiator(holder);
  }

  /**
   * Sends Instantiator registration message to all cache clients
   *
   * @param instantiator
   */
  private static void sendRegistrationMessageToClients(Instantiator instantiator)
  {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      // A cache has not yet been created.
      // we can't propagate it to clients
      return;
    }
    byte[][] serializedInstantiators = new byte[3][];
    try {
      serializedInstantiators[0] = CacheServerHelper.serialize(instantiator
          .getClass().toString().substring(6));
      serializedInstantiators[1] = CacheServerHelper.serialize(instantiator
          .getInstantiatedClass().toString().substring(6));
      {
        byte[] idBytes = new byte[4];
        Part.encodeInt(instantiator.getId(), idBytes);
        serializedInstantiators[2] = idBytes;
      }
    }
    catch (IOException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("IOException encountered while serializing instantiators using CacheServerHelper.serialize() method");
      }
    }
    ClientInstantiatorMessage clientInstantiatorMessage = new ClientInstantiatorMessage(
        EnumListenerEvent.AFTER_REGISTER_INSTANTIATOR, serializedInstantiators,
        (ClientProxyMembershipID)instantiator.getContext(), (EventID)instantiator.getEventId());
    // Deliver it to all the clients
    CacheClientNotifier.routeClientMessage(clientInstantiatorMessage);
  }

  /**
   * Creates a new <code>Instantiator</code> with the given class and
   * id and {@linkplain #register(Instantiator, boolean) registers} it
   * with the data serialization framework.
   *
   * @throws IllegalArgumentException
   *         The instantiator cannot be created
   * @throws IllegalStateException
   *         The instantiator cannot be registered
   */
  public static void register(Class instantiatorClass,
                              Class instantiatedClass,
                              int id, boolean distribute) {

    if(checkForThread()) {
      Instantiator inst = newInstance(instantiatorClass,
          instantiatedClass, id );
      _register(inst, distribute);
    }
  }

  /**
   * Creates a new <code>Instantiator</code> with the given class and
   * id and {@linkplain #register(Instantiator, boolean) registers} it
   * with the data serialization framework.
   *
   * This method is only called when server connection and CacheClientUpdaterThread
   *
   * @throws IllegalArgumentException
   *         The instantiator cannot be created
   * @throws IllegalStateException
   *         The instantiator cannot be registered
   */
  public static void register(Class instantiatorClass,
                              Class instantiatedClass,
                              int id, boolean distribute, EventID eventId, ClientProxyMembershipID context) {
    Instantiator inst = newInstance(instantiatorClass,
                                    instantiatedClass, id );
    //This method is only called when server connection and CacheClientUpdaterThread
    inst.setEventId(eventId);
    inst.setContext(context);
    _register(inst, distribute);
  }

  /**
   * Lazily creates a new <code>Instantiator</code> with the given class and id.
   * 
   * @throws IllegalArgumentException
   *           The instantiator cannot be created
   * @throws IllegalStateException
   *           The instantiator cannot be registered
   */
  public static void register(String instantiatorClass,
                              String instantiatedClass,
                              int id, boolean distribute) {

    if (checkForThread()) {
      register(instantiatorClass, new InstantiatorAttributesHolder(
          instantiatorClass, instantiatedClass, id), distribute);
    }
  }

  /**
   * Lazily creates a new <code>Instantiator</code> with the given class and id.
   * 
   * This method is only called when server connection and
   * CacheClientUpdaterThread
   * 
   * @throws IllegalArgumentException
   *           The instantiator cannot be created
   * @throws IllegalStateException
   *           The instantiator cannot be registered
   */
  public static void register(String instantiatorClass,
                              String instantiatedClass,
                              int id, boolean distribute, 
                              EventID eventId, 
                              ClientProxyMembershipID context) {
    register(instantiatorClass, new InstantiatorAttributesHolder(
        instantiatorClass, instantiatedClass, id, eventId, context), distribute);
  }

  private static void register(String instantiatorClassName,
      InstantiatorAttributesHolder holder, boolean distribute) {
    
    Object inst = null;
    synchronized(InternalInstantiator.class) {
      inst = idsToInstantiators.get(holder.getId());
      if (inst == null) {
        if (instantiatorClassName == null || instantiatorClassName.trim().equals("")) {
          throw new IllegalArgumentException("Instantiator class name cannot be null or empty.");
        }
        if (holder.getId() == 0) {
          throw new IllegalArgumentException(LocalizedStrings.Instantiator_INSTANTIATOR_ID_CANNOT_BE_ZERO.toLocalizedString());
        }

        InstantiatorAttributesHolder iah = classNamesToHolders.putIfAbsent(
            holder.getInstantiatedClassName(), holder);

        if (iah != null && iah.getId() != holder.getId()) {
          throw new IllegalStateException(
              LocalizedStrings.InternalInstantiator_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_IT_CANNOT_BE_REGISTERED_WTH_ID_2
              .toLocalizedString(new Object[] {instantiatorClassName,
                  iah.getId(), holder.getId()}));
        }

        idsToHolders.putIfAbsent(holder.getId(), holder);

        logger.info(LocalizedMessage.create(LocalizedStrings.InternalInstantiator_REGISTERED_HOLDER, new Object[] { Integer.valueOf(holder.getId()), holder.getInstantiatedClassName()} ));

        if (distribute) {
          sendRegistrationMessageToServers(holder);
        }
        return;
      }
    }
    
    if (inst instanceof Marker) {
      Class instantiatorClass = null, instantiatedClass = null;
      try {
        // fix bug 46355, need to move getCachedClass() outside of sync
        instantiatorClass = InternalDataSerializer.getCachedClass(
            holder.getInstantiatorClassName());
        instantiatedClass = InternalDataSerializer.getCachedClass(
            holder.getInstantiatedClassName());
      } catch (ClassNotFoundException cnfe) {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache != null && cache.getLoggerI18n() != null
            && cache.getLoggerI18n().infoEnabled()) {
          cache
              .getLoggerI18n()
              .info(
                  LocalizedStrings.InternalInstantiator_COULD_NOT_LOAD_INSTANTIATOR_CLASS_0,
                  new Object[] { cnfe.getMessage() });
        }
      }
      synchronized(InternalInstantiator.class) {
        Object inst2 = idsToInstantiators.get(holder.getId());
        if (inst2 == inst) {
          register(instantiatorClass, instantiatedClass, holder.getId(),
              distribute, holder.getEventId(), holder.getContext());
        } else {
          if (inst2 == null || inst2 instanceof Marker) {
            // recurse
            register(instantiatorClassName, holder, distribute);
          } else {
            // already registered
            return;
          }
        }
      }
    }
  }

  public static class InstantiatorAttributesHolder {
    private String instantiatorName;
    private String instantiatedName;
    private int id;
    private EventID eventId;
    private ClientProxyMembershipID context;

    public InstantiatorAttributesHolder(String instantiatorClass,
        String instantiatedClass, int id) {
      this.instantiatorName = instantiatorClass;
      this.instantiatedName = instantiatedClass;
      this.id = id;
    }

    public InstantiatorAttributesHolder(String instantiatorClass,
        String instantiatedClass, int id, EventID eventId,
        ClientProxyMembershipID context) {
      this.instantiatorName = instantiatorClass;
      this.instantiatedName = instantiatedClass;
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
      return "InstantiatorAttributesHolder[irName=" + this.instantiatorName
          + ",idName=" + this.instantiatedName + ",id=" + this.id
          + (this.eventId != null ? ",this.eventId=" + this.eventId : "")
          + (this.context != null ? ",this.context=" + this.context : "") + "]";
    }
  }
  /**
   * Unregisters the given class with the given class id with the
   * <code>Instantiator</code>.
   *
   * @throws IllegalArgumentException
   *         If <code>c</code> was not previously registered with id
   *         <code>classId</code>.
   * @throws NullPointerException
   *         If <code>c</code> is <code>null</code>
   */
  public static synchronized void unregister(Class c, int classId) {
    if (c == null) {
      throw new NullPointerException(LocalizedStrings.InternalInstantiator_CANNOT_UNREGISTER_A_NULL_CLASS.toLocalizedString());
    }
    final Integer idx = Integer.valueOf(classId);
    final Instantiator i = (Instantiator)idsToInstantiators.remove(idx);
    if (i == null) {
      throw new IllegalArgumentException(LocalizedStrings.InternalInstantiator_CLASS_0_WAS_NOT_REGISTERED_WITH_ID_1.toLocalizedString(new Object[] {c.getName(), Integer.valueOf(classId)}));
    } else {
      dsMap.remove(c.getName(), i);
    }
    idsToHolders.remove(idx);
    classNamesToHolders.remove(i.getInstantiatedClass().getName());
  }
  
  // testhook that removes all registed instantiators
  public static void reinitialize() {
    idsToInstantiators.clear();
    dsMap.clear();
    idsToHolders.clear();
    classNamesToHolders.clear();
  }

  /**
   * Returns the class id for the given class.
   *
   * @return <code>0</code> if the class has not be registered
   *
   * @see DataSerializer#writeObject(Object, DataOutput)
   */
  public static int getClassId(Class c) {
    int result = 0;
    final Instantiator i = (Instantiator)dsMap.get(c.getName());
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
    final Integer idx = Integer.valueOf(classId);
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
        marker = (Marker)o;
      } else {
        return (Instantiator)o;
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
          Class instantiatorClass = InternalDataSerializer.getCachedClass(
              holder.getInstantiatorClassName());
          Class instantiatedClass = InternalDataSerializer.getCachedClass(
              holder.getInstantiatedClassName());
          // 46355: move getCachedClass out of sync
          synchronized (InternalInstantiator.class) {
            register(instantiatorClass, instantiatedClass, holder.getId(), false,
                holder.getEventId(), holder.getContext());
            classNamesToHolders.remove(holder.getInstantiatedClassName());
            idsToHolders.remove(classId);
            instantiator = (Instantiator)idsToInstantiators.get(classId);
          }
        } catch (ClassNotFoundException cnfe) {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          if (cache != null && cache.getLoggerI18n() != null
              && cache.getLoggerI18n().infoEnabled()) {
            cache
                .getLoggerI18n()
                .info(
                    LocalizedStrings.InternalInstantiator_COULD_NOT_LOAD_INSTANTIATOR_CLASS_0,
                    new Object[] {cnfe.getMessage()});
          }
        }
      }
      return instantiator;
    }
  }

  /**
   * If we are connected to a distributed system, send a message to
   * other members telling them about a newly-registered instantiator.
   */
  private static void sendRegistrationMessage(Instantiator s) {
    InternalDistributedSystem system =
      InternalDistributedSystem.getAnyInstance();
    if (system != null) {
      RegistrationMessage m = null ;
      if(s.getContext()== null){
        m = new RegistrationMessage(s);
      }else{
        m = new RegistrationContextMessage(s);
      }
      system.getDistributionManager().putOutgoing(m);
    }
  }

  /**
   * Reflectively instantiates an instance of
   * <code>Instantiator</code>.
   *
   * @param instantiatorClass
   *        The implementation of <code>Instantiator</code> to
   *        instantiate
   * @param instantiatedClass
   *        The implementation of <code>DataSerialization</code> that
   *        will be produced by the <code>Instantiator</code>
   *
   * @throws IllegalArgumentException
   *         If the class can't be instantiated
   */
  protected static Instantiator newInstance(Class instantiatorClass,
                                            Class instantiatedClass,
                                            int id)  {
    if (!Instantiator.class.isAssignableFrom(instantiatorClass)) {
      throw new IllegalArgumentException(LocalizedStrings.InternalInstantiator_0_DOES_NOT_EXTEND_INSTANTIATOR.toLocalizedString(instantiatorClass.getName()));
    }

    Constructor init;
    boolean intConstructor = false;
    Class[] types;
    try {
      types = new Class[] { Class.class, int.class };
      init = instantiatorClass.getDeclaredConstructor(types);
      intConstructor = true;
    } catch (NoSuchMethodException ex) {
      // for backwards compat check for (Class, byte)
      try {
        types = new Class[] { Class.class, byte.class };
        init = instantiatorClass.getDeclaredConstructor(types);
      } catch (NoSuchMethodException ex2) {
        StringId msg = LocalizedStrings.InternalInstantiator_CLASS_0_DOES_NOT_HAVE_A_TWOARGUMENT_CLASS_INT_CONSTRUCTOR;
        Object[] msgArgs = new Object[] { instantiatorClass.getName() };
        if (instantiatorClass.getDeclaringClass() != null) {
          msg = LocalizedStrings.InternalInstantiator_CLASS_0_DOES_NOT_HAVE_A_TWOARGUMENT_CLASS_INT_CONSTRUCTOR_IT_IS_AN_INNER_CLASS_OF_1_SHOULD_IT_BE_A_STATIC_INNER_CLASS;
          msgArgs = new Object[] { instantiatorClass.getName(), instantiatorClass.getDeclaringClass() };
        }
        throw new IllegalArgumentException(msg.toLocalizedString(msgArgs));
      }
    }

    Instantiator s;
    try {
      init.setAccessible(true);
      Object[] args = new Object[] {instantiatedClass,
                                    intConstructor
                                    ? (Object)Integer.valueOf(id)
                                    : (Object)Byte.valueOf((byte)id)};
      s = (Instantiator) init.newInstance(args);

    } catch (IllegalAccessException ex) {
      throw new IllegalArgumentException(LocalizedStrings.InternalInstantiator_COULD_NOT_ACCESS_ZEROARGUMENT_CONSTRUCTOR_OF_0.toLocalizedString(instantiatorClass.getName()));

    } catch (InstantiationException ex) {
      RuntimeException ex2 = new IllegalArgumentException(LocalizedStrings.InternalInstantiator_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0.toLocalizedString(instantiatorClass.getName()));
      ex2.initCause(ex);
      throw ex2;

    } catch (InvocationTargetException ex) {
      RuntimeException ex2 = new IllegalArgumentException(LocalizedStrings.InternalInstantiator_WHILE_INSTANTIATING_AN_INSTANCE_OF_0.toLocalizedString(instantiatorClass.getName()));
      ex2.initCause(ex);
      throw ex2;
    }

    return s;
  }

  /**
   * Returns all of the currently registered instantiators
   */
  public static Instantiator[] getInstantiators() {
    Collection coll = new ArrayList();
    if (!classNamesToHolders.isEmpty()) {
      Iterator it = classNamesToHolders.values().iterator();
      while (it.hasNext()) {
        try {
          InstantiatorAttributesHolder holder = (InstantiatorAttributesHolder)it
              .next();
          Class instantiatorClass = InternalDataSerializer.getCachedClass(
              holder.getInstantiatorClassName());
          Class instantiatedClass = InternalDataSerializer.getCachedClass(
              holder.getInstantiatedClassName());
          synchronized (InternalInstantiator.class) {
            if (!idsToInstantiators.containsKey(holder.getId())) {
              register(instantiatorClass, instantiatedClass, holder.getId(), false,
                  holder.getEventId(), holder.getContext());
            }
            classNamesToHolders.remove(holder.getInstantiatedClassName());
            idsToHolders.remove(holder.getId());
          }
        } catch (ClassNotFoundException cnfe) {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          if (cache != null && cache.getLoggerI18n() != null
              && cache.getLoggerI18n().infoEnabled()) {
            cache
                .getLoggerI18n()
                .info(
                    LocalizedStrings.InternalInstantiator_COULD_NOT_LOAD_INSTANTIATOR_CLASS_0,
                    new Object[] {cnfe.getMessage()});
          }
        }
      }
    }
    coll.addAll(dsMap.values()); // Don't move it before the if block above.
    return (Instantiator[]) coll.toArray(new Instantiator[coll.size()]);
  }

  /**
   * Does not trigger loading of the instantiator/instantiated classes into the
   * vm.
   * 
   * @return array of InstantiatorAttributesArray instances.
   */
  public static Object[] getInstantiatorsForSerialization() {
    Collection coll = new ArrayList(dsMap.size() + idsToHolders.size());
    coll.addAll(dsMap.values());
    coll.addAll(classNamesToHolders.values()); // TODO (ashetkar) will it add duplicates?
    return coll.toArray(new Object[coll.size()]);
  }

  public static int getIdsToHoldersSize() {
    return idsToHolders.size();
  }

  public static int getNamesToHoldersSize() {
    return classNamesToHolders.size();
  }
  ///////////////////////  Inner Classes  ///////////////////////

  /**
   * A marker object for <Code>Instantiator</code>s that have not
   * been registered.  Using this marker object allows us to
   * asynchronously send <Code>Instantiator</code> registration
   * updates.  If the serialized bytes arrive at a VM before the
   * registration message does, the deserializer will wait an amount
   * of time for the registration message to arrive.
   */
  static class Marker {

    /** The Instantiator that is filled in upon registration */
    private volatile Instantiator instantiator = null;

    /**
     * Creates a new <code>Marker</code> whose {@link #getInstantiator}
     * method will wait for the instantiator to be registered.
     */
    Marker() {

    }

    /**
     * Returns the instantiator associated with this marker.  If the
     * instantiator has not been registered yet, then this method will
     * wait until the instantiator is registered.  If this method has to
     * wait for too long, then <code>null</code> is returned.
     */
    Instantiator getInstantiator() {
      synchronized (this) {
        if (this.instantiator == null) {
          try {
            this.wait(InternalDataSerializer.GetMarker.WAIT_MS);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // just return null, let it fail.
            return null;
          }
        }

        return this.instantiator;
      }
    }

    /**
     * Sets the instantiator associated with this marker.  It will
     * notify any threads that are waiting for the instantiator to be
     * registered.
     */
    void setInstantiator(Instantiator instantiator) {
      synchronized (this) {
        this.instantiator = instantiator;
        this.notifyAll();
      }
    }
  }

  /**
   * Persist this class's map to out
   */
  public static void saveRegistrations(DataOutput out) throws IOException {
    for (Instantiator inst : InternalInstantiator.getInstantiators()) {
      out.writeInt(inst.getId());
      DataSerializer.writeClass(inst.getClass(), out);
      DataSerializer.writeClass(inst.getInstantiatedClass(), out);
    }
    // We know that Instantiator id's must not be 0 so write a zero
    // to mark then end of the instantiators.
    out.writeInt(0);
  }

  /**
   * Read the data from in and register it with this class.
   * @throws IllegalArgumentException if a registration fails
   */
  public static void loadRegistrations(DataInput in) throws IOException
  {
    int instId;
    while ((instId = in.readInt()) != 0) {
      Class instClass = null;
      Class instantiatedClass = null;
      boolean skip = false;
      try {
        instClass = DataSerializer.readClass(in);
      } catch (ClassNotFoundException ex) {
        skip = true;
      }
      try {
        instantiatedClass = DataSerializer.readClass(in);
      } catch (ClassNotFoundException ex) {
        skip = true;
      }
      if (skip) {
        continue;
      }
      register(newInstance(instClass, instantiatedClass, instId), true);
    }
  }

  /**
   * A distribution message that alerts other members of the
   * distributed cache of a new <code>Instantiator</code> being
   * registered.
   */
  public static class RegistrationMessage extends SerialDistributionMessage {
    /**
     * The <code>Instantiator</code> class that was registered
     */
    protected Class instantiatorClass;

    /** The class that is instantiated by the instantiator */
    protected Class instantiatedClass;

    /** The id of the <codE>Instantiator</code> that was
     * registered */
    protected int id;

    /** The eventId of the <codE>Instantiator</code> that was
     * registered */
    protected EventID eventId;

    /** Problems encountered while running fromData.  See bug
     * 31573. */
    protected transient StringBuffer fromDataProblems;

    /**
     * The name of the <code>Instantiator</code> class that was registered
     */
    protected String instantiatorClassName;

    /** Name of the class that is instantiated by the instantiator */
    protected String instantiatedClassName;

    /**
     * Constructor for <code>DataSerializable</code>
     */
    public RegistrationMessage() {

    }

    /**
     * Creates a new <code>RegistrationMessage</code> that broadcasts
     * that the given <code>Instantiator</code> was registered.
     */
    public RegistrationMessage(Instantiator s) {
      this.instantiatorClass = s.getClass();
      this.instantiatedClass = s.getInstantiatedClass();
      this.id = s.getId();
      this.eventId = (EventID)s.getEventId();
    }

    @Override
    protected void process(DistributionManager dm) {
      if (this.fromDataProblems != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(this.fromDataProblems);
        }
      }

      if (this.instantiatorClass != null &&
          this.instantiatedClass != null) {
        Instantiator s = newInstance(this.instantiatorClass,
                                     this.instantiatedClass, this.id);
        s.setEventId(eventId);
        InternalInstantiator.register(s, false);
      } else if (this.instantiatorClassName != null
          && this.instantiatedClassName != null) {
        InternalInstantiator.register(this.instantiatorClassName,
            this.instantiatedClassName, this.id, false, this.eventId, null);
      }
    }

    public int getDSFID() {
      return REGISTRATION_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeNonPrimitiveClassName(this.instantiatorClass.getName(), out);
      DataSerializer.writeNonPrimitiveClassName(this.instantiatedClass.getName(), out);
      out.writeInt(this.id);
      DataSerializer.writeObject(this.eventId, out);
    }

    private void fromDataProblem(String s) {
      if (this.fromDataProblems == null) {
        this.fromDataProblems = new StringBuffer();
      }

      this.fromDataProblems.append(s);
      this.fromDataProblems.append("\n\n");
    }

    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {

      super.fromData(in);
      this.instantiatorClassName = DataSerializer.readNonPrimitiveClassName(in);
      this.instantiatedClassName = DataSerializer.readNonPrimitiveClassName(in);
      if (CacheClientNotifier.getInstance() != null) {
        // This is a server so we need to send the instantiator to clients
        // right away. For that we need to load the class as the constructor of
        // ClientDataSerializerMessage requires instantiated class.
        try {
          this.instantiatorClass = InternalDataSerializer.getCachedClass(this.instantiatorClassName); // fix for bug 41206
        } catch (ClassNotFoundException ex) {
          fromDataProblem(
              LocalizedStrings.InternalInstantiator_COULD_NOT_LOAD_INSTANTIATOR_CLASS_0
              .toLocalizedString(ex));
          this.instantiatorClass = null;
        }
        try {
          this.instantiatedClass = InternalDataSerializer.getCachedClass(this.instantiatedClassName); // fix for bug 41206
        } catch (ClassNotFoundException ex) {
          fromDataProblem(
              LocalizedStrings.InternalInstantiator_COULD_NOT_LOAD_INSTANTIATED_CLASS_0
              .toLocalizedString(ex));
          this.instantiatedClass = null;
        }
      }

      this.id = in.readInt();
      this.eventId = (EventID)DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      String instatiatorName = (this.instantiatorClass == null) ? this.instantiatorClassName : this.instantiatorClass.getName();
      String instatiatedName = (this.instantiatedClass == null) ? this.instantiatedClassName : this.instantiatedClass.getName();
      return LocalizedStrings.InternalInstantiator_REGISTER_INSTANTIATOR_0_OF_CLASS_1_THAT_INSTANTIATES_A_2.toLocalizedString( new Object[] {Integer.valueOf(this.id), instatiatorName, instatiatedName});
    }

  }
  /**
   * A distribution message that alerts other members of the
   * distributed cache of a new <code>Instantiator</code> being
   * registered.
   *
   *
   * @since GemFire 5.0
   */
  public static final class RegistrationContextMessage extends RegistrationMessage {

    private transient ClientProxyMembershipID context;

    /**
     * Constructor for <code>RegistrationConetxtMessage</code>
     */
    public RegistrationContextMessage() {

    }

    /**
     * Creates a new <code>RegistrationContextMessage</code> that broadcasts
     * that the given <code>Instantiator</code> was registered.
     */
    public RegistrationContextMessage(Instantiator s) {
      this.instantiatorClass = s.getClass();
      this.instantiatedClass = s.getInstantiatedClass();
      this.id = s.getId();
      this.eventId = (EventID)s.getEventId();
      this.context = (ClientProxyMembershipID)s.getContext();
    }

    @Override
    protected void process(DistributionManager dm) {
      if (fromDataProblems != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(fromDataProblems);
        }
      }
      if (this.instantiatorClass != null &&
          this.instantiatedClass != null) {
        Instantiator s = newInstance(this.instantiatorClass,
                                     this.instantiatedClass, this.id);
        s.setEventId(this.eventId);
        s.setContext(this.context);
        InternalInstantiator.register(s, false);
      } else if (this.instantiatorClassName != null &&
          this.instantiatedClassName != null) {
        InternalInstantiator.register(this.instantiatorClassName, this.instantiatedClassName, this.id, false, this.eventId, this.context);
      }
    }

    @Override
    public int getDSFID() {
      return REGISTRATION_CONTEXT_MESSAGE;
    }
    
    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.context = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.context, out);
    }
  }
  public static void logInstantiators() {
    for(Iterator itr = dsMap.values().iterator(); itr.hasNext(); ) {
      Instantiator instantiator = (Instantiator) itr.next();

      logger.info(LocalizedMessage.create(LocalizedStrings.InternalInstantiator_REGISTERED, new Object[] { Integer.valueOf(instantiator.getId()), instantiator.getInstantiatedClass().getName()} ));
    }
    
    for(Iterator itr = idsToHolders.values().iterator(); itr.hasNext(); ) {
      InstantiatorAttributesHolder holder = (InstantiatorAttributesHolder) itr.next();

      logger.info(LocalizedMessage.create(LocalizedStrings.InternalInstantiator_REGISTERED_HOLDER, new Object[] { Integer.valueOf(holder.getId()), holder.getInstantiatedClassName()} ));
    }
    
  }
}
