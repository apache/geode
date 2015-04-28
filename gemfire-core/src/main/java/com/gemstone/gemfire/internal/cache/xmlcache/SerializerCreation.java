/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

import java.util.Map;
import java.util.Vector;
import java.util.HashMap;

import org.apache.logging.log4j.Logger;

/**
 * @author shaley
 *
 */
public class SerializerCreation {
  private static final Logger logger = LogService.getLogger();
  
  private final Vector<Class> serializerReg = new Vector<Class>();
  private final HashMap<Class, Integer> instantiatorReg = new HashMap<Class, Integer>();
 
  public static class InstantiatorImpl extends Instantiator{
    private Class m_class;
    
    /**
     * @param c
     * @param classId
     */
    public InstantiatorImpl(Class<? extends DataSerializable> c, int classId) {
      super(c, classId);
      m_class = c;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.Instantiator#newInstance()
     */
    @Override
    public DataSerializable newInstance() {
      try {            
        return (DataSerializable) m_class.newInstance();
      }
      catch(Exception ex) {
        logger.error(LocalizedMessage.create(LocalizedStrings.SerializerCreation_A_0_INSTANTIATION_FAILED, new Object[] {m_class.getName()}), ex);
        return null;
      }              
    }    
  }
  
  public SerializerCreation() {
  }
    
  public void registerSerializer(Class c) {
    serializerReg.add(c);
  }
  
  public void registerInstantiator(Class c, Integer id) {
    instantiatorReg.put(c, id);
  }
  
  public void create(){
    final boolean isDebugEnabled = logger.isDebugEnabled();
    for(Class c : serializerReg ) {
      if (isDebugEnabled) {
        logger.debug("Registering serializer: {}", c.getName());
      }
      DataSerializer.register(c);
    }
    
    for(Map.Entry<Class, Integer> e : instantiatorReg.entrySet()) {
      final Class k = e.getKey();
      if (isDebugEnabled) {
        logger.debug("Registering instantiator: {}", k.getName());
      }
      Instantiator.register(new InstantiatorImpl(k, e.getValue()));
    }
  }
  
  public Vector<Class> getSerializerRegistrations(){
    return serializerReg;
  }
  
  public HashMap<Class, Integer> getInstantiatorRegistrations() {
    return instantiatorReg;
  }
}
