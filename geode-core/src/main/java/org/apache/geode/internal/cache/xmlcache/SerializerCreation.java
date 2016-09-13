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
/**
 * 
 */
package org.apache.geode.internal.cache.xmlcache;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

import java.util.Map;
import java.util.Vector;
import java.util.HashMap;

import org.apache.logging.log4j.Logger;

/**
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
     * @see org.apache.geode.Instantiator#newInstance()
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
