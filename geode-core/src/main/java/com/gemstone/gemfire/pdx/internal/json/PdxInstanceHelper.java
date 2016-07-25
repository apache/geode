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
package com.gemstone.gemfire.pdx.internal.json; 
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;

/*
 * This class is intermediate class to create PdxInstance.
 */
public class PdxInstanceHelper {
  private static final Logger logger = LogService.getLogger();
  
  PdxInstanceHelper m_parent;
  PdxInstanceFactoryImpl m_pdxInstanceFactory;
  PdxInstance m_pdxInstance;
  String m_PdxName;//when pdx is member, else null if part of lists
  
  public PdxInstanceHelper(String className , PdxInstanceHelper parent)
  {
    GemFireCacheImpl gci = (GemFireCacheImpl)CacheFactory.getAnyInstance();
    if(logger.isTraceEnabled()) {
      logger.trace("ClassName {}", className );
    }
    m_PdxName = className;
    m_parent = parent;
    m_pdxInstanceFactory = (PdxInstanceFactoryImpl)gci.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME, false);
  }
  
  public PdxInstanceHelper getParent()
  {
    return m_parent;
  }
  
  public void setPdxFieldName(String name)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("setPdxClassName : {}", name);
    }
    m_PdxName = name;
  }

  public void addStringField(String fieldName, String value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addStringField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeString(fieldName, value);
  }
  
  public void addByteField(String fieldName, byte value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addByteField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeByte(fieldName, value);
  }
  
  public void addShortField(String fieldName, short value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addShortField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeShort(fieldName, value);
  }
  
  public void addIntField(String fieldName, int value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addIntField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeInt(fieldName, value);
  }
  
  public void addLongField(String fieldName, long value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addLongField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeLong(fieldName, value);
  }
  
  public void addBigDecimalField(String fieldName, BigDecimal value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addBigDecimalField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeObject(fieldName, value);    
  }
  
  public void addBigIntegerField(String fieldName, BigInteger value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addBigIntegerField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeObject(fieldName, value);    
  }
  
  public void addBooleanField(String fieldName, boolean value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addBooleanField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeBoolean(fieldName, value);
  }
  
  public void addFloatField(String fieldName, float value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addFloatField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeFloat(fieldName, value);
  }
  
  public void addDoubleField(String fieldName, double value)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addDoubleField fieldName: {}; value: {}", fieldName, value );
    }
    m_pdxInstanceFactory.writeDouble(fieldName, value);
  }
  
  public void addNullField(String fieldName)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addNullField fieldName: {}; value: NULL", fieldName);
    }
    m_pdxInstanceFactory.writeObject(fieldName, null);
  }
  
  public void addListField(String fieldName, PdxListHelper list)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addListField fieldName: {}", fieldName  );
    }
    m_pdxInstanceFactory.writeObject(fieldName, list.getList());
  }
  
  public void endListField(String fieldName)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("endListField fieldName: {}", fieldName  );
    }
  }
  
  public void addObjectField(String fieldName, PdxInstance member)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addObjectField fieldName: {}", fieldName  );
    }
    if(fieldName == null)
      throw new IllegalStateException("addObjectField:PdxInstance should have fieldname");
    m_pdxInstanceFactory.writeObject(fieldName, member);
  }
  
  public void endObjectField(String fieldName)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("endObjectField fieldName: {}", fieldName  );
    }
    m_pdxInstance = m_pdxInstanceFactory.create();
  }
  
  public PdxInstance getPdxInstance()
  {
    return m_pdxInstance;
  }
  public String getPdxFieldName()
  {
    //return m_fieldName != null ? m_fieldName : "emptyclassname"; //when object is just like {  }
    return m_PdxName ;
  }   
}
