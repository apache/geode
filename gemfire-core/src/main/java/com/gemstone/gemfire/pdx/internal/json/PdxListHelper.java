/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal.json; 
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;

/*
 * This class is to convert JSON array into List.
 */
public class PdxListHelper {
  private static final Logger logger = LogService.getLogger();
  
  String m_name;
  PdxListHelper m_parent;
  List list = new LinkedList();
  
  public PdxListHelper(PdxListHelper parent, String name)
  {
    GemFireCacheImpl gci = (GemFireCacheImpl)CacheFactory.getAnyInstance();
    m_name = name;
    if(logger.isTraceEnabled()) {
      logger.trace("PdxListHelper name: {}", name  );
    }
    m_parent = parent;
  }
  
  public PdxListHelper getParent()
  {
    return m_parent;
  }
  
  public void setListName(String fieldName)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("setListName fieldName: {}", fieldName  );
    }
    m_name = fieldName;
  }
  public void addStringField(String fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addStringField fieldValue: {}", fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addByteField(byte fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addByteField fieldValue: {}", fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addShortField(short fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addShortField fieldValue: {}", fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addIntField(int fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addIntField fieldValue: {}", fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addLongField(long fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addLongField fieldValue: {}", fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addBigIntegerField(BigInteger fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addBigIntegerField fieldValue: {}", fieldValue  );
    }
    list.add(fieldValue);
  }
  
  public void addBooleanField(boolean fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addBooleanField fieldValue: {}", fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addFloatField(float fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addFloatField fieldValue: {}", fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addDoubleField(double fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addDoubleField fieldValue: {}", fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addBigDecimalField(BigDecimal fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addBigDecimalField fieldValue: {}", fieldValue );
    }
    list.add(fieldValue);
  }
  
  public void addNullField(Object fieldValue)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addNULLField fieldValue: {}",fieldValue   );
    }
    list.add(fieldValue);
  }
  
  public PdxListHelper addListField()
  {
    if(logger.isTraceEnabled()) {
      logger.trace("addListField");
    }
    PdxListHelper tmp = new PdxListHelper(this, "no-name");
    list.add(tmp);
    return tmp;
  }
  
  public PdxListHelper endListField()
  {
    if(logger.isTraceEnabled()) {
      logger.trace("endListField");
    }
    return m_parent;
  }
  
  public void addObjectField(String fieldName, PdxInstanceHelper dpi)
  {
    if(fieldName != null)
      throw new IllegalStateException("addObjectField:list should have object no fieldname");
    if(logger.isTraceEnabled()) {
      logger.trace("addObjectField fieldName: {}", fieldName);
    }
    //dpi.setPdxFieldName(fieldName);
    list.add(dpi.getPdxInstance());
  }
  
  public void endObjectField(String fieldName)
  {
    if(logger.isTraceEnabled()) {
      logger.trace("endObjectField fieldName: {}", fieldName);
    }
  }
  
  public List getList()
  {
    return list;
  }
}
