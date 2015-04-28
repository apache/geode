/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.internal.cache.delta.*;
import com.gemstone.gemfire.InternalGemFireException;

import com.gemstone.gemfire.cache.EntryEvent;


/**
 * 
 * @author Neeraj
 *
 */
public final class ListOfDeltas implements Delta {

  private  List<Delta> listOfDeltas;
  transient private int deltaAppliedIndex = 0;

  public ListOfDeltas(Delta deltaObj) {
    this.listOfDeltas = new ArrayList<Delta>();
    this.listOfDeltas.add(deltaObj);
  }
  
  public ListOfDeltas() {    
  }

  

  public Object apply(EntryEvent ev) {
    if (ev != null && ev instanceof EntryEventImpl) {
      EntryEventImpl putEvent = (EntryEventImpl)ev;
      int last = this.listOfDeltas.size() -1;
      for (int i = this.deltaAppliedIndex; i < listOfDeltas.size(); i++) {
        Object o = listOfDeltas.get(i).apply(putEvent);
        if(i < last) { 
          putEvent.setOldValue(o);
        }else {
          putEvent.setNewValue(o);
        }
      }
      return putEvent.getNewValue();
    }
    else {
      throw new InternalGemFireException(
          "ListOfDeltas.apply: putEvent is either null "
              + "or is not of type EntryEventImpl");
    }
  }


  public Object merge(Object toMerge, boolean isCreate)
  {
    throw new UnsupportedOperationException("Invocation not expected");
  }
  
  public Object merge(Object toMerge)
  {
    this.listOfDeltas.add((Delta)toMerge); 
    return this;
  }
  
  public Object getResultantValue()
  {
    return this;
  } 
  
  public int getNumDeltas() {
    return this.listOfDeltas.size();
  }
  
  public void setDeltaAppliedIndex(int deltaApplied) {
    this.deltaAppliedIndex = deltaApplied;
  }
  
  public List<Delta> getListOfDeltas() {
    return Collections.unmodifiableList(this.listOfDeltas);
  }

}
//SqlFabric changes END