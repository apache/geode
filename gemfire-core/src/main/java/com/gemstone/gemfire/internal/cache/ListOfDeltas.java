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
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.internal.cache.delta.Delta;


/**
 * 
 * @author Neeraj
 *
 */
public final class ListOfDeltas implements Delta {

  private  List<Delta> listOfDeltas;
  transient private int deltaAppliedIndex = 0;
  public ListOfDeltas(final int size) {
    this.listOfDeltas = new ArrayList<Delta>(size);
  }

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
