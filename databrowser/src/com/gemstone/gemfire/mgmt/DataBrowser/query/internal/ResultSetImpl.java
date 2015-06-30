/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ResultSet;

public class ResultSetImpl implements ResultSet {

  private Map<Object, IntrospectionResult> types;
  private Collection<?>                    results;
  
  public ResultSetImpl(Map<Object, IntrospectionResult> typs, Collection<?> res) {
    types = typs;
    results = (res != null) ? res : new ArrayList();
  }  
  
  public void close() {
   if(this.results != null) {
     this.results = null;
   }
   
   if(this.types != null) {
     this.types.clear();
     this.types = null;    
   }
  }

  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {

    Object type = (tuple instanceof Struct) ? ((Struct) tuple).getStructType()
        : tuple.getClass();

    if (types.containsKey(type)) {
      IntrospectionResult metaInfo = types.get(type);
      return metaInfo.getColumnValue(tuple, index);
    } 

    throw new ColumnNotFoundException("Could not identify type :" + type);
  }

  
  public IntrospectionResult[] getIntrospectionResult() {
    return this.types.values().toArray(new IntrospectionResult[0]);
  }


  public Collection<?> getQueryResult() {
    return this.results;
  }


  /**
   * This method returns the subset of result of the executed query for a given
   * introspection type.
   * 
   * @param metaInfo
   *          IntrospectionResult type for which we require result
   * @return subset of result of the query execution.
   * @see ResultSet#getQueryResult(IntrospectionResult)
   */
  public Collection<?> getQueryResult(IntrospectionResult metaInfo) {
    List result = new ArrayList();
    result.addAll(getQueryResult());

    if (metaInfo == null || metaInfo.getJavaType() == null) {
      return result;
    }

    if(metaInfo.getResultType() == IntrospectionResult.STRUCT_TYPE_RESULT) {
      //In case of Struct type of result, it is not possible to get different types of results.
      //Hence we should return the complete results.  
      return result;
    } 

    Iterator iter = result.iterator();

    while (iter.hasNext()) {
      Object obj = iter.next();
      if ( (obj == null) || !metaInfo.isCompatible(obj)) {
        iter.remove();
      }
    }

    return result;
  }


  public boolean isEmpty() {
    if(this.results != null)
     return results.isEmpty();

    return true; 
  }
  
  @Override
  public String toString() {
    return String.valueOf(results);
  }

}
