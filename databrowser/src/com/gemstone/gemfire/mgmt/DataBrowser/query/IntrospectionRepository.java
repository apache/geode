/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.CollectionTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.MapTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.ObjectIntrospector;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PdxHelper;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PrimitiveTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.StructTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class IntrospectionRepository {

  private Map<Object, IntrospectionResult> types      = null;
  private static IntrospectionRepository   repository = null;

  private IntrospectionRepository() {
    types = new HashMap<Object, IntrospectionResult>();
  }

  public static synchronized IntrospectionRepository singleton() {
    if (repository == null) {
      repository = new IntrospectionRepository();
    }
    return repository;
  }

  public IntrospectionResult getIntrospectionResult(Object type) {
    return this.types.get(type);
  }

  public void addIntrospectionResult(Object type, IntrospectionResult result) {
    LogUtil.fine("Adding a type for :" + type);
    this.types.put(type, result);
  }

  /**
   * Introspect the type using given object instance
   * 
   * @param objInstance
   *          Instance of an object whose type is to be introspected
   * @return IntrospectionResult instance for the type
   * @throws IntrospectionException
   *           if fails to introspect
   */
  public IntrospectionResult introspectTypeByObject(Object objInstance) 
      throws IntrospectionException {
    IntrospectionResult metaInfo = null;
    if (QueryUtil.isPdxInstance(objInstance)) {
      metaInfo = introspectPdxType(objInstance);
    } else {
      metaInfo = introspectType(objInstance.getClass());
    }
    return metaInfo;
  }

  /**
   * Introspect the type using given
   * com.gemstone.gemfire.pdx.internal.PdxInstanceImpl object instance. The type
   * used for storing the introspection result for Pdx Instances is
   * com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PdxInfoType unlike
   * java.lang.Class for other objects.
   * 
   * It returns null if given pdxObjInstance is not a PdxInstance.
   * 
   * @param pdxObjInstance
   *          Instance of an object whose type is to be introspected
   * @return IntrospectionResult instance for the PdxInstanceImpl, null if
   *          given pdxObjInstance is not a PdxInstance.
   * @throws IntrospectionException
   *           if fails to introspect
   */
  public IntrospectionResult introspectPdxType(Object pdxObjInstance)
    throws IntrospectionException {
    IntrospectionResult metaInfo = null;
    
    if (QueryUtil.isPdxInstance(pdxObjInstance)) {
      PdxHelper pdxHelper   = PdxHelper.getInstance();
      Object    pdxInfoType = pdxHelper.getPdxInfoType(pdxObjInstance);
      if(!repository.isTypeIntrospected(pdxInfoType)) {//PdxInstance not known
        metaInfo = pdxHelper.getPdxMetaInfo(pdxObjInstance);
        repository.addIntrospectionResult(pdxInfoType, metaInfo);
      } else { //PdxInstance already known
        metaInfo = repository.getIntrospectionResult(pdxInfoType);
      }
    }
    return metaInfo;
  }

  public IntrospectionResult introspectType(Object type)
      throws IntrospectionException {
    if (type instanceof Class) {
      Class classType = (Class) type;
      IntrospectionResult metaInfo = null;

      if (QueryUtil.isPrimitiveType(classType)) {
        if (!repository.isTypeIntrospected(type)) {
          metaInfo = new PrimitiveTypeResultImpl(classType);
          repository.addIntrospectionResult(classType, metaInfo);
        }
      } else if (QueryUtil.isCompositeType(classType)) {
        if (!repository.isTypeIntrospected(type)) {
          metaInfo = ObjectIntrospector.introspectClass(classType);
          repository.addIntrospectionResult(classType, metaInfo);
        }
      }

      else if (QueryUtil.isCollectionType(classType)) {
        if (!repository.isTypeIntrospected(type)) {
          metaInfo = new CollectionTypeResultImpl(classType);
          repository.addIntrospectionResult(classType, metaInfo);
        }
      }

      else if (QueryUtil.isMapType(classType)) {
        if (!repository.isTypeIntrospected(type)) {
          metaInfo = new MapTypeResultImpl(classType);
          repository.addIntrospectionResult(classType, metaInfo);
        }
      }
    }

    if (type instanceof StructType) {
      StructType sType = (StructType) type;
      if (!repository.isTypeIntrospected(type)) {
        IntrospectionResult metaInfo = new StructTypeResultImpl(sType);
        repository.addIntrospectionResult(type, metaInfo);
      }
    }

    return repository.getIntrospectionResult(type);
  }

  public boolean isTypeIntrospected(Object type) {
    return this.types.containsKey(type);
  }

  public IntrospectionResult[] getIntrospectionResultInfo() {
    return types.values().toArray(new IntrospectionResult[0]);
  }

  /**
   * Cleans up introspected Pdx Types. Should be called on re-connect. 
   */
  //NOTE: Why to do this?:
  // The next connection from Data Browser could be to some other
  // DS/Cache Server which might have different version of the same object
  public boolean removePdxIntrospectionResults() {
    boolean clearedOldInfo = false;
    if (!types.isEmpty()) {
      Set<Entry<Object, IntrospectionResult>> entries = types.entrySet();
      for (Iterator<Entry<Object, IntrospectionResult>> it = entries.iterator(); it.hasNext();) {
        Entry<Object, IntrospectionResult> entry = (Entry<Object, IntrospectionResult>) it.next();
        IntrospectionResult metaInfo = entry.getValue();
        /* remove if metaInfo is null or it is PdxIntrospectionResult */
        if (metaInfo == null || 
            metaInfo.getResultType() == IntrospectionResult.PDX_TYPE_COLUMN || 
            metaInfo.getResultType() == IntrospectionResult.PDX_OBJECT_TYPE_COLUMN) {
          it.remove();
        }
      }
      PdxHelper.getInstance().clearKnownPdxTypeInfo();
      clearedOldInfo = true;
    }
    
    return clearedOldInfo;
  }
}
