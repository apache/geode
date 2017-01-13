//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Collections;
using System.Runtime.Serialization;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;

  /// <summary>
  /// Interface class for wrappers of gemfire cacheable types.
  /// </summary>
  /// <remarks>
  /// This interface has various useful functions like setting value randomly,
  /// and finding checksum.
  /// </remarks>
  public abstract class CacheableWrapper
  {
    #region Protected members

    protected IGFSerializable m_cacheableObject = null;
    protected uint m_typeId;

    #endregion

    public virtual IGFSerializable Cacheable
    {
      get
      {
        return m_cacheableObject;
      }
    }

    public uint TypeId
    {
      get
      {
        return m_typeId;
      }
      set
      {
        m_typeId = value;
      }
    }

    public abstract void InitRandomValue(int maxSize);
    
    public abstract uint GetChecksum(IGFSerializable cacheableObject);

    public virtual uint GetChecksum()
    {
      return GetChecksum(m_cacheableObject);
    }
  }

  /// <summary>
  /// Interface class for wrappers of gemfire cacheable key types.
  /// </summary>
  /// <remarks>
  /// This interface has various useful functions like setting value randomly,
  /// and finding checksum, initializing key etc.
  /// </remarks>
  public abstract class CacheableKeyWrapper : CacheableWrapper
  {
    public override IGFSerializable Cacheable
    {
      get
      {
        return m_cacheableObject;
      }
    }

    public virtual ICacheableKey CacheableKey
    {
      get
      {
        return (ICacheableKey)m_cacheableObject;
      }
    }

    public abstract int MaxKeys
    {
      get;
    }

    public abstract void InitKey(int keyIndex, int maxSize);
  }

  public delegate CacheableWrapper CacheableWrapperDelegate();
  public delegate CacheableKeyWrapper CacheableKeyWrapperDelegate();

  /// <summary>
  /// Factory class to create <c>CacheableWrapper</c> objects.
  /// </summary>
  public static class CacheableWrapperFactory
  {
    #region Private members

    private static Dictionary<UInt32, CacheableKeyWrapperDelegate>
      m_registeredKeyTypeIdMap =
      new Dictionary<UInt32, CacheableKeyWrapperDelegate>();
    private static Dictionary<UInt32, Delegate>
      m_registeredValueTypeIdMap = new Dictionary<UInt32, Delegate>();
    private static Dictionary<UInt32, Type>
      m_typeIdNameMap = new Dictionary<UInt32, Type>();

    #endregion

    #region Public methods

    public static CacheableWrapper CreateInstance(UInt32 typeId)
    {
      Delegate wrapperDelegate;
      lock (((ICollection)m_registeredValueTypeIdMap).SyncRoot)
      {
        if (m_registeredValueTypeIdMap.TryGetValue(typeId,
          out wrapperDelegate))
        {
          CacheableWrapper wrapper =
            (CacheableWrapper)wrapperDelegate.DynamicInvoke(null);
          wrapper.TypeId = typeId;
          return wrapper;
        }
      }
      return null;
    }

    public static CacheableKeyWrapper CreateKeyInstance(UInt32 typeId)
    {
      CacheableKeyWrapperDelegate wrapperDelegate;
      lock (((ICollection)m_registeredKeyTypeIdMap).SyncRoot)
      {
        if (m_registeredKeyTypeIdMap.TryGetValue(typeId,
          out wrapperDelegate))
        {
          CacheableKeyWrapper wrapper = wrapperDelegate();
          wrapper.TypeId = typeId;
          return wrapper;
        }
      }
      return null;
    }

    public static void RegisterType(UInt32 typeId,
      Type type, CacheableWrapperDelegate wrapperDelegate)
    {
      m_registeredValueTypeIdMap[typeId] = wrapperDelegate;
      m_typeIdNameMap[typeId] = type;
    }

    public static void RegisterKeyType(UInt32 typeId,
      Type type, CacheableKeyWrapperDelegate wrapperDelegate)
    {
      m_registeredKeyTypeIdMap[typeId] = wrapperDelegate;
      m_registeredValueTypeIdMap[typeId] = wrapperDelegate;
      m_typeIdNameMap[typeId] = type;
    }

    public static void ClearStatics()
    {
      m_registeredKeyTypeIdMap.Clear();
      m_registeredValueTypeIdMap.Clear();
      m_typeIdNameMap.Clear();
    }

    public static Type GetTypeForId(UInt32 typeId)
    {
      Type type;
      lock (((ICollection)m_typeIdNameMap).SyncRoot)
      {
        if (m_typeIdNameMap.TryGetValue(typeId, out type))
        {
          return type;
        }
      }
      return null;
    }

    public static ICollection<UInt32> GetRegisteredKeyTypeIds()
    {
      return m_registeredKeyTypeIdMap.Keys;
    }

    public static ICollection<UInt32> GetRegisteredValueTypeIds()
    {
      return m_registeredValueTypeIdMap.Keys;
    }

    #endregion
  }
}
