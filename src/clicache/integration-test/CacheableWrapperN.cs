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

using System;
using System.Collections.Generic;
using System.Collections;
using System.Runtime.Serialization;

namespace Apache.Geode.Client.UnitTests
{

  using NUnit.Framework;
  using Apache.Geode.Client;
  /// <summary>
  /// Interface class for wrappers of geode cacheable types.
  /// </summary>
  /// <remarks>
  /// This interface has various useful functions like setting value randomly,
  /// and finding checksum.
  /// </remarks>
  public abstract class CacheableWrapper
  {
    #region Protected members

    protected object m_cacheableObject = null;
    protected uint m_typeId;

    #endregion

    public virtual object Cacheable
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

    public abstract uint GetChecksum(object cacheableObject);

    public virtual int GetHashCodeN(object cacheableObject)
    {
      return cacheableObject.GetHashCode();
    }

    public virtual uint GetChecksum()
    {
      return GetChecksum(m_cacheableObject);
    }

    public virtual int GetHashCodeN()
    {
      return GetHashCodeN(m_cacheableObject);
    }

  }

  /// <summary>
  /// Interface class for wrappers of geode cacheable key types.
  /// </summary>
  /// <remarks>
  /// This interface has various useful functions like setting value randomly,
  /// and finding checksum, initializing key etc.
  /// </remarks>
  public abstract class CacheableKeyWrapper : CacheableWrapper
  {
    public override object Cacheable
    {
      get
      {
        return m_cacheableObject;
      }
    }

    public virtual object CacheableKey
    {
      get
      {
        return (object)m_cacheableObject;
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
    private static Dictionary<Type, UInt32>
      m_dotnetTypeVsCKWDelegate =
      new Dictionary<Type, UInt32>();

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

    public static CacheableWrapper CreateInstance(Object obj)
    {
      UInt32 typeId;
      lock (((ICollection)m_registeredValueTypeIdMap).SyncRoot)
      {
        if (m_dotnetTypeVsCKWDelegate.TryGetValue(obj.GetType(),
          out typeId))
        {
          return CreateInstance(typeId);
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

    public static CacheableKeyWrapper CreateKeyInstance(object obj)
    {
      uint typeId;
      lock (((ICollection)m_dotnetTypeVsCKWDelegate).SyncRoot)
      {
        if (m_dotnetTypeVsCKWDelegate.TryGetValue(obj.GetType(),
          out typeId))
        {
          return CreateKeyInstance(typeId);
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

    public static void RegisterType(UInt32 typeId,
      Type type, CacheableWrapperDelegate wrapperDelegate, Type dotnetType)
    {
      m_registeredValueTypeIdMap[typeId] = wrapperDelegate;
      m_typeIdNameMap[typeId] = type;
      m_dotnetTypeVsCKWDelegate[dotnetType] = typeId;
    }

    public static void RegisterKeyType(UInt32 typeId,
      Type type, CacheableKeyWrapperDelegate wrapperDelegate)
    {
      m_registeredKeyTypeIdMap[typeId] = wrapperDelegate;
      m_registeredValueTypeIdMap[typeId] = wrapperDelegate;
      m_typeIdNameMap[typeId] = type;
    }

    public static void RegisterKeyType(UInt32 typeId,
      Type type, CacheableKeyWrapperDelegate wrapperDelegate, Type dotnetType)
    {
      m_registeredKeyTypeIdMap[typeId] = wrapperDelegate;
      m_registeredValueTypeIdMap[typeId] = wrapperDelegate;
      m_typeIdNameMap[typeId] = type;
      m_dotnetTypeVsCKWDelegate[dotnetType] = typeId;
    }

    public static void ClearStaticVaraiables()
    {
      m_registeredKeyTypeIdMap.Clear();
      m_registeredValueTypeIdMap.Clear();
      m_typeIdNameMap.Clear();
      m_dotnetTypeVsCKWDelegate.Clear();

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
