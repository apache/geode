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
using System.Text;

using Apache.Geode.Client;

namespace Apache.Geode.Client.UnitTests
{
  [Serializable]
  class CustomSerializableObject
  {
    public String key;
    public String value;

    public CustomSerializableObject()
    {
      key = "key";
      value = "value";
    }
  }
  class DefaultCacheable : Apache.Geode.Client.IGFSerializable
  {
    Apache.Geode.Client.CacheableBoolean m_cacheableBoolean = null;
    Apache.Geode.Client.CacheableInt32 m_cacheableInt32 = null;
    Apache.Geode.Client.CacheableInt32Array m_cacheableInt32Array = null;
    Apache.Geode.Client.CacheableFileName m_cacheableFileName = null;
    Apache.Geode.Client.CacheableString m_CacheableStringASCII = null;
    Apache.Geode.Client.CacheableStringArray m_cacheableStringArray = null;
    Apache.Geode.Client.CacheableHashSet m_cacheableHashSet = null;
    Apache.Geode.Client.CacheableHashMap m_cacheableHashMap = null;
    Apache.Geode.Client.CacheableDate m_cacheableDate = null;
    Apache.Geode.Client.CacheableVector m_cacheableVector = null;
    Apache.Geode.Client.CacheableObject m_cacheableObject = null;

    bool m_initialized = false;

    public DefaultCacheable()
    { 
    
    }

    public DefaultCacheable(bool initialized)
    {
      if (initialized)
      {
        Apache.Geode.Client.Log.Fine(" in constructor");
        m_initialized = true;

        m_cacheableBoolean = Apache.Geode.Client.CacheableBoolean.Create(true);

        m_cacheableInt32 = Apache.Geode.Client.CacheableInt32.Create(1000);

        m_cacheableInt32Array = Apache.Geode.Client.CacheableInt32Array.Create(new Int32[] { 1, 2, 3 });

        m_cacheableFileName = Apache.Geode.Client.CacheableFileName.Create("gemstone.txt");

        m_CacheableStringASCII = Apache.Geode.Client.CacheableString.Create("asciistring");

        m_cacheableStringArray = Apache.Geode.Client.CacheableStringArray.Create(new string[] { "one", "two" });

        m_cacheableHashSet = Apache.Geode.Client.CacheableHashSet.Create(2);
        m_cacheableHashSet.Add(Apache.Geode.Client.CacheableString.Create("first"));
        m_cacheableHashSet.Add(Apache.Geode.Client.CacheableString.Create("second"));

        m_cacheableHashMap = new Apache.Geode.Client.CacheableHashMap(new Dictionary<string, string>() {{ "key-hm", "value-hm" }});

        m_cacheableDate = (CacheableDate)CacheableDate.Create(DateTime.Now);

        m_cacheableVector = new Apache.Geode.Client.CacheableVector(new List<string>() { "one-vec", "two-vec" });

        m_cacheableObject = Apache.Geode.Client.CacheableObject.Create(new CustomSerializableObject());
      } 
    }

    public Apache.Geode.Client.CacheableBoolean CBool
    {
      get { return m_cacheableBoolean; }
    }

    public Apache.Geode.Client.CacheableInt32 CInt
    {
      get { return m_cacheableInt32; }
    }

    public Apache.Geode.Client.CacheableInt32Array CIntArray
    {
      get { return m_cacheableInt32Array; }
    }

    public Apache.Geode.Client.CacheableFileName CFileName
    {
      get { return m_cacheableFileName; }
    }

    public Apache.Geode.Client.CacheableString CString
    {
      get { return m_CacheableStringASCII; }
    }

    public Apache.Geode.Client.CacheableStringArray CStringArray
    {
      get { return m_cacheableStringArray; }
    }

    public Apache.Geode.Client.CacheableHashSet CHashSet
    {
      get { return m_cacheableHashSet; }
    }

    public Apache.Geode.Client.CacheableHashMap CHashMap
    {
      get { return m_cacheableHashMap; }
    }

    public Apache.Geode.Client.CacheableDate CDate
    {
      get { return m_cacheableDate; }
    }

    public Apache.Geode.Client.CacheableVector CVector
    {
      get { return m_cacheableVector; }
    }

    public Apache.Geode.Client.CacheableObject CObject
    {
      get { return m_cacheableObject; }
    }

    #region IGFSerializable Members

    public uint ClassId
    {
      get { return 0x04; }
    }

    public Apache.Geode.Client.IGFSerializable FromData(Apache.Geode.Client.DataInput input)
    {
      if (!m_initialized)
      {
        m_cacheableBoolean = (Apache.Geode.Client.CacheableBoolean)CacheableBoolean.CreateDeserializable();
        m_cacheableInt32 = (Apache.Geode.Client.CacheableInt32)CacheableInt32.CreateDeserializable();
        m_cacheableInt32Array = (Apache.Geode.Client.CacheableInt32Array)CacheableInt32Array.CreateDeserializable();
       // m_cacheableFileName = (CacheableFileName)CacheableFileName.CreateDeserializable();
        //m_CacheableStringASCII = (CacheableString)CacheableString.CreateDeserializable();
        m_cacheableStringArray = (Apache.Geode.Client.CacheableStringArray)CacheableStringArray.CreateDeserializable();
        m_cacheableHashSet = (Apache.Geode.Client.CacheableHashSet)CacheableHashSet.CreateDeserializable();
        m_cacheableHashMap = (Apache.Geode.Client.CacheableHashMap)CacheableHashMap.CreateDeserializable();
        m_cacheableDate = (CacheableDate)CacheableDate.CreateDeserializable();
        m_cacheableVector = (Apache.Geode.Client.CacheableVector)CacheableVector.CreateDeserializable();
        m_cacheableObject = (Apache.Geode.Client.CacheableObject)CacheableObject.CreateDeserializable();
      }

      m_cacheableBoolean.FromData(input);
      m_cacheableInt32.FromData(input); ;
      m_cacheableInt32Array.FromData(input);
      //m_cacheableFileName.FromData(input);
      //m_CacheableStringASCII.FromData(input);
      m_cacheableStringArray.FromData(input);
      m_cacheableHashSet.FromData(input);
      m_cacheableHashMap.FromData(input);
      m_cacheableDate.FromData(input);
      m_cacheableVector.FromData(input);
      m_cacheableObject.FromData(input);
      return this;
    }

    public uint ObjectSize
    {
      get { return 100; }//need to implement
    }

    public void ToData(Apache.Geode.Client.DataOutput output)
    {
      if (m_initialized)
      {
        m_cacheableBoolean.ToData(output);
        m_cacheableInt32.ToData(output);
        m_cacheableInt32Array.ToData(output);
        //m_cacheableFileName.ToData(output);
        //m_CacheableStringASCII.ToData(output);
        m_cacheableStringArray.ToData(output);
        m_cacheableHashSet.ToData(output);
        m_cacheableHashMap.ToData(output);
        m_cacheableDate.ToData(output);
        m_cacheableVector.ToData(output);
        m_cacheableObject.ToData(output);
      }
    }

    #endregion

    public static Apache.Geode.Client.IGFSerializable CreateDeserializable()
    {
      return new DefaultCacheable();
    }
  }
}
