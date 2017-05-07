using System;
using System.Collections.Generic;
using System.Text;

namespace GemStone.GemFire.Cache.UnitTests
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
  class DefaultCacheable : IGFSerializable
  {
    CacheableBoolean m_cacheableBoolean = null;
    CacheableInt32 m_cacheableInt32 = null;
    CacheableInt32Array m_cacheableInt32Array = null;
    CacheableFileName m_cacheableFileName = null;
    CacheableString m_CacheableStringASCII = null;
    CacheableStringArray m_cacheableStringArray = null;
    CacheableHashSet m_cacheableHashSet = null;
    CacheableHashMap m_cacheableHashMap = null;
    CacheableDate m_cacheableDate = null;
    CacheableVector m_cacheableVector = null;
    CacheableObject m_cacheableObject = null;

    bool m_initialized = false;

    public DefaultCacheable()
    { 
    
    }

    public DefaultCacheable(bool initialized)
    {
      if (initialized)
      {
        Log.Fine("hitesh in constructor");
        m_initialized = true;
        
        m_cacheableBoolean = CacheableBoolean.Create(true);
        
        m_cacheableInt32 = CacheableInt32.Create(1000);
        
        m_cacheableInt32Array = CacheableInt32Array.Create(new Int32[]{1,2,3});
        
        m_cacheableFileName = CacheableFileName.Create("gemstone.txt");
        
        m_CacheableStringASCII = CacheableString.Create("asciistring");
        
        m_cacheableStringArray = CacheableStringArray.Create(new string[] { "one", "two" });
        
        m_cacheableHashSet = CacheableHashSet.Create(2);
        m_cacheableHashSet.Add(CacheableString.Create("first"));
        m_cacheableHashSet.Add(CacheableString.Create("second"));
        
        m_cacheableHashMap = CacheableHashMap.Create();
        m_cacheableHashMap[new CacheableString("key-hm")] = new CacheableString("value-hm");
        
        m_cacheableDate = (CacheableDate)CacheableDate.Create(DateTime.Now);

        m_cacheableVector = CacheableVector.Create();
        m_cacheableVector.Add(new CacheableString("one-vec"));
        m_cacheableVector.Add(new CacheableString("two-vec"));

        m_cacheableObject = CacheableObject.Create(new CustomSerializableObject());
      } 
    }

    public CacheableBoolean CBool
    {
      get { return m_cacheableBoolean; }
    }

    public CacheableInt32 CInt
    {
      get { return m_cacheableInt32; }
    }

    public CacheableInt32Array CIntArray
    {
      get { return m_cacheableInt32Array; }
    }

    public CacheableFileName CFileName
    {
      get { return m_cacheableFileName; }
    }

    public CacheableString CString
    {
      get { return m_CacheableStringASCII; }
    }

    public CacheableStringArray CStringArray
    {
      get { return m_cacheableStringArray; }
    }

    public CacheableHashSet CHashSet
    {
      get { return m_cacheableHashSet; }
    }

    public CacheableHashMap CHashMap
    {
      get { return m_cacheableHashMap; }
    }

    public CacheableDate CDate
    {
      get { return m_cacheableDate; }
    }

    public CacheableVector CVector
    {
      get { return m_cacheableVector; }
    }

    public CacheableObject CObject
    {
      get { return m_cacheableObject; }
    }

    #region IGFSerializable Members

    public uint ClassId
    {
      get { return 0x04; }
    }

    public IGFSerializable FromData(DataInput input)
    {
      if (!m_initialized)
      {
        m_cacheableBoolean = (CacheableBoolean)CacheableBoolean.CreateDeserializable();
        m_cacheableInt32 = (CacheableInt32)CacheableInt32.CreateDeserializable();
        m_cacheableInt32Array = (CacheableInt32Array)CacheableInt32Array.CreateDeserializable();
       // m_cacheableFileName = (CacheableFileName)CacheableFileName.CreateDeserializable();
        //m_CacheableStringASCII = (CacheableString)CacheableString.CreateDeserializable();
        m_cacheableStringArray = (CacheableStringArray)CacheableStringArray.CreateDeserializable();
        m_cacheableHashSet = (CacheableHashSet)CacheableHashSet.CreateDeserializable();
        m_cacheableHashMap = (CacheableHashMap)CacheableHashMap.CreateDeserializable();
        m_cacheableDate = (CacheableDate)CacheableDate.CreateDeserializable();
        m_cacheableVector = (CacheableVector)CacheableVector.CreateDeserializable();
        m_cacheableObject = (CacheableObject)CacheableObject.CreateDeserializable();
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

    public void ToData(DataOutput output)
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

    public static IGFSerializable CreateDeserializable()
    {
      return new DefaultCacheable();
    }
  }
}
