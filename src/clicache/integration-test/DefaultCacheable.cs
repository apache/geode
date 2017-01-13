using System;
using System.Collections.Generic;
using System.Text;

using GemStone.GemFire.Cache.Generic;

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
  class DefaultCacheable : GemStone.GemFire.Cache.Generic.IGFSerializable
  {
    GemStone.GemFire.Cache.Generic.CacheableBoolean m_cacheableBoolean = null;
    GemStone.GemFire.Cache.Generic.CacheableInt32 m_cacheableInt32 = null;
    GemStone.GemFire.Cache.Generic.CacheableInt32Array m_cacheableInt32Array = null;
    GemStone.GemFire.Cache.Generic.CacheableFileName m_cacheableFileName = null;
    GemStone.GemFire.Cache.Generic.CacheableString m_CacheableStringASCII = null;
    GemStone.GemFire.Cache.Generic.CacheableStringArray m_cacheableStringArray = null;
    GemStone.GemFire.Cache.Generic.CacheableHashSet m_cacheableHashSet = null;
    GemStone.GemFire.Cache.Generic.CacheableHashMap m_cacheableHashMap = null;
    GemStone.GemFire.Cache.Generic.CacheableDate m_cacheableDate = null;
    GemStone.GemFire.Cache.Generic.CacheableVector m_cacheableVector = null;
    GemStone.GemFire.Cache.Generic.CacheableObject m_cacheableObject = null;

    bool m_initialized = false;

    public DefaultCacheable()
    { 
    
    }

    public DefaultCacheable(bool initialized)
    {
      if (initialized)
      {
        GemStone.GemFire.Cache.Generic.Log.Fine(" in constructor");
        m_initialized = true;

        m_cacheableBoolean = GemStone.GemFire.Cache.Generic.CacheableBoolean.Create(true);

        m_cacheableInt32 = GemStone.GemFire.Cache.Generic.CacheableInt32.Create(1000);

        m_cacheableInt32Array = GemStone.GemFire.Cache.Generic.CacheableInt32Array.Create(new Int32[] { 1, 2, 3 });

        m_cacheableFileName = GemStone.GemFire.Cache.Generic.CacheableFileName.Create("gemstone.txt");

        m_CacheableStringASCII = GemStone.GemFire.Cache.Generic.CacheableString.Create("asciistring");

        m_cacheableStringArray = GemStone.GemFire.Cache.Generic.CacheableStringArray.Create(new string[] { "one", "two" });

        m_cacheableHashSet = GemStone.GemFire.Cache.Generic.CacheableHashSet.Create(2);
        m_cacheableHashSet.Add(GemStone.GemFire.Cache.Generic.CacheableString.Create("first"));
        m_cacheableHashSet.Add(GemStone.GemFire.Cache.Generic.CacheableString.Create("second"));

        m_cacheableHashMap = new GemStone.GemFire.Cache.Generic.CacheableHashMap(new Dictionary<string, string>() {{ "key-hm", "value-hm" }});

        m_cacheableDate = (CacheableDate)CacheableDate.Create(DateTime.Now);

        m_cacheableVector = new GemStone.GemFire.Cache.Generic.CacheableVector(new List<string>() { "one-vec", "two-vec" });

        m_cacheableObject = GemStone.GemFire.Cache.Generic.CacheableObject.Create(new CustomSerializableObject());
      } 
    }

    public GemStone.GemFire.Cache.Generic.CacheableBoolean CBool
    {
      get { return m_cacheableBoolean; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableInt32 CInt
    {
      get { return m_cacheableInt32; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableInt32Array CIntArray
    {
      get { return m_cacheableInt32Array; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableFileName CFileName
    {
      get { return m_cacheableFileName; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableString CString
    {
      get { return m_CacheableStringASCII; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableStringArray CStringArray
    {
      get { return m_cacheableStringArray; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableHashSet CHashSet
    {
      get { return m_cacheableHashSet; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableHashMap CHashMap
    {
      get { return m_cacheableHashMap; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableDate CDate
    {
      get { return m_cacheableDate; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableVector CVector
    {
      get { return m_cacheableVector; }
    }

    public GemStone.GemFire.Cache.Generic.CacheableObject CObject
    {
      get { return m_cacheableObject; }
    }

    #region IGFSerializable Members

    public uint ClassId
    {
      get { return 0x04; }
    }

    public GemStone.GemFire.Cache.Generic.IGFSerializable FromData(GemStone.GemFire.Cache.Generic.DataInput input)
    {
      if (!m_initialized)
      {
        m_cacheableBoolean = (GemStone.GemFire.Cache.Generic.CacheableBoolean)CacheableBoolean.CreateDeserializable();
        m_cacheableInt32 = (GemStone.GemFire.Cache.Generic.CacheableInt32)CacheableInt32.CreateDeserializable();
        m_cacheableInt32Array = (GemStone.GemFire.Cache.Generic.CacheableInt32Array)CacheableInt32Array.CreateDeserializable();
       // m_cacheableFileName = (CacheableFileName)CacheableFileName.CreateDeserializable();
        //m_CacheableStringASCII = (CacheableString)CacheableString.CreateDeserializable();
        m_cacheableStringArray = (GemStone.GemFire.Cache.Generic.CacheableStringArray)CacheableStringArray.CreateDeserializable();
        m_cacheableHashSet = (GemStone.GemFire.Cache.Generic.CacheableHashSet)CacheableHashSet.CreateDeserializable();
        m_cacheableHashMap = (GemStone.GemFire.Cache.Generic.CacheableHashMap)CacheableHashMap.CreateDeserializable();
        m_cacheableDate = (CacheableDate)CacheableDate.CreateDeserializable();
        m_cacheableVector = (GemStone.GemFire.Cache.Generic.CacheableVector)CacheableVector.CreateDeserializable();
        m_cacheableObject = (GemStone.GemFire.Cache.Generic.CacheableObject)CacheableObject.CreateDeserializable();
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

    public void ToData(GemStone.GemFire.Cache.Generic.DataOutput output)
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

    public static GemStone.GemFire.Cache.Generic.IGFSerializable CreateDeserializable()
    {
      return new DefaultCacheable();
    }
  }
}
