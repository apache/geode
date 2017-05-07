using System;
using System.Collections.Generic;
using System.Text;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;

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

  // VJR: TODO: IGFSerializable should be replaced by IPdxSerializable when ready
  class DefaultType : IGFSerializable
  {
    bool m_cacheableBoolean;
    int m_cacheableInt32;
    int[] m_cacheableInt32Array = null;
    string m_cacheableFileName = null;
    string m_CacheableStringASCII = null;
    string[] m_cacheableStringArray = null;
    //CacheableHashSet m_cacheableHashSet = null;
    Dictionary<Object, Object> m_cacheableHashMap;
    //DateTime m_cacheableDate = null;
    IList<object> m_cacheableVector = null;
    object m_cacheableObject = null;

    bool m_initialized = false;

    public DefaultType()
    { 
    
    }

    public DefaultType(bool initialized)
    {
      if (initialized)
      {
        Log.Fine("DefaultType in constructor");
        m_initialized = true;
        
        m_cacheableBoolean = true;
        
        m_cacheableInt32 = 1000;
        
        m_cacheableInt32Array =new int[]{1,2,3};
        
        m_cacheableFileName = "gemstone.txt";
        
        m_CacheableStringASCII = "asciistring";
        
        m_cacheableStringArray = new string[] { "one", "two" };
        
        /*
        m_cacheableHashSet = CacheableHashSet.Create(2);
        m_cacheableHashSet.Add(CacheableString.Create("first"));
        m_cacheableHashSet.Add(CacheableString.Create("second"));
         * */
        
        m_cacheableHashMap = new Dictionary<Object, Object>();
        m_cacheableHashMap.Add("key-hm", "value-hm");
        
        //m_cacheableDate = DateTime.Now;

        m_cacheableVector = new List<object>();
        m_cacheableVector.Add("one-vec");
        m_cacheableVector.Add("two-vec");

        //m_cacheableObject = new CustomSerializableObject();
      } 
    }

    public bool CBool
    {
      get { return m_cacheableBoolean; }
    }

    public int CInt
    {
      get { return m_cacheableInt32; }
    }

    public int[] CIntArray
    {
      get { return m_cacheableInt32Array; }
    }

    public string CFileName
    {
      get { return m_cacheableFileName; }
    }

    public string CString
    {
      get { return m_CacheableStringASCII; }
    }

    public string[] CStringArray
    {
      get { return m_cacheableStringArray; }
    }

    /*
    public CacheableHashSet CHashSet
    {
      get { return m_cacheableHashSet; }
    }
     * */

    public IDictionary<object, object> CHashMap
    {
      get { return m_cacheableHashMap; }
    }

    /*
    public DateTime CDate
    {
      get { return m_cacheableDate; }
    }
     * */

    public IList<object> CVector
    {
      get { return m_cacheableVector; }
    }

    public object CObject
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
        /*
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
         * */
      }

      m_cacheableBoolean = input.ReadBoolean();
      m_cacheableInt32 = input.ReadInt32();
      int arraylen = input.ReadArrayLen();
      m_cacheableInt32Array = new int[arraylen];
      for (int item = 0; item < arraylen; item++)
      {
        m_cacheableInt32Array[item] = input.ReadInt32();
      }
      //m_cacheableFileName.FromData(input);
      //m_CacheableStringASCII.FromData(input);
      m_cacheableFileName = input.ReadUTF();
      m_CacheableStringASCII = input.ReadUTF();
      arraylen = input.ReadArrayLen();
      m_cacheableStringArray = new string[arraylen];
      for (int item = 0; item < arraylen; item++)
      {
        m_cacheableStringArray[item] = input.ReadUTF();
      }
      //m_cacheableHashSet.FromData(input);
      m_cacheableHashMap = new Dictionary<Object, Object>();
      input.ReadDictionary((System.Collections.IDictionary)m_cacheableHashMap);
      //m_cacheableHashMap = input.ReadDictionary();
      //m_cacheableDate = input.ReadDate();
      arraylen = input.ReadArrayLen();
      m_cacheableVector = new object[arraylen];
      for (int item = 0; item < arraylen; item++)
      {
        m_cacheableVector[item] = input.ReadObject();
      }
      //m_cacheableObject = input.ReadObject();
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
        output.WriteBoolean(m_cacheableBoolean);
        output.WriteInt32(m_cacheableInt32);
        output.WriteArrayLen(m_cacheableInt32Array.Length);
        foreach (int item in m_cacheableInt32Array)
        {
          output.WriteInt32(item);
        }
        //m_cacheableFileName.ToData(output);
        //m_CacheableStringASCII.ToData(output);
        output.WriteUTF(m_cacheableFileName);
        output.WriteUTF(m_CacheableStringASCII);
        output.WriteArrayLen(m_cacheableStringArray.Length);
        foreach (string item in m_cacheableStringArray)
        {
          output.WriteUTF(item);
        }
        //m_cacheableHashSet.ToData(output);
        output.WriteDictionary((System.Collections.IDictionary)m_cacheableHashMap);
        //output.WriteDate(m_cacheableDate);
        output.WriteArrayLen(m_cacheableVector.Count);
        foreach (object item in m_cacheableVector)
        {
          output.WriteObject(item);
        }
        //output.WriteObject(m_cacheableObject);
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new DefaultType();
    }
  }
}
