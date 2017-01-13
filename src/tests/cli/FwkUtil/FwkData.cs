//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Xml;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  [Serializable]
  public enum DataKind
  {
    String,
    List,
    Region,
    Range,
    Pool
  }

  [Serializable]
  public class FwkLocalFile
  {
    private string m_name;
    private bool m_append;
    private string m_description;
    private string m_content;

    #region Public accessors

    public string Name
    {
      get
      {
        return m_name;
      }
    }

    public bool Append
    {
      get
      {
        return m_append;
      }
    }

    public string Description
    {
      get
      {
        return m_description;
      }
    }

    public string Content
    {
      get
      {
        return m_content;
      }
    }

    #endregion

    public FwkLocalFile(string name, bool append, string description, string content)
    {
      m_name = name;
      m_append = append;
      m_description = description;
      m_content = content;
    }

    public static void LoadLocalFileNodes(XmlNode node)
    {
      XmlNodeList xmlNodes = node.SelectNodes("localFile");
      if (xmlNodes != null)
      {
        foreach (XmlNode lf in xmlNodes)
        {
          XmlAttributeCollection attrcoll = lf.Attributes;
          string name = null;
          bool append = false; ;
          string description = null;
          foreach (XmlAttribute attr in attrcoll)
          {
            // Console.WriteLine("attr name " + attr.Name);
            if (attr.Name == "name")
            {
              name = attr.Name;
            }
            else if (attr.Name == "append")
            {
              if (attr.Value == "true")
              {
                append = true;
              }
              else
              {
                append = false;
              }
            }
            else if (attr.Name == "description")
            {
              description = attr.Value;
            }
            else
            {
              throw new IllegalArgException("Local file data format incorrect");
            }
          }
          FwkLocalFile lfobj = new FwkLocalFile(name, append, description, lf.InnerText);
          Util.BBSet(string.Empty, name, lfobj);
        }
      }
    }
  }

  [Serializable]
  public class FwkData 
  {
    #region Private members

    private object m_data1;
    private object m_data2;
    private DataKind m_kind;

    #endregion

    #region Public accessors

    public DataKind Kind
    {
      get
      {
        return m_kind;
      }
    }

    public object Data1
    {
      get
      {
        return m_data1;
      }
      set
      {
        m_data1 = value;
      }
    }
    public object Data2
    {
      get
      {
        return m_data2;
      }
      set
      {
        m_data2 = null;
      }
    }
    #endregion
    
    public FwkData(object data1, object data2, DataKind kind)
    {
      m_data1 = data1;
      m_data2 = data2;
      m_kind = kind;
    }
    
    public static Dictionary<string, FwkData> ReadDataNodes(XmlNode node)
    {
      XmlNodeList xmlNodes = node.SelectNodes("data");
      // Console.WriteLine("Total number of data nodes found = " + xmlNodes.Count);
      if (xmlNodes != null)
      {
        Dictionary<string, FwkData> dataNodes = new Dictionary<string, FwkData>();
        foreach (XmlNode xmlNode in xmlNodes)
        {
          XmlAttribute tmpattr = xmlNode.Attributes["name"];
          string name;
          if (tmpattr != null)
          {
            name = tmpattr.Value;
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknown format");
          }

          //Console.WriteLine("xmlNode.FirstChild.Name = " + xmlNode.FirstChild.Name);
          if (xmlNode.FirstChild == null || xmlNode.FirstChild.NodeType == XmlNodeType.Text)
          {
            object data = xmlNode.InnerText;
          //  Console.WriteLine("Going to construct FwkData with data = " + data.ToString() +
           //   " data2 = null " + " datakind = " + DataKind.String.ToString());
            FwkData td = new FwkData(data, null, DataKind.String);
            dataNodes[name] = td;
          }
          else if (xmlNode.FirstChild.Name == "snippet")
          {
            string regionName;
            if (xmlNode.FirstChild.FirstChild.Name == "region")
            {
              XmlAttribute nameattr = xmlNode.FirstChild.FirstChild.Attributes["name"];
              regionName = nameattr.Value;

              // Now collect the region atributes
              XmlNode attrnode = xmlNode.FirstChild.FirstChild.FirstChild;
              GemStone.GemFire.Cache.Generic.Properties<string, string> rattr = GemStone.GemFire.Cache.Generic.Properties<string, string>.Create<string, string>();
              //AttributesFactory af = new AttributesFactory();
              if (attrnode.Name == "region-attributes")
              {
                XmlAttributeCollection attrcoll = attrnode.Attributes;
                if (attrcoll != null)
                {
                  foreach (XmlAttribute eachattr in attrcoll)
                  {
                    rattr.Insert(eachattr.Name, eachattr.Value);
                    //SetThisAttribute(eachattr.Name, eachattr, af);
                  }
                }
                if (attrnode.ChildNodes != null)
                {
                  foreach (XmlNode tmpnode in attrnode.ChildNodes)
                  {
                    rattr.Insert(tmpnode.Name, tmpnode.Value);
                    //SetThisAttribute(tmpnode.Name, tmpnode, af);
                  }
                }
                GemStone.GemFire.Cache.Generic.DataOutput dout = new GemStone.GemFire.Cache.Generic.DataOutput();
                //RegionAttributes rattr = af.CreateRegionAttributes();
                rattr.ToData(dout);
                // Console.WriteLine("Going to construct FwkData with region = " + regionName +
                // " data2 = region attributes" + " datakind = " + DataKind.Region.ToString());
                FwkData td = new FwkData(regionName, dout.GetBuffer(), DataKind.Region);
                dataNodes[name] = td;
              }
              else
              {
                throw new IllegalArgException("The xml file passed has an unknown format");
              }
            }
            else if (xmlNode.FirstChild.FirstChild.Name == "pool")
            {
              XmlAttribute nameattr = xmlNode.FirstChild.FirstChild.Attributes["name"];
              String poolName = nameattr.Value;
              // Now collect the pool atributes
              GemStone.GemFire.Cache.Generic.Properties<string, string> prop = GemStone.GemFire.Cache.Generic.Properties<string, string>.Create<string, string>();
              XmlAttributeCollection attrcoll = xmlNode.FirstChild.FirstChild.Attributes;
              if (attrcoll != null)
              {
                foreach (XmlAttribute eachattr in attrcoll)
                {
                  prop.Insert(eachattr.Name, eachattr.Value);
                }
                GemStone.GemFire.Cache.Generic.DataOutput dout = new GemStone.GemFire.Cache.Generic.DataOutput();
                prop.ToData(dout);
                FwkData td = new FwkData(poolName, dout.GetBuffer(), DataKind.Pool);
                dataNodes[name] = td;
              }
              else
              {
                throw new IllegalArgException("The xml file passed has an unknown format");
              }
            }
            else
            {
              throw new IllegalArgException("The xml file passed has an unknown format");
            }
          }
          else if (xmlNode.FirstChild.Name == "list")
          {
            List<string> tmplist = new List<string>();
            XmlNode listNode = xmlNode.FirstChild;
            if (listNode.FirstChild != null)
            {
              bool isOneOf = false;
              if (listNode.FirstChild.Name == "oneOf")
              {
                isOneOf = true;
                listNode = listNode.FirstChild;
              }
              XmlNodeList tmpListNodes = listNode.ChildNodes;
              foreach (XmlNode itemnode in tmpListNodes)
              {
                if (itemnode.Name == "item")
                {
                  tmplist.Add(itemnode.InnerText);
                  //Console.WriteLine("Adding the value " + itemnode.InnerText + " to the list");
                }
                else
                {
                  throw new IllegalArgException("The xml file passed has an unknown node " +
                    itemnode.Name + " in data-list");
                }
              }
              //Console.WriteLine("Going to construct FwkData list data: oneof = " + isOneOf.ToString() + " datakind = " + DataKind.List.ToString());
              FwkData td = new FwkData(tmplist, isOneOf, DataKind.List);
              dataNodes[name] = td;
            }
          }
          else if ( xmlNode.FirstChild.Name == "range" )
          {
            XmlAttributeCollection rangeAttr = xmlNode.FirstChild.Attributes;
            string lowmarkstr = rangeAttr["low"].Value;
            string highmarkstr = rangeAttr["high"].Value;
            int lowmark = int.Parse(lowmarkstr);
            int highmark = int.Parse(highmarkstr);
            //Console.WriteLine("Going to construct FwkData Range datakind = " + DataKind.Range.ToString() + " high = " + highmark + " low = " + lowmark);
            FwkData td = new FwkData(lowmark, highmark, DataKind.Range);
            dataNodes[name] = td;
          }
          else if (xmlNode.FirstChild.Name == "oneof")
          {
            XmlNodeList itemlist = xmlNode.FirstChild.ChildNodes;
            List<string> opItemList = new List<string>();
            foreach (XmlNode tmpitem in itemlist)
            {
              if (tmpitem.Name == "item")
              {
                opItemList.Add(tmpitem.InnerText);
              }
            }
            FwkData td = new FwkData(opItemList, true, DataKind.List);
            dataNodes[name] = td;
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknown child: " +
              xmlNode.FirstChild.Name + " ; number of childnodes: " +
              (xmlNode.ChildNodes == null ? XmlNodeType.None :
              xmlNode.FirstChild.NodeType));
          }
        }
        return dataNodes;
      }
      return null;
    }

    public static void SetThisAttribute(string name, XmlNode node, GemStone.GemFire.Cache.Generic.AttributesFactory<string, string> af)
    {
      string value = node.Value;
      switch (name)
      {
        case "caching-enabled":
          if (value == "true")
          {
            af.SetCachingEnabled(true);
          }
          else
          {
            af.SetCachingEnabled(false);
          }
          break;

        case "load-factor":
          float lf = float.Parse(value);
          af.SetLoadFactor(lf);
          break;

        case "concurrency-level":
          int cl = int.Parse(value);
          af.SetConcurrencyLevel(cl);
          break;

        case "lru-entries-limit":
          uint lel = uint.Parse(value);
          af.SetLruEntriesLimit(lel);
          break;

        case "initial-capacity":
          int ic = int.Parse(value);
          af.SetInitialCapacity(ic);
          break;

        case "disk-policy":
          if (value == "none")
          {
              af.SetDiskPolicy(GemStone.GemFire.Cache.Generic.DiskPolicyType.None);
          }
          else if (value == "overflows")
          {
              af.SetDiskPolicy(GemStone.GemFire.Cache.Generic.DiskPolicyType.Overflows);
          }
          else
          {
            throw new IllegalArgException("Unknown disk policy");
          }
          break;
        case "pool-name":
          if (value.Length != 0)
          {
            af.SetPoolName(value);
          }
          else
          {
            af.SetPoolName(value);
          }
          break;

        case "client-notification":
          break;

        case "region-time-to-live":
          XmlNode nlrttl = node.FirstChild;
          if (nlrttl.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nlrttl.Attributes;
            GemStone.GemFire.Cache.Generic.ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string rttl = exAttrColl["timeout"].Value;
            af.SetRegionTimeToLive(action, uint.Parse(rttl));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "region-idle-time":
          XmlNode nlrit = node.FirstChild;
          if (nlrit.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nlrit.Attributes;
            GemStone.GemFire.Cache.Generic.ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string rit = exAttrColl["timeout"].Value;
            af.SetRegionIdleTimeout(action, uint.Parse(rit));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "entry-time-to-live":
          XmlNode nlettl = node.FirstChild;
          if (nlettl.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nlettl.Attributes;
            GemStone.GemFire.Cache.Generic.ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string ettl = exAttrColl["timeout"].Value;
            af.SetEntryTimeToLive(action, uint.Parse(ettl));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "entry-idle-time":
          XmlNode nleit = node.FirstChild;
          if (nleit.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nleit.Attributes;
            GemStone.GemFire.Cache.Generic.ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string eit = exAttrColl["timeout"].Value;
            af.SetEntryIdleTimeout(action, uint.Parse(eit));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "cache-loader":
          XmlAttributeCollection loaderattrs = node.Attributes;
          string loaderlibrary = null;
          string loaderfunction = null;
          foreach(XmlAttribute tmpattr in loaderattrs)
          {
            if (tmpattr.Name == "library")
            {
              loaderlibrary = tmpattr.Value;
            }
            else if (tmpattr.Name == "function")
            {
              loaderfunction = tmpattr.Value;
            }
            else
            {
              throw new IllegalArgException("cahe-loader attributes in improper format");
            }
          }
          if (loaderlibrary != null && loaderfunction != null)
          {
            if (loaderfunction.IndexOf('.') < 0)
            {
              Type myType = typeof(FwkData);
              loaderfunction = myType.Namespace + '.' +
                loaderlibrary + '.' + loaderfunction;
              loaderlibrary = "FwkLib";
            }
            af.SetCacheLoader(loaderlibrary, loaderfunction);
          }
          break;

        case "cache-listener":
          XmlAttributeCollection listenerattrs = node.Attributes;
          string listenerlibrary = null;
          string listenerfunction = null;
          foreach (XmlAttribute tmpattr in listenerattrs)
          {
            if (tmpattr.Name == "library")
            {
              listenerlibrary = tmpattr.Value;
            }
            else if (tmpattr.Name == "function")
            {
              listenerfunction = tmpattr.Value;
            }
            else
            {
              throw new IllegalArgException("cahe-loader attributes in improper format");
            }
          }
          if (listenerlibrary != null && listenerfunction != null)
          {
            if (listenerfunction.IndexOf('.') < 0)
            {
              Type myType = typeof(FwkData);
              listenerfunction = myType.Namespace + '.' +
                listenerlibrary + '.' + listenerfunction;
              listenerlibrary = "FwkLib";
              
            }
            af.SetCacheListener(listenerlibrary, listenerfunction);
          }
          break;

        case "cache-writer":
          XmlAttributeCollection writerattrs = node.Attributes;
          string writerlibrary = null;
          string writerfunction = null;
          foreach (XmlAttribute tmpattr in writerattrs)
          {
            if (tmpattr.Name == "library")
            {
              writerlibrary = tmpattr.Value;
            }
            else if (tmpattr.Name == "function")
            {
              writerfunction = tmpattr.Value;
            }
            else
            {
              throw new IllegalArgException("cahe-loader attributes in improper format");
            }
          }
          if (writerlibrary != null && writerfunction != null)
          {
            if (writerfunction.IndexOf('.') < 0)
            {
              Type myType = typeof(FwkData);
              writerfunction = myType.Namespace + '.' +
                writerlibrary + '.' + writerfunction;
              writerlibrary = "FwkLib";
            }
            af.SetCacheWriter(writerlibrary, writerfunction);
          }
          break;

        case "persistence-manager":
          string pmlibrary = null;
          string pmfunction = null;
          GemStone.GemFire.Cache.Generic.Properties<string, string> prop = new GemStone.GemFire.Cache.Generic.Properties<string, string>();
          XmlAttributeCollection pmattrs = node.Attributes;
          foreach (XmlAttribute attr in pmattrs)
          {
            if (attr.Name == "library")
            {
              pmlibrary = attr.Value;
            }
            else if (attr.Name == "function")
            {
              pmfunction = attr.Value;
            }
            else
            {
              throw new IllegalArgException("Persistence Manager attributes in wrong format: " + attr.Name);
            }
          }

          if (node.FirstChild.Name == "properties")
          {
            XmlNodeList pmpropnodes = node.FirstChild.ChildNodes;
            foreach (XmlNode propnode in pmpropnodes)
            {
              if (propnode.Name == "property")
              {
                XmlAttributeCollection keyval = propnode.Attributes;
                XmlAttribute keynode = keyval["name"];
                XmlAttribute valnode = keyval["value"];

                if (keynode.Value == "PersistenceDirectory" || keynode.Value == "EnvironmentDirectory")
                {
                  prop.Insert(keynode.Value, valnode.Value);
                }
                else if (keynode.Value == "CacheSizeGb" || keynode.Value == "CacheSizeMb"
                         || keynode.Value == "PageSize" || keynode.Value == "MaxFileSize")
                {
                  prop.Insert(keynode.Value, valnode.Value);
                }
              }
            }
          }
          af.SetPersistenceManager(pmlibrary, pmfunction, prop);
          break;
      }
    }

    private static GemStone.GemFire.Cache.Generic.ExpirationAction StrToExpirationAction(string str)
    {
        return (GemStone.GemFire.Cache.Generic.ExpirationAction)Enum.Parse(typeof(GemStone.GemFire.Cache.Generic.ExpirationAction),
        str.Replace("-", string.Empty), true);
    }
  }

  /// <summary>
  /// Reader class for <see cref="FwkData"/>
  /// </summary>
  public class FwkReadData : MarshalByRefObject
  {
    #region Private members

    private Dictionary<string, FwkData> m_dataMap =
      new Dictionary<string, FwkData>();
    private Dictionary<string, int> m_dataIndexMap =
      new Dictionary<string, int>();
    private Stack<string> m_taskNames =
      new Stack<string>();

    #endregion

    #region Private methods

    private FwkData ReadData(string key)
    {
      FwkData data;
      lock (((ICollection)m_dataMap).SyncRoot)
      {
        if (m_dataMap.ContainsKey(key))
        {
          data = m_dataMap[key];
        }
        else
        {
          try
          {
            // First search the task specific data (which overrides the global
            // data) and then search in global data for the key.
            data = Util.BBGet(TaskName, key) as FwkData;
          }
          catch (KeyNotFoundException)
          {
            data = null;
          }
          if (data == null)
          {
            try
            {
              data = Util.BBGet(string.Empty, key) as FwkData;
            }
            catch (KeyNotFoundException)
            {
              data = null;
            }
          }
          m_dataMap[key] = data;
        }
      }
      return data;
    }

    #endregion

    #region Public accessors and constants

    public const string TestRunNumKey = "TestRunNum";
    public const string HostGroupKey = "HostGroup";

    public string TaskName
    {
      get
      {
        return (m_taskNames.Count > 0 ? m_taskNames.Peek() : null);
      }
    }

    #endregion

    #region Public methods

    public void PushTaskName(string taskName)
    {
      m_taskNames.Push(taskName);
      Util.Log("FWKLIB:: Setting the taskname to [{0}]", taskName);
    }

    public void PopTaskName()
    {
      Util.Log("FWKLIB:: Removing taskname [{0}]", TaskName);
      m_taskNames.Pop();
      string oldTaskName = TaskName;
      if (oldTaskName != null)
      {
        Util.Log("FWKLIB:: Setting the taskname back to [{0}]", oldTaskName);
      }
    }

    /// <summary>
    /// Read the value of an object as a string from the server
    /// for the given key.
    /// </summary>
    /// <remarks>
    /// For the case when the key corresponds to a list, this function
    /// gives a random value if the list has 'oneOf' attribute,
    /// else gives the values from the list in sequential order.
    /// </remarks>
    /// <param name="key">The key of the string to read.</param>
    /// <returns>
    /// The string value with the given key; null if not found.
    /// </returns>
    public string GetStringValue(string key)
    {
      FwkData data = ReadData(key);
      string res = null;
      if (data != null)
      {
        if (data.Kind == DataKind.String)
        {
          lock (((ICollection)m_dataIndexMap).SyncRoot)
          {
            int currentIndex;
            if (!m_dataIndexMap.TryGetValue(key, out currentIndex))
            {
              currentIndex = 0;
            }
            if (currentIndex == 0)
            {
              res = data.Data1 as string;
              m_dataIndexMap[key] = 1;
            }
          }
        }
        else if (data.Kind == DataKind.List)
        {
          int len;
          List<string> dataList = data.Data1 as List<string>;
          if (dataList != null && (len = dataList.Count) > 0)
          {
            bool oneOf = false;
            if (data.Data2 != null && data.Data2 is bool)
            {
              oneOf = (bool)data.Data2;
            }
            if (oneOf)
            {
              res = dataList[Util.Rand(len)];
            }
            else
            {
              lock (((ICollection)m_dataIndexMap).SyncRoot)
              {
                int currentIndex;
                if (!m_dataIndexMap.TryGetValue(key, out currentIndex))
                {
                  currentIndex = 0;
                }
                if (currentIndex < len)
                {
                  res = dataList[currentIndex];
                  m_dataIndexMap[key] = currentIndex + 1;
                }
              }
            }
          }
        }
        // TODO: Deal with Range here.
      }
      return res;
    }

    /// <summary>
    /// Read the value of an object as an integer from the server
    /// for the given key.
    /// </summary>
    /// <remarks>
    /// The value is assumed to be an unsigned integer.
    /// For the case when the key corresponds to a list, this function
    /// gives a random value if the list has 'oneOf' attribute,
    /// else gives the values from the list in sequential order.
    /// </remarks>
    /// <param name="key">The key of the string to read.</param>
    /// <returns>
    /// The integer value with the given key; -1 if not found.
    /// </returns>
    public int GetUIntValue(string key)
    {
      string str = GetStringValue(key);
      if (str == null)
      {
        return -1;
      }
      try
      {
        return int.Parse(str);
      }
      catch
      {
        return -1;
      }
    }

    /// <summary>
    /// Read the value of an object as time in seconds from the server
    /// for the given key.
    /// </summary>
    /// <remarks>
    /// If the value contains 'h' or 'm' then it is assumed to be in
    /// hours and minutes respectively.
    /// </remarks>
    /// <param name="key">The key of the value to read.</param>
    /// <returns>
    /// The integer value with the given key; -1 if not found.
    /// </returns>
    public int GetTimeValue(string key)
    {
      return XmlNodeReaderWriter.String2Seconds(GetStringValue(key), -1);
    }

    /// <summary>
    /// Read the value of an object as a boolean value for the given key.
    /// </summary>
    /// <remarks>
    /// If the key is not defined the default value returned is false.
    /// </remarks>
    /// <param name="key">The key of the boolean to read.</param>
    /// <returns>
    /// The boolean value with the given key;
    /// false if not found or in incorrect format.
    /// </returns>
    public bool GetBoolValue(string key)
    {
      string str = GetStringValue(key);
      if (str == null) return false;
      try
      {
        return bool.Parse(str);
      }
      catch
      {
      }
      return false;
    }

    /// <summary>
    /// Read the name of the region for the given key.
    /// </summary>
    /// <param name="key">The key of the region to read.</param>
    /// <returns>The name of the region.</returns>
    public string GetRegionName(string key)
    {
      FwkData data = ReadData(key);
      if (data != null && data.Kind == DataKind.Region)
      {
        return data.Data1 as string;
      }
      return null;
    }

    /// <summary>
    /// Read the region attributes for the given key.
    /// </summary>
    /// <param name="key">The key of the region to read.</param>
    /// <returns>The attributes of the region.</returns>
    public GemStone.GemFire.Cache.Generic.RegionAttributes<string, string> GetRegionAttributes(string key)
    {
      FwkData data = ReadData(key);
      if (data != null && data.Kind == DataKind.Region)
      {
        GemStone.GemFire.Cache.Generic.AttributesFactory<string, string> af = new GemStone.GemFire.Cache.Generic.AttributesFactory<string, string>();
        GemStone.GemFire.Cache.Generic.RegionAttributes<string, string> attrs = af.CreateRegionAttributes();
        byte[] attrsArr = data.Data2 as byte[];
        if (attrsArr != null && attrsArr.Length > 0)
        {
          GemStone.GemFire.Cache.Generic.DataInput dinp = new GemStone.GemFire.Cache.Generic.DataInput(attrsArr);
          attrs.FromData(dinp);
        }
        return attrs;
      }
      return null;
    }
    public GemStone.GemFire.Cache.Generic.Properties<string, string> GetPoolAttributes(string key)
    {
      FwkData data = ReadData(key);
      if (data != null && data.Kind == DataKind.Pool)
      {
        GemStone.GemFire.Cache.Generic.Properties<string, string> prop = GemStone.GemFire.Cache.Generic.Properties<string, string>.Create<string, string>();
        //RegionAttributes attrs = af.CreateRegionAttributes();
        byte[] attrsArr = data.Data2 as byte[];
        if (attrsArr != null && attrsArr.Length > 0)
        {
          GemStone.GemFire.Cache.Generic.DataInput dinp = new GemStone.GemFire.Cache.Generic.DataInput(attrsArr);
          prop.FromData(dinp);
        }
        return prop;
      }
      return null;
    }

    /// <summary>
    /// Reset a key to the start.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    public void ResetKey(string key)
    {
      lock (((ICollection)m_dataIndexMap).SyncRoot)
      {
        if (m_dataIndexMap.ContainsKey(key))
        {
          m_dataIndexMap.Remove(key);
        }
      }
    }

    /// <summary>
    /// Clear all the keys from the local map.
    /// </summary>
    public void ClearCachedKeys()
    {
      lock (((ICollection)m_dataMap).SyncRoot)
      {
        m_dataMap.Clear();
      }
      lock (((ICollection)m_dataIndexMap).SyncRoot)
      {
        m_dataIndexMap.Clear();
      }
    }

    #endregion
  }
}
