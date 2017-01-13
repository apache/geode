//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Examples
{
  using GemStone.GemFire.Cache.Tests;
  using GemStone.GemFire.Cache;

  // Example class to perform puts/gets using one or more clients.
  class CacheRunner
  {
    #region Local constants

    private static string DefaultXMLPath = "tcr_cache.xml";

    #endregion

    #region Private static members

    private static Cache m_cache = null;
    private static Region m_region = null;

    #endregion

    static void Main(string[] args)
    {
      try
      {
        // Connect to distributed system and cache 
        DSInit(args);

        // Enter puts and gets manually
        DoCommand();
      }
      catch (Exception ex)
      {
        Console.WriteLine("{0}An exception occurred: {1}",
          Environment.NewLine, ex.Message);
        Console.WriteLine("---[ Press <Enter> to End the Application ]---");
        Console.ReadLine();
      }
      finally
      {
        try
        {
          // Close the cache and disconnect
          DSClose();
        }
        catch // Ignore any exception here.
        {
        }
      }
    }

    #region Local functions

    // Initialize the distributed system, setup the cache, register callbacks
    // and user-defined classes and create the region.
    public static void DSInit(string[] args)
    {
      Console.WriteLine("{0}Connecting to GemFire{0}", Environment.NewLine);

      // Register the ComplexNumber type
      Serializable.RegisterType(ComplexNumber.Create);

      // Register the Portfolio and Position classes for Query
      Serializable.RegisterType(Portfolio.CreateDeserializable);
      Serializable.RegisterType(Position.CreateDeserializable);

      // Register the ExampleObject for putall and interop 
      Serializable.RegisterType(ExampleObject.CreateDeserializable);

      string xmlPath;
      // Create a cache
      if (args != null && args.Length > 1)
      {
        throw new ApplicationException(string.Format("Usage: CacheRunner " +
          "[<XML file name>]{0}\tXML file name is optional " +
          "and the default is {1}.", Environment.NewLine, DefaultXMLPath));
      }
      else if (args != null && args.Length == 1)
      {
        xmlPath = args[0];
      }
      else
      {
        xmlPath = DefaultXMLPath;
      }
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(null);
      m_cache = cacheFactory.Set("cache-xml-file", xmlPath).Create();
      Region[] rootRegions = m_cache.RootRegions();
      if (rootRegions != null && rootRegions.Length > 0)
      {
        SetRegion(rootRegions[rootRegions.Length - 1]);
      }
      else
      {
        throw new ApplicationException("No root regions found.");
      }
    }

    // Close the cache and disconnect from the distributed system.
    public static void DSClose()
    {
      // Close cache
      if (m_cache != null)
      {
        m_cache.Close();
        m_cache = null;
        
        Console.WriteLine("{0}Closed cache", Environment.NewLine);
      }
      Console.WriteLine("{0}Connection closed", Environment.NewLine);
    }

    private static void SetRegion(Region region)
    {
      m_region = region;
      RegionAttributes attrs = m_region.Attributes;
      if (attrs.ClientNotificationEnabled)
      {
        m_region.RegisterAllKeys();
      }
      // Add cache listener callback to region
      if (attrs.CacheListener == null)
      {
        ExampleCacheListenerCallback listenerCallback =
          new ExampleCacheListenerCallback();
        AttributesMutator mutator = m_region.GetAttributesMutator();
        mutator.SetCacheListener(listenerCallback);
      }
    }

    // Puts a string key and complex number value
    public static void PutComplex(string key, ComplexNumber value)
    {
      m_region.Put(key, value);
      Console.WriteLine("Put complex -- key: " + key + " value: " + value);
    }

    // Puts a string key and string value
    public static void PutStr(string key, string value)
    {
      m_region.Put(key, value);
      Console.WriteLine("Put string  -- key: " + key + " value: " + value);
    }

    // Puts a string key and bytes value
    public static void PutBytes(string key, string value)
    {
      byte[] buffer = new byte[value.Length];
      for (int index = 0; index < value.Length; index++)
      {
        buffer[index] = (byte)value[index];
      }
      m_region.Put(key, buffer);
      Console.WriteLine("Put string  -- key: " + key + " value: " + value);
    }

    // Puts a string key and object
    public static void PutObject(string key, IGFSerializable cValue)
    {
      m_region.Put(key, cValue);
      Console.WriteLine("Put  -- key: " + key + " value: " + cValue);
    }

    // Puts a string key and portfolio object
    public static void PutPortfolio(string key, int id)
    {
      Portfolio portfolio = new Portfolio(id, 2);
      m_region.Put(key, portfolio);
      Console.WriteLine("Put portfolio  -- key: " + key + " id: " + id);
    }

    // Puts a string key and position object
    public static void PutPosition(string key, int id)
    {
      Position position = new Position(key, id);
      m_region.Put(key, position);
      Console.WriteLine("Put position  -- key: " + key + " id: " + id);
    }

    // Gets the value, if the key exists
    public static string GetStr(string key)
    {
      string testStr = string.Empty;

      IGFSerializable cValue = m_region.Get(key);
      // Is the key found?
      if (cValue != null)
      {
        testStr = cValue.ToString();

        // Type of value?
        if (cValue is CacheableBytes)
        {
          System.Text.StringBuilder sb = new System.Text.StringBuilder();
          CacheableBytes cBytes = cValue as CacheableBytes;
          foreach (byte b in cBytes.Value)
          {
            sb.Append((char)b);
          }
          testStr = sb.ToString();
        }
        else if (cValue is CacheableObjectXml)
        {
          object val = ((CacheableObjectXml)cValue).Value;
          System.Xml.XmlDocument xd = val as System.Xml.XmlDocument;
          if (xd != null)
          {
            testStr = xd.OuterXml;
          }
        }
        Console.WriteLine("Get [{0}] -- key: {1}, value: {2}",
          cValue.GetType().Name, key, testStr);
      }
      else
      {
        testStr = "NULL";
        Console.WriteLine("No such key in region: " + key);
      }

      return testStr;
    }

    // Run the given query
    public static void RunQuery(string query, bool isRegionQuery)
    {
      try
      {
        ISelectResults results = null;

        if (isRegionQuery)
        {
          results = m_region.Query(query);
        }
        else
        {
          QueryService qs = m_cache.GetQueryService("examplePool");
          Query qry = qs.NewQuery(query);
          results = qry.Execute();
        }
        if (results is ResultSet)
        {
          uint index = 1;
          foreach (IGFSerializable result in results)
          {
            Console.WriteLine("\tResult {0}: {1}", index, result);
            index++;
          }
        }
        else
        {
          StructSet ss = (StructSet)results;
          Console.Write("Columns:");
          uint index = 0;
          string colName;
          while ((colName = ss.GetFieldName(index)) != null)
          {
            Console.Write('\t' + colName);
            index++;
          }
          Console.WriteLine();
          index = 1;
          foreach (Struct si in results)
          {
            Console.Write("\tResult {0}: ");
            while (si.HasNext())
            {
              Console.Write(si.Next() + " || ");
            }
            Console.WriteLine();
            index++;
          }
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine("Exception while running the query [{0}]: {1}",
          query, ex.Message);
      }
    }

    // Run the given query and display the single result value
    public static void SelectValue(string query)
    {
      try
      {
        IGFSerializable result = m_region.SelectValue(query);

        if (result is Struct)
        {
          Struct si = result as Struct;
          Console.Write("Columns:");
          uint index = 0;
          string colName;
          while ((colName = si.Set.GetFieldName(index)) != null)
          {
            Console.Write('\t' + colName);
            index++;
          }
          Console.WriteLine();

          index = 0;
          Console.Write("\tResult {0}: ");
          while (si.HasNext())
          {
            Console.Write(si.Next() + " || ");
          }
          Console.WriteLine();
          index++;
        }
        else
        {
          Console.WriteLine("\tResult : {0}", result);
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine("Exception while running the query [{0}]: {1}",
          query, ex.Message);
      }
    }

    private static int getTS(DateTime ts)
    {
      int ms = (ts.Hour * 3600 + ts.Minute * 60 + ts.Second) * 1000 + ts.Millisecond;
      return ms;
    }

    // Run the given query and display if value exists
    public static void ExistsValue(string query)
    {
      try
      {
        bool result = m_region.ExistsValue(query);

        Console.WriteLine("\tResult is {0}", result);
      }
      catch (Exception ex)
      {
        Console.WriteLine("Exception while running the query [{0}]: {1}",
          query, ex.Message);
      }
    }

    // Waits for input of a command (chrgn, get, put, putAll, exec, query,
    // existsvalue, selectvalue or quit), then does just that.
    public static void DoCommand()
    {
      string myCmd = string.Empty;
      string myKey;
      string myValue;

      while (myCmd != "quit")
      {
        Console.Write("{0}: chrgn, lsrgn, get, put, putAll, run, exec, query, " +
          "existsvalue, selectvalue, quit: ", m_region.FullPath);

        string strIn = Console.ReadLine().Trim();
        string[] strSplit = strIn.Split(' ');

        myCmd = strSplit[0];
        myCmd = myCmd.ToLower();

        if (myCmd == "q") { myCmd = "quit"; }

        switch (myCmd)
        {
          case "chrgn":
            if (strSplit.Length == 2)
            {
              string regionPath = strSplit[1].Trim();
              Region region = null;
              try
              {
                region = m_cache.GetRegion(regionPath);
              }
              catch
              {
              }
              if (region == null)
              {
                Console.WriteLine("No region {0} found.", regionPath);
                Console.WriteLine("usage: chrgn region-path");
              }
              else
              {
                SetRegion(region);
              }
            }
            else
            {
              Console.WriteLine("usage: chrgn region-path \t change the " +
                "current region to the given region; the region-path can " +
                "be absolute or relative to the current region");
            }
            break;

          case "lsrgn":
            Region[] rootRegions = m_cache.RootRegions();
            Console.Write("\nNumber of regions in Cache: {0}\n", rootRegions.Length);
            int count = 1;
            for (int rgnCnt = 0; rgnCnt < rootRegions.Length; rgnCnt++)
            {
              Console.Write("Region Name {0}: {1}\n",count++,rootRegions[rgnCnt].Name);
            }
            break;

          case "run":
            if (strSplit.Length == 3)
            {
              string myNumber = strSplit[1];
              string mySize = strSplit[2];
              int number = int.Parse(myNumber);
              int size = int.Parse(mySize);
              byte[] payload = new byte[size];

              CacheableHashMap map = new CacheableHashMap();
              int ts1 = getTS(System.DateTime.Now);
              for (int i = 0; i < number; i++)
              {
                if (size == 0)
                {
                  map.Add(new CacheableInt32(i), new ExampleObject(i));
                }
                else
                {
                  map.Add(CacheableInt32.Create(i),
                    CacheableBytes.Create(payload));
                }
              }
              m_region.PutAll(map);
              int ts2 = getTS(System.DateTime.Now);
              Double run_t = (ts2 - ts1) / 1000.0;
              Console.WriteLine("Ops/sec: {0}, {1}, {2}", number / run_t, ts1, ts2);
            } else {
              Console.WriteLine("usage: run numOfKeys entrySize");
            }
            break;

          case "putall":
            if (strSplit.Length == 3)
            {
              myKey = strSplit[1];
              myValue = strSplit[2];
              int num = int.Parse(myValue);

              //byte[] payload = new byte[10];
              //for (int index = 0; index < 10; index++)
              //{
              //payload[index] = 1;
              //}

              Console.WriteLine("putAll: " + myKey + ":" + num);

              CacheableHashMap map = new CacheableHashMap();
              map.Clear();
              for (int i = 0; i < num; i++)
              {
                string key = myKey + i;
                // map.Add(new CacheableString(key), (CacheableBytes)(payload));
                map.Add(new CacheableString(key), new ExampleObject(i));
                // map.Add(new CacheableString(key), new Position(i));
              }
              m_region.PutAll(map);
            } else {
              Console.WriteLine("usage: putAll keyBase numOfKeys");
            }
            break;

          case "put":
            if (strSplit.Length == 3)
            {
              myKey = strSplit[1];
              myValue = strSplit[2];

              // Check to see if value is ComplexNumber or String
              ComplexNumber cNum = ComplexNumber.Parse(myValue);

              if (cNum != null)
              {
                // Put the key and value
                PutComplex(myKey, cNum);
              }
              else
              {
                // Put the key and value
                PutStr(myKey, myValue);
              }
            }
            else if (strSplit.Length == 4)
            {
              myKey = strSplit[1];
              myValue = strSplit[2];
              string type = strSplit[3];
              type = type.ToLower();

              switch (type)
              {
                case "str":
                  PutStr(myKey, myValue);
                  break;

                case "bytes":
                  PutBytes(myKey, myValue);
                  break;

                case "complex":
                  ComplexNumber cNum = ComplexNumber.Parse(myValue);
                  if (cNum != null)
                  {
                    PutComplex(myKey, cNum);
                  }
                  else
                  {
                    Console.WriteLine("Could not parse the complex number.");
                  }
                  break;

                case "double":
                  double dval = 0.0;
                  try
                  {
                    dval = double.Parse(myValue);
                  }
                  catch (Exception ex)
                  {
                    Console.WriteLine("Double value [{0}] not valid: {1}.", myValue, ex);
                    break;
                  }
                  PutObject(myKey, new CacheableDouble(dval));
                  break;

                case "float":
                  float fval = 0.0F;
                  try
                  {
                    fval = float.Parse(myValue);
                  }
                  catch (Exception ex)
                  {
                    Console.WriteLine("Float value [{0}] not valid: {1}.", myValue, ex);
                    break;
                  }
                  PutObject(myKey, new CacheableFloat(fval));
                  break;

                case "obj":
                  string xmlStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
                  xmlStr += "<IndexMessage xmlns=\"urn:example.com:FinanceManager.Messages:v1.0\">";
                  xmlStr += "<Keys>11</Keys><Keys>22</Keys><Keys>33</Keys></IndexMessage>";

                  System.Xml.XmlDocument xd = new System.Xml.XmlDocument();
                  xd.LoadXml(xmlStr);
                  PutObject(myKey, CacheableObjectXml.Create(xd));
                  break;

                case "port":
                  int id = 0;
                  try
                  {
                    id = int.Parse(myValue);
                  }
                  catch
                  {
                    Console.WriteLine("Portfolio value [{0}] not a valid"
                      + " integer ID.", myValue);
                    break;
                  }
                  PutPortfolio(myKey, id);
                  break;

                case "pos":
                  int id2 = 0;
                  try
                  {
                    id2 = int.Parse(myValue);
                  }
                  catch
                  {
                    Console.WriteLine("Position value [{0}] not a valid"
                      + " integer ID.", myValue);
                    break;
                  }
                  PutPosition(myKey, id2);
                  break;

                default:
                  Console.WriteLine("usage: put key value [str|bytes|complex|double|float|obj|port|pos]");
                  break;
              }
            }
            else
            {
              Console.WriteLine("usage: put key value [str|bytes|complex|double|float|obj|port|pos]");
            }
            break;

          case "get":
            if (strSplit.Length == 2)
            {
              myKey = strSplit[1];

              // Get the value
              string xStr = GetStr(myKey);
            }
            else
            {
              Console.WriteLine("usage: get key");
            }

            break;

          case "exec":
            if (strSplit.Length > 1)
            {
              string query = string.Empty;
              for (int index = 1; index < strSplit.Length; index++)
              {
                query += strSplit[index] + ' ';
              }
              query = query.Trim();
              RunQuery(query, false);
            }
            else
            {
              Console.WriteLine("usage: exec <select query string>");
            }

            break;

          case "query":
            if (strSplit.Length > 1)
            {
              string query = string.Empty;
              for (int index = 1; index < strSplit.Length; index++)
              {
                query += strSplit[index] + ' ';
              }
              query = query.Trim();
              RunQuery(query, true);
            }
            else
            {
              Console.WriteLine("usage: query <query predicate>");
            }

            break;

          case "existsvalue":
            if (strSplit.Length > 1)
            {
              string query = string.Empty;
              for (int index = 1; index < strSplit.Length; index++)
              {
                query += strSplit[index] + ' ';
              }
              query = query.Trim();
              ExistsValue(query);
            }
            else
            {
              Console.WriteLine("usage: existsvalue <query predicate>");
            }

            break;

          case "selectvalue":
            if (strSplit.Length > 1)
            {
              string query = string.Empty;
              for (int index = 1; index < strSplit.Length; index++)
              {
                query += strSplit[index] + ' ';
              }
              query = query.Trim();
              SelectValue(query);
            }
            else
            {
              Console.WriteLine("usage: selectvalue <query predicate>");
            }

            break;
        }
      }
    }

    #endregion
  }
}
