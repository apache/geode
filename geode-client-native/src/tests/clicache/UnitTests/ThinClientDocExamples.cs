
using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;
  using GemStone.GemFire.Cache.Examples;
  namespace gemfire.cliwrap.Examples
  {
    public class ExampleCacheListenerCallback : ICacheListener
    {
      public virtual void AfterCreate(EntryEvent ev) { }
      public virtual void AfterDestroy(EntryEvent ev) { }
      public virtual void AfterInvalidate(EntryEvent ev) { }
      public virtual void AfterRegionClear(RegionEvent ev) { }
      public virtual void AfterRegionDestroy(RegionEvent ev) { }
      public virtual void AfterRegionDisconnected(Region region) { }
      public virtual void AfterRegionInvalidate(RegionEvent ev) { }
      public virtual void AfterRegionLive(RegionEvent ev) { }
      public virtual void AfterUpdate(EntryEvent ev) { }
      public virtual void Close(Region region) { }
    }

    //Example 13.2 Demonstrating Gets and Puts Using the C# .NET API
    public class ExamplePutGet
    {
      #region Local constants
      private static Cache cache = null;
      private static Region region = null;
      #endregion

      public static void Main()
      {
        // Create a GemFire cache
        DSInit();
        // Enter puts and gets manually
        DoCommand();
        // Close the cache
        DSClose();
      }

      #region Local functions
      public static void DSInit()
      {
        // Create a cache
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(null);
        cache = cacheFactory.Create();

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        // Add cache listener callback to region
        ExampleCacheListenerCallback listenerCallback = new
        ExampleCacheListenerCallback();
        region = regionFactory.SetCacheListener(listenerCallback).Create("exampleputgetregion");
        // Register the ComplexNumber type
        Serializable.RegisterType(ComplexNumber.Create);
      }
      public static void DSClose()
      {
        // Close cache
        cache.Close();
      }
      // Puts a string key and string value
      public static void PutStr(string key, string value)
      {
        CacheableString cKey = new CacheableString(key);
        CacheableString cValue = new CacheableString(value);
        region.Put(cKey, cValue);
        Console.WriteLine("Put string -- key: " + key + " value: " + value);
      }
      // Puts a string key and complex number value
      public static void PutComplex(string key, ComplexNumber value)
      {
        CacheableString cKey = new CacheableString(key);
        region.Put(cKey, value);
        Console.WriteLine("Put complex -- key: " + key + " value: " + value);
      }
      // Gets the value, if the key exists
      public static string GetStr(string key)
      {
        string testStr = "";
        CacheableString cKey = new CacheableString(key);
        // Get the value
        IGFSerializable cValue = region.Get(cKey);
        // Is the key found?
        if (cValue != null)
        {
          testStr = cValue.ToString();
          // Type of value?
          if (cValue is CacheableString)
          {
            Console.WriteLine("Get string -- key: " + key + " value: " + testStr);
          }
          else if (cValue is ComplexNumber)
          {
            Console.WriteLine("Get complex -- key: " + key + " value: " + testStr);
          }
        }
        else
        {
          testStr = "NULL";
          Console.WriteLine("No such key in region: " + key);
        }
        return testStr;
      }
      // Waits for input of get, put, or quit command, then
      // does just that.
      public static void DoCommand()
      {
        string myCmd = "";
        string myKey;
        string myValue;
        while (myCmd != "quit")
        {
          Console.Write("get, put, quit: ");
          string strIn = Console.ReadLine().Trim();
          string[] strSplit = new string[3];
          strSplit = strIn.Split(' ');
          myCmd = strSplit[0];
          if (myCmd == "q") { myCmd = "quit"; }
          switch (myCmd)
          {
            case "put":
              if (strSplit.Length == 3)
              {
                myKey = strSplit[1];
                myValue = strSplit[2];
                // Check to see if value is ComplexNumber or String
                ComplexNumber cNumValue = ComplexNumber.Parse(myValue);
                if (cNumValue != null)
                {
                  // Put the key and value
                  PutComplex(myKey, cNumValue);
                }
                else
                {
                  // Put the key and value
                  PutStr(myKey, myValue);
                }
              }
              else
              {
                Console.WriteLine("usage: put key value");
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
          }
        }
      }
      #endregion
    }
  }
  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientDocExamples : ThinClientRegionSteps
  {
    #region Private members
    private static Cache cache = null;
    private static Region region = null;
    private UnitProcess m_client1;
    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      return new ClientBase[] { m_client1 };
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      CacheHelper.StopJavaServers();
      base.EndTests();
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        m_client1.Call(closeDS);
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    #region NC doc chapter 5
    public void closeDS()
    {
      if (cache != null)
        cache.Close();
    }

    public void preIntializeCache()
    {
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
      cache = cacheFactory.Create();
    }
    public void preIntializeCache1()
    {
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
      cacheFactory.SetSubscriptionEnabled(true);
      cache = cacheFactory.Create();
    }

    //Example 5.2 Creating a Cache
    public void example_5_2()
    {
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(null);
      Cache cache = cacheFactory.Create();
    }

    //Example 5.3 Creating a Cache with a cache.xml File
    public void example_5_3()
    {
      Properties prop = Properties.Create();
      prop.Insert("cache-xml-file", "cache.xml");
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prop);
      Cache cache = cacheFactory.Create();
    }

    //Example 5.4 Creating a Region with Caching and LRU
    public void example_5_4()
    {
      // Create the region
      RegionFactory regionFact = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
      region = regionFact.SetLruEntriesLimit(20000)
                         .SetInitialCapacity(20000)
                         .Create("exampleRegion");
    }

    //Example 5.5 Creating a Region and Subregion With Disk Overflow
    public void example_5_5()
    {
      // Set up some region attributes
      Properties bdbProperties = Properties.Create();
      bdbProperties.Insert("PersistenceDirectory", "BDB");
      bdbProperties.Insert("EnvironmentDirectory", "BDBEnv");
      bdbProperties.Insert("CacheSizeGb", "1");
      bdbProperties.Insert("CacheSizeMb", "10");
      bdbProperties.Insert("PageSize", "65536");
      bdbProperties.Insert("MaxFileSize", "512000");

      RegionFactory regionFact = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
      region = regionFact.SetLruEntriesLimit(20000)
                         .SetInitialCapacity(20000)
                         .SetDiskPolicy(DiskPolicyType.Overflows)
                         .SetPersistenceManager("BDBImpl", "createBDBInstance", bdbProperties)
                         .Create("exampleRegion");
    }

    //Example 5.6 Using the API to Put Values Into the Cache
    public void example_5_6()
    {
      for (int i = 0; i < 100; i++)
      {
        region.Put(i, i);
      }
    }

    //Example 5.7 Using the Get API to Retrieve Values From the Cache
    public void example_5_7()
    {
      for (int i = 0; i < 100; i++)
      {
        CacheableInt32 value = region.Get(i) as CacheableInt32;
      }
    }

    //Example 5.9 Implementing a Serializable Class        
    public class BankAccount : IGFSerializable
    {
      private int m_customerId;
      private int m_accountId;
      public int Customer
      {
        get
        {
          return m_customerId;
        }
      }
      public int Account
      {
        get
        {
          return m_accountId;
        }
      }
      public BankAccount(int customer, int account)
      {
        m_customerId = customer;
        m_accountId = account;
      }
      // Our TypeFactoryMethod
      public static IGFSerializable CreateInstance()
      {
        return new BankAccount(0, 0);
      }
      #region IGFSerializable Members
      public void ToData(DataOutput output)
      {
        output.WriteInt32(m_customerId);
        output.WriteInt32(m_accountId);
      }
      public IGFSerializable FromData(DataInput input)
      {
        m_customerId = input.ReadInt32();
        m_accountId = input.ReadInt32();
        return this;
      }
      public UInt32 ClassId
      {
        get
        {
          return 11;
        }
      }
      public UInt32 ObjectSize
      {
        get
        {
          return (UInt32)(sizeof(Int32) + sizeof(Int32));
        }

      }
      #endregion
    }

    //Example 5.10 Extending an IGFSerializable Class to Be a Key
    class BankAccountKey : ICacheableKey
    {
      #region Private members

      private int m_customerId;
      private int m_accountId;

      #endregion

      #region Public accessors

      public int Customer
      {
        get
        {
          return m_customerId;
        }
      }

      public int Account
      {
        get
        {
          return m_accountId;
        }
      }

      #endregion

      public BankAccountKey(int customer, int account)
      {
        m_customerId = customer;
        m_accountId = account;
      }

      // Our TypeFactoryMethod
      public static IGFSerializable CreateInstance()
      {
        return new BankAccountKey(0, 0);
      }

      #region IGFSerializable Members

      public void ToData(DataOutput output)
      {
        output.WriteInt32(m_customerId);
        output.WriteInt32(m_accountId);
      }

      public IGFSerializable FromData(DataInput input)
      {
        m_customerId = input.ReadInt32();
        m_accountId = input.ReadInt32();
        return this;
      }

      public UInt32 ClassId
      {
        get
        {
          return 11;
        }
      }

      public UInt32 ObjectSize
      {
        get
        {
          return (UInt32)(sizeof(Int32) + sizeof(Int32));
        }

      }

      #endregion

      #region ICacheableKey Members

      public bool Equals(ICacheableKey other)
      {
        BankAccountKey otherAccount = other as BankAccountKey;
        if (otherAccount != null)
        {
          return (m_customerId == otherAccount.m_customerId) &&
            (m_accountId == otherAccount.m_accountId);
        }
        return false;
      }

      public override int GetHashCode()
      {
        return (m_customerId ^ m_accountId);
      }

      #endregion

      #region Overriden System.Object methods

      public override bool Equals(object obj)
      {
        BankAccountKey otherAccount = obj as BankAccountKey;
        if (otherAccount != null)
        {
          return (m_customerId == otherAccount.m_customerId) &&
            (m_accountId == otherAccount.m_accountId);
        }
        return false;
      }

      // Also override ToString to get a nice string representation.
      public override string ToString()
      {
        return string.Format("BankAccountKey( customer: {0}, account: {1} )",
          m_customerId, m_accountId);
      }

      #endregion
    }

    //Example 5.11 Using a BankAccount Object    
    class AccountHistory : IGFSerializable
    {
      #region Private members

      private List<string> m_history;

      #endregion

      public AccountHistory()
      {
        m_history = new List<string>();
      }

      public void ShowAccountHistory()
      {
        Console.WriteLine("AccountHistory:");
        foreach (string hist in m_history)
        {
          Console.WriteLine("\t{0}", hist);
        }
      }

      public void AddLog(string entry)
      {
        m_history.Add(entry);
      }

      public static IGFSerializable CreateInstance()
      {
        return new AccountHistory();
      }

      #region IGFSerializable Members

      public IGFSerializable FromData(DataInput input)
      {
        int len = input.ReadInt32();

        m_history.Clear();
        for (int i = 0; i < len; i++)
        {
          m_history.Add(input.ReadUTF());
        }
        return this;
      }

      public void ToData(DataOutput output)
      {
        output.WriteInt32(m_history.Count);
        foreach (string hist in m_history)
        {
          output.WriteUTF(hist);
        }
      }

      public UInt32 ClassId
      {
        get
        {
          return 0x05;
        }
      }

      public UInt32 ObjectSize
      {
        get
        {
          UInt32 objectSize = 0;
          foreach (string hist in m_history)
          {
            objectSize += (UInt32)(hist == null ? 0 : sizeof(char) * hist.Length);
          }
          return objectSize;

        }

      }

      #endregion
    }
    public class TestBankAccount
    {
      public static void Main()
      {
        // Register the user-defined serializable type.
        Serializable.RegisterType(AccountHistory.CreateInstance);
        Serializable.RegisterType(BankAccountKey.CreateInstance);
        // Create a cache.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(null);
        Cache cache = cacheFactory.Create();
        // Create a region.
        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
        Region region = regionFactory.Create("BankAccounts");
        // Place some instances of BankAccount cache region.
        BankAccountKey baKey = new BankAccountKey(2309, 123091);
        AccountHistory ahVal = new AccountHistory();
        ahVal.AddLog("Created account");
        region.Put(baKey, ahVal);
        Console.WriteLine("Put an AccountHistory in cache keyed with BankAccount.");
        // Display the BankAccount information.
        Console.WriteLine(baKey.ToString());
        // Call custom behavior on instance of AccountHistory.
        ahVal.ShowAccountHistory();
        // Get a value out of the region.
        AccountHistory history = region.Get(baKey) as AccountHistory;
        if (history != null)
        {
          Console.WriteLine("Found AccountHistory in the cache.");
          history.ShowAccountHistory();
          history.AddLog("debit $1,000,000.");
          region.Put(baKey, history);
          Console.WriteLine("Updated AccountHistory in the cache.");
        }
        // Look up the history again.
        history = region.Get(baKey) as AccountHistory;
        if (history != null)
        {
          Console.WriteLine("Found AccountHistory in the cache.");
          history.ShowAccountHistory();
        }
        // Close the cache.
        cache.Close();
      }
    }

    //Example 5.12 Using ICacheLoader to Load New Integers in the Region
    class ExampleLoaderCallback : ICacheLoader
    {
      #region Private members
      private int m_loads = 0;
      #endregion
      #region Public accessors
      public int Loads
      {
        get
        {
          return m_loads;
        }
      }
      #endregion
      #region ICacheLoader Members
      public IGFSerializable Load(Region region, ICacheableKey key, IGFSerializable helper)
      {
        return new CacheableInt32(m_loads++);
      }
      public virtual void Close(Region region)
      {
        Console.WriteLine("Received region close event.");
      }
      #endregion
    }

    //Example 5.13 Using ICacheWriter to Track Creates and Updates for a Region
    class ExampleWriterCallback : ICacheWriter
    {
      #region Private members
      private int m_creates = 0;
      private int m_updates = 0;
      #endregion
      #region Public accessors
      public int Creates
      {
        get
        {
          return m_creates;
        }
      }
      public int Updates
      {
        get
        {
          return m_updates;
        }
      }
      #endregion
      public void ShowTallies()
      {
        Console.WriteLine("Updates = {0}, Creates = {1}",
        m_updates, m_creates);
      }
      #region ICacheWriter Members
      public virtual bool BeforeCreate(EntryEvent ev)
      {
        m_creates++;
        Console.WriteLine("Received BeforeCreate event of: {0}", ev.Key);
        return true;
      }
      public virtual bool BeforeDestroy(EntryEvent ev)
      {
        Console.WriteLine("Received BeforeDestroy event of: {0}", ev.Key);
        return true;
      }
      public virtual bool BeforeRegionDestroy(RegionEvent ev)
      {
        Console.WriteLine("Received BeforeRegionDestroy event of: {0}", ev.Region.Name);
        return true;
      }
      public virtual bool BeforeUpdate(EntryEvent ev)
      {
        m_updates++;
        Console.WriteLine("Received BeforeUpdate event of: {0}", ev.Key);
        return true;
      }
      public virtual bool BeforeRegionClear(RegionEvent ev)
      {
        Console.WriteLine("Received BeforeRegionClear event");
        return true;
      }
      public virtual void Close(Region region)
      {
        Console.WriteLine("Received Close event of: {0}", region.Name);
      }
      #endregion
    }

    //Example 5.14 A Sample ICacheListener Implementation
    /// <summary>
    /// Capture and display cache events.
    /// </summary>
    class ExampleListenerCallback : ICacheListener
    {
      #region ICacheListener Members
      public void AfterCreate(EntryEvent ev)
      {
        Console.WriteLine("Received AfterCreate event of: {0}", ev.Key);
      }
      public void AfterDestroy(EntryEvent ev)
      {
        Console.WriteLine("Received AfterDestroy event of: {0}", ev.Key);
      }
      public void AfterInvalidate(EntryEvent ev)
      {
        Console.WriteLine("Received AfterInvalidate event of: {0}",
        ev.Key);
      }
      public void AfterRegionDestroy(RegionEvent ev)
      {
        Console.WriteLine("Received AfterRegionDestroy event of region: {0}",
        ev.Region.Name);
      }
      public void AfterRegionClear(RegionEvent ev)
      {
        Console.WriteLine("Received AfterRegionClear event of region: {0}",
        ev.Region.Name);
      }
      public void AfterRegionLive(RegionEvent ev)
      {
        Console.WriteLine("Received AfterRegionLive event of region: {0}",
        ev.Region.Name);
      }
      public void AfterRegionInvalidate(RegionEvent ev)
      {
        Console.WriteLine("Received AfterRegionInvalidate event of region:{0}", ev.Region.Name);
      }
      public void AfterRegionDisconnected(Region region)
      {
        Console.WriteLine("Received AfterRegionDisconnected event of region:{0}", region.Name);
      }
      public void AfterUpdate(EntryEvent ev)
      {
        Console.WriteLine("Received AfterUpdate event of: {0}", ev.Key);
      }
      public void Close(Region region)
      {
        Console.WriteLine("Received Close event of region: {0}",
        region.Name);
      }
      #endregion
    }

    //Example 5.15 Simple C# Code   
    class FirstSteps
    {
      public static void Main()
      {
        // 1. Create a cache
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
        Cache cache = cacheFactory.Create();
        // 2. Create default region attributes using region factory
        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
        // 3. Create region
        Region region = regionFactory.Create("exampleputgetregion");
        // 4. Put some entries
        int iKey = 777;
        string sKey = "abc";
        region.Put(iKey, 12345678);
        region.Put(sKey, "testvalue");
        // 5. Get the entries
        CacheableInt32 ciValue = region.Get(iKey) as CacheableInt32;
        Console.WriteLine("Get - key: {0}, value: {1}", iKey, ciValue.Value);
        CacheableString csValue = region.Get(sKey) as CacheableString;
        Console.WriteLine("Get - key: {0}, value: {1}", sKey, csValue.Value);
        // 6. Close cache
        cache.Close();
      }
    }

    public void ch_Example_5()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(example_5_2);
      m_client1.Call(example_5_4);
      m_client1.Call(example_5_6);
      m_client1.Call(example_5_7);
      CacheHelper.StopJavaServers();
    }

    public void example_5_11()
    {
      TestBankAccount.Main();
    }

    public void ch_Example_5_11()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(example_5_11);
      CacheHelper.StopJavaServers();
    }

    public void example_5_15()
    {
      FirstSteps.Main();
    }

    public void ch_Example_5_15()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(example_5_15);
      CacheHelper.StopJavaServers();
    }

    #endregion

    //Example 8.2 .NET Client Acquiring Credentials Programmatically
    public void example_8_2_Security()
    {
      Properties secProp = Properties.Create();
      secProp.Insert("security-client-auth-factory",
      "GemStone.GemFire.Templates.Cache.Security.UserPasswordAuthInit.Create");
      secProp.Insert("security-client-auth-library", "GemStone.GemFire.Templates.Cache.Security");
      secProp.Insert("security-username", " gemfire6");
      secProp.Insert("security-password", " gemfire6Pass");
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(secProp);
      Cache cache = cacheFactory.Create();
    }

    public void ch_Example_8_2()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(example_8_2_Security);
      CacheHelper.StopJavaServers();
    }


    class TradeOrder : IGFSerializable
    {
      private
       double price;
      CacheableString pkid;
      CacheableString type;
      CacheableString status;
      CacheableStringArray names;
      byte[] newVal;
      int newValSize;
      CacheableDate creationDate;
      byte[] arrayZeroSize;
      byte[] arrayNull;

      public
      TradeOrder()
      {
        price = 0.0;
        pkid = null;
        type = null;
        status = null;
        newVal = null;
        arrayZeroSize = null;
        arrayNull = null;
        creationDate = null;
        newValSize = 0;
      }
      ~TradeOrder()
      {
      }
      public TradeOrder(double pr, int id)
      {
        price = pr;
        pkid = new CacheableString(id.ToString());
        type = null;
        status = null;
        newVal = null;
        arrayZeroSize = null;
        arrayNull = null;
        creationDate = null;
        newValSize = 0;
      }

      public override string ToString()
      {
        String str = "TradeOrder [price=" + price + " status=" + status + " type=" + type
        + "pkid=" + pkid + "creationDate=" + creationDate + "\n ";
        return str + "\n]";
      }

      // Add the following for the Serializable interface
      // Our TypeFactoryMethod
      public static IGFSerializable CreateInstance()
      {
        return new TradeOrder();
      }
      public UInt32 ClassId
      {
        get
        {
          return 4; // must match Java
        }
      }

      uint getObjectSize(IGFSerializable obj)
      {
        return (obj == null ? 0 : obj.ObjectSize);
      }

      public uint ObjectSize
      {
        get
        {
          return (uint)sizeof(double) + sizeof(int);
        }
      }

      public void ToData(DataOutput output)
      {
        output.WriteDouble(price);
        output.WriteObject(pkid);
        output.WriteObject(type);
        output.WriteUTF(status);
        output.WriteObject(names);
        output.WriteBytes(newVal, newValSize + 1);
        output.WriteObject(creationDate);
        output.WriteBytes(arrayNull, 0);
        output.WriteBytes(arrayZeroSize, 0);
      }

      public IGFSerializable FromData(DataInput input)
      {
        price = input.ReadDouble();
        pkid = (CacheableString)input.ReadObject();
        type = (CacheableString)input.ReadObject();
        status = (CacheableString)input.ReadUTF();
        names = (CacheableStringArray)input.ReadObject();
        newVal = input.ReadBytes();
        creationDate = (CacheableDate)input.ReadObject();
        arrayNull = input.ReadBytes();
        arrayZeroSize = input.ReadBytes();
        return this;
      }
    }

    //Example 10.2 CqListener Implementation (C#)
    // CqListener class
    public class TradeEventListener : ICqListener
    {
      public void OnEvent(CqEvent cqEvent)
      {
        // Operation associated with the query op
        CqOperationType queryOperation = cqEvent.getQueryOperation();
        // key and new value from the event
        ICacheableKey key = cqEvent.getKey();
        CacheableString keyStr = key as CacheableString;
        IGFSerializable val = cqEvent.getNewValue();
        TradeOrder tradeOrder = val as TradeOrder;
        if (queryOperation == CqOperationType.OP_TYPE_UPDATE)
        {
          // update data on the screen for the trade order
          //. . .
        }
        else if (queryOperation == CqOperationType.OP_TYPE_CREATE)
        {
          // add the trade order to the screen
          //. . .
        }
        else if (queryOperation == CqOperationType.OP_TYPE_DESTROY)
        {
          // remove the trade order from the screen
          //. . .
        }
      }
      public void OnError(CqEvent cqEvent)
      {
        // handle the error
      }
      // From CacheCallback
      public void Close()
      {
        // close the output screen for the trades
        //. . .
      }
    }

    //Example 10.4 CQ Creation, Execution, and Close (C#)
    public void example_10_4_Continous_Querying()
    {
      // Get cache and queryService - refs to local cache and QueryService
      // Create client /tradeOrder region configured to talk to the server     
      //Serializable.RegisterType( TradeOrder.CreateInstance);
      RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
      Region region = regionFactory.Create("tradeOrder");
      // Create CqAttribute using CqAttributeFactory
      CqAttributesFactory cqf = new CqAttributesFactory();
      // Create a listener and add it to the CQ attributes
      //callback defined below
      ICqListener tradeEventListener = new TradeEventListener();
      cqf.AddCqListener(tradeEventListener);
      CqAttributes cqa = cqf.Create();
      // Name of the CQ and its query
      String cqName = "priceTracker ";
      String queryStr = "SELECT * FROM /tradeOrder t where t.price >100 ";
      QueryService qrySvc = cache.GetQueryService();
      // Create the CqQuery      
      CqQuery priceTracker = qrySvc.NewCq(cqName, queryStr, cqa, true);
      try
      {
        // Execute CQ
        priceTracker.Execute();
      }
      catch (Exception /*ex*/)
      {
        //handle exception
      }
      // Now the CQ is running on the server, sending CqEvents to the listener
      //. . .
      // End of life for the CQ - clear up resources by closing
      priceTracker.Close();
    }

    public void ch_Example_10_4()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(preIntializeCache1);
      m_client1.Call(example_10_4_Continous_Querying);
      CacheHelper.StopJavaServers();
    }

    //Example 11.3 Connection Pool Creation and Execution Using (C#)
    public void example_11_3_Connection_Pools()
    {
      Properties prop = Properties.Create();
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prop);

      Cache cache = cacheFactory.Create();

      PoolFactory poolFact = PoolManager.CreateFactory();
      //to create pool add either endpoints or add locators
      //pool with endpoint, adding to pool factory.
      poolFact.AddServer("localhost", 40404 /*port number*/);
      //pool with locator, adding to pool factory
      //poolFact.AddLocator("hostname", 15000 /*port number*/);
      Pool pool = null;
      if (PoolManager.Find("poolName") == null)
      {
        pool = poolFact.Create("poolName");
      }
      int loadConfigInterval = pool.LoadConditioningInterval;
      RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
      Region region = regionFactory.SetPoolName("poolName").Create("regionName");
      QueryService qs = cache.GetQueryService("poolName");
    }

    public void ch_Example_11_3()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(example_11_3_Connection_Pools);
      CacheHelper.StopJavaServers();
    }

    //Example 12.2 Data-Dependant Function Invoked from a Client (C#)
    public void example_12_2_FunctionExecution()
    {
      region = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY).Create("partition_region");
      Console.WriteLine("Created the Region");
      String getFuncName = "MultiGetFunctionI";

      for (int i = 0; i < 34; i++)
      {
        region.Put("KEY--" + i, "VALUE--" + i);
      }

      IGFSerializable[] routingObj = new IGFSerializable[17];
      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = new CacheableString("KEY--" + i);
        j++;
      }
      Console.WriteLine("routingObj count= {0}.", routingObj.Length);

      IGFSerializable args0 = new CacheableBoolean(true);
      Boolean getResult = true;
      //test data dependant function execution
      //     test get function with result
      Execution exc = FunctionService.OnRegion(region);
      IResultCollector rc = exc.WithArgs(args0).WithFilter(routingObj).Execute(
      getFuncName, getResult);
      IGFSerializable[] executeFunctionResult = rc.GetResult();
      Console.WriteLine("routingObj count1= {0}.", routingObj.Length);
    }

    public void ch_Example_12_2()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(CacheHelper.InitName, "example_dsys", "example_cache");
      m_client1.Call(preIntializeCache);
      m_client1.Call(example_12_2_FunctionExecution);
      CacheHelper.StopJavaServers();
    }

    //Example 12.4 Function Execution on a Server in a Distributed System (C#)
    public void example_12_4_FunctionExecution()
    {
      region = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY).Create("partition_region");
      Console.WriteLine("Created the Region");
      String getFuncName = "MultiGetFunctionI";

      for (int i = 0; i < 34; i++)
      {
        region.Put("KEY--" + i, "VALUE--" + i);
      }

      IGFSerializable[] routingObj = new IGFSerializable[17];
      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = new CacheableString("KEY--" + i);
        j++;
      }
      Console.WriteLine("routingObj count= {0}.", routingObj.Length);
      bool getResult = true;
      //test date independant fucntion execution on one server
      //     test get function with result
      Execution exc = FunctionService.OnServer(cache);
      CacheableVector args1 = new CacheableVector();
      for (int i = 0; i < routingObj.Length; i++)
      {
        Console.WriteLine("routingObj[{0}]={1}.", i, (routingObj[i] as CacheableString).Value);
        args1.Add(routingObj[i]);
      }
      IResultCollector rc = exc.WithArgs(args1).Execute(
      getFuncName, getResult);
      IGFSerializable[] executeFunctionResult = rc.GetResult();
      Console.WriteLine("on one server: result count= {0}.", executeFunctionResult.Length);
    }

    public void ch_Example_12_4()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(CacheHelper.InitName, "example_dsys", "example_cache");
      m_client1.Call(preIntializeCache);
      m_client1.Call(example_12_4_FunctionExecution);
      CacheHelper.StopJavaServers();
    }

    public class ExampleObject
    : IGFSerializable
    {
      #region Private members
      private double double_field;
      private float float_field;
      private long long_field;
      private int int_field;
      private short short_field;
      private string string_field;
      private System.Collections.ArrayList string_vector = new System.Collections.ArrayList();
      #endregion

      #region Public accessors
      public int Int_Field
      {
        get
        {
          return int_field;
        }
        set
        {
          int_field = value;
        }
      }
      public short Short_Field
      {
        get
        {
          return short_field;
        }
        set
        {
          short_field = value;
        }
      }
      public long Long_Field
      {
        get
        {
          return long_field;
        }
        set
        {
          long_field = value;
        }
      }
      public float Float_Field
      {
        get
        {
          return float_field;
        }
        set
        {
          float_field = value;
        }
      }
      public double Double_Field
      {
        get
        {
          return double_field;
        }
        set
        {
          double_field = value;
        }
      }
      public string String_Field
      {
        get
        {
          return string_field;
        }
        set
        {
          string_field = value;
        }
      }
      public System.Collections.ArrayList String_Vector
      {
        get
        {
          return string_vector;
        }
        set
        {
          string_vector = value;
        }
      }
      public override string ToString()
      {
        string buffer = "ExampleObject: " + int_field + "(int)," + string_field + "(str),";
        buffer += "[";
        for (int idx = 0; idx < string_vector.Count; idx++)
        {
          buffer += string_vector[idx];
        }
        buffer += "(string_vector)]";
        return buffer;
      }
      #endregion

      #region Constructors
      public ExampleObject()
      {
        double_field = (double)0.0;
        float_field = (float)0.0;
        long_field = 0;
        int_field = 0;
        short_field = 0;
        string_field = "";
        string_vector.Clear();
      }

      public ExampleObject(int id)
      {
        int_field = id;
        short_field = (Int16)id;
        long_field = (Int64)id;
        float_field = (float)id;
        double_field = (double)id;
        string_field = "" + id;
        string_vector.Clear();
        for (int i = 0; i < 3; i++)
        {
          string_vector.Add(string_field);
        }
      }
      public ExampleObject(string sValue)
      {
        int_field = Int32.Parse(sValue);
        long_field = Int64.Parse(sValue);
        short_field = Int16.Parse(sValue);
        double_field = (double)int_field;
        float_field = (float)int_field;
        string_field = sValue;
        string_vector.Clear();
        for (int i = 0; i < 3; i++)
        {
          string_vector.Add(sValue);
        }
      }
      #endregion

      #region IGFSerializable Members
      public IGFSerializable FromData(DataInput input)
      {
        double_field = input.ReadInt64();
        float_field = input.ReadFloat();
        long_field = input.ReadInt64();
        int_field = input.ReadInt32();
        short_field = input.ReadInt16();
        string_field = input.ReadUTF();
        int itemCount = input.ReadInt32();
        string_vector.Clear();
        for (int idx = 0; idx < itemCount; itemCount++)
        {
          string_vector.Add(input.ReadUTF());
        }
        return this;
      }

      public void ToData(DataOutput output)
      {
        output.WriteDouble(double_field);
        output.WriteFloat(float_field);
        output.WriteInt64(long_field);
        output.WriteInt32(int_field);
        output.WriteInt16(short_field);
        output.WriteUTF(string_field);
        int itemCount = string_vector.Count;
        output.WriteInt32(itemCount);
        for (int idx = 0; idx < itemCount; idx++)
        {
          string s = (string)string_vector[idx];
          output.WriteUTF(s);
        }
      }

      public UInt32 ObjectSize
      {
        get
        {
          UInt32 objectSize = 0;
          objectSize += (UInt32)sizeof(double);
          objectSize += (UInt32)sizeof(float);
          objectSize += (UInt32)sizeof(Int64);
          objectSize += (UInt32)sizeof(Int32);
          objectSize += (UInt32)sizeof(Int16);
          objectSize += (UInt32)(string_field == null ? 0 : sizeof(char) * string_field.Length);
          objectSize += (UInt32)sizeof(Int32);
          for (int idx = 0; idx < string_vector.Count; idx++)
          {
            string s = (string)string_vector[idx];
            objectSize += (UInt32)(string_vector[idx] == null ? 0 : sizeof(char) * s.Length);
          }
          return objectSize;
        }
      }

      public UInt32 ClassId
      {
        get
        {
          return 0x2e;
        }
      }
      #endregion

      public static IGFSerializable CreateDeserializable()
      {
        return new ExampleObject();
      }
    }

    //Example 13.6 Implementing a User-Defined Serializable Object Using the C# API
    class User : IGFSerializable
    {
      private string m_name;
      private int m_userId;
      ExampleObject m_eo;
      public User(string name, int userId)
      {
        m_name = name;
        m_userId = userId;
        m_eo = new ExampleObject();
      }
      public User()
      {
        m_name = string.Empty;
        m_userId = 0;
        m_eo = new ExampleObject();
      }
      public int UserId
      {
        get
        {
          return m_userId;
        }
      }
      public string Name
      {
        get
        {
          return m_name;
        }
      }
      public ExampleObject EO
      {
        get
        {
          return m_eo;
        }
        set
        {
          m_eo = value;
        }
      }
      public override string ToString()
      {
        return string.Format("User: {0}, {1}\n{2}", m_userId, m_name,
        m_eo.ToString());
      }

      // Our TypeFactoryMethod
      public static IGFSerializable CreateInstance()
      {
        return new User();
      }
      #region IGFSerializable Members
      public UInt32 ClassId
      {
        get
        {
          return 45; // must match Java
        }
      }

      public uint ObjectSize
      {
        get
        {
          return m_eo.ObjectSize + 10;
        }
      }

      public IGFSerializable FromData(DataInput input)
      {
        m_name = input.ReadUTF();
        m_userId = input.ReadInt32();
        m_eo.FromData(input);
        return this;
      }
      public void ToData(DataOutput output)
      {
        output.WriteUTF(m_name);
        output.WriteInt32(m_userId);
        m_eo.ToData(output);
      }
      #endregion
    }

    //public void ch_Example_13_2()
    //{
    //  CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription_forDoc.xml");
    //  CacheHelper.StartJavaServer(1, "GFECS1");
    //  m_client1.Call(gemfire.cliwrap.Examples.ExamplePutGet.Main);
    //  CacheHelper.StopJavaServers();
    //}

    [Test]
    public void DocExamples5()
    {
      ch_Example_5();
    }
    [Test]
    public void DocExamples5_11()
    {
      ch_Example_5_11();
    }
    [Test]
    public void DocExamples5_15()
    {
      ch_Example_5_15();
    }
    [Test]
    public void DocExamples8_2()
    {
      ch_Example_8_2();
    }
    [Test]
    public void DocExamples10_4()
    {
      ch_Example_10_4();
    }
    [Test]
    public void DocExamples11_3()
    {
      ch_Example_11_3();
    }
    [Test]
    public void DocExamples12_2()
    {
      ch_Example_12_2();
    }
    [Test]
    public void DocExamples12_4()
    {
      ch_Example_12_4();
    }
    //  [Test]
    //public void DocExamples13_2()
    //{
    //  ch_Example_13_2();
    //}
  }
}