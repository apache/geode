//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Reflection;
using System.IO;
using PdxTests;
namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.Cache.Generic;
  using QueryCategory = GemStone.GemFire.Cache.Tests.QueryCategory;
  using QueryStrings = GemStone.GemFire.Cache.Tests.QueryStrings;
  using QueryStatics = GemStone.GemFire.Cache.Tests.QueryStatics;

  public class SilenceListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>
  {

      long currentTimeInMillies()
      {
          DateTime startTime = DateTime.Now;
          long curruntMillis = SmokePerf<TKey, TVal>.GetDateTimeMillis(startTime);
          return curruntMillis;
      }
    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {

      Util.BBSet("ListenerBB", "lastEventTime", currentTimeInMillies());
    }
    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      Util.BBSet("ListenerBB", "lastEventTime", currentTimeInMillies());
    }
    public override void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      Util.BBSet("ListenerBB", "lastEventTime", currentTimeInMillies());
    }
    public override void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      Util.BBSet("ListenerBB", "lastEventTime", currentTimeInMillies());
    }
  }
  
  public class MyCqListener<TKey, TResult> : ICqListener<TKey, TResult>
  {
    private UInt32 m_updateCnt;
    private UInt32 m_createCnt;
    private UInt32 m_destroyCnt;
    private UInt32 m_eventCnt;

    public MyCqListener()
    {
      m_updateCnt = 0;
      m_createCnt = 0;
      m_destroyCnt = 0;
      m_eventCnt = 0;
    }

    public UInt32 NumInserts()
    {
      return m_createCnt;
    }

    public UInt32 NumUpdates()
    {
      return m_updateCnt;
    }

    public UInt32 NumDestroys()
    {
      return m_destroyCnt;
    }

    public UInt32 NumEvents()
    {
      return m_eventCnt;
    }

    public virtual void UpdateCount(CqEvent<TKey, TResult> ev)
    {
      m_eventCnt++;
      CqOperationType opType = ev.getQueryOperation();
      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
        m_createCnt++;
       // Util.Log("m_create is {0}",m_createCnt);
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        m_updateCnt++;
       // Util.Log("m_create is {0}", m_updateCnt);
      }
      else if (opType == CqOperationType.OP_TYPE_DESTROY)
      {
        m_destroyCnt++;
      //  Util.Log("m_create is {0}", m_destroyCnt);
      }

    }
    public virtual void OnError(CqEvent<TKey, TResult> ev)
    {
      UpdateCount(ev);
    }

    public virtual void OnEvent(CqEvent<TKey, TResult> ev)
    {
      UpdateCount(ev);
    }

    public virtual void Close()
    {
    }
  }
  //----------------------------------- DoOpsTask start ------------------------
    public class DoOpsTask<TKey, TVal> : ClientTask
    {
        private IRegion<TKey, TVal> m_region;
        private  const int INVALIDATE             = 1;
        private  const int LOCAL_INVALIDATE       = 2;
        private  const int DESTROY                = 3;
        private  const int LOCAL_DESTROY          = 4;
        private  const int UPDATE_EXISTING_KEY    = 5;
        private  const int GET                    = 6;
        private  const int ADD_NEW_KEY            = 7;
        private  const int PUTALL_NEW_KEY         = 8;
        private  const int NUM_EXTRA_KEYS = 100;
        private static bool m_istransaction = false;
        CacheTransactionManager txManager = null;
        private object CLASS_LOCK = new object();
        private static string m_sharePath;
        //Assembly m_pdxVersionOneAsm;
        //Assembly m_pdxVersionTwoAsm;
        protected int[] operations = { INVALIDATE, LOCAL_INVALIDATE, DESTROY, LOCAL_DESTROY, UPDATE_EXISTING_KEY, GET, ADD_NEW_KEY, PUTALL_NEW_KEY };
        public DoOpsTask(IRegion<TKey, TVal> region, bool istransaction)
            : base()
        {
            m_region = region;
            m_istransaction = istransaction;
            if(m_istransaction)
                txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;

        }
        public override void DoTask(int iters, object data)
        {
            Random random = new Random();
            List<int> availableOps = new List<int>(operations);
            if (m_istransaction)
            {
               availableOps.Remove(LOCAL_DESTROY);
               availableOps.Remove(LOCAL_INVALIDATE);
            }
            while (Running && (availableOps.Count != 0))
            {
                int opcode = -1;
                lock (CLASS_LOCK)
                {
                    bool doneWithOps = false;
                    int i = random.Next(0, availableOps.Count);
                    //try
                    //{
                        opcode = availableOps[i];
                        if (m_istransaction)
                        {
                            //txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
                            txManager.Begin();
                        }
                        switch (opcode)
                        {
                            case ADD_NEW_KEY:
                                doneWithOps = addNewKey();
                                break;
                            case PUTALL_NEW_KEY:
                                doneWithOps = putAllNewKey();
                                break;
                            case INVALIDATE:
                                doneWithOps = invalidate();
                                break;
                            case DESTROY:
                                doneWithOps = destroy();
                                break;
                            case UPDATE_EXISTING_KEY:
                                doneWithOps = updateExistingKey();
                                break;
                            case GET:
                                doneWithOps = get();
                                break;
                            case LOCAL_INVALIDATE:
                                doneWithOps = localInvalidate();
                                break;
                            case LOCAL_DESTROY:
                                doneWithOps = localDestroy();
                                break;
                            default:
                                {
                                    throw new Exception("Invalid operation specified:" + opcode);
                                }
                        }
                        if (m_istransaction)// && (!doneWithOps))
                        {
                            try
                            {
                                txManager.Commit();
                            }
                           catch (CommitConflictException ex)
                            {
                                string errStr = ex.ToString();
                                if ((errStr.IndexOf("REMOVED") >= 0) ||
                                    (errStr.IndexOf("INVALID") >= 0) ||
                                    (errStr.IndexOf("Deserializ") >= 0))
                                {
                                    throw new Exception("Test got " + errStr + ", but this error text " +
                                          "might contain GemFire internal values\n" + ex.Message);
                                }
                            }
                           
                        }
                    
                    if (doneWithOps)
                    {
                        availableOps.Remove(opcode);
                    }
                }
            }
         } //end doTask function

         private TVal GetValue()
         {
             return GetValue(null);
         }

         private TVal GetValue(object value)
         {
             TVal tmpValue = default(TVal);
             FwkTest<TKey, TVal>.CurrentTest.ResetKey("valueSizes");
             int size = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("valueSizes");
             StringBuilder builder = new StringBuilder();
             Random random = new Random();
             char ch;
             for (int j = 0; j < size; j++)
             {
                 ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                 builder.Append(ch);
             }
             FwkTest<TKey, TVal>.CurrentTest.ResetKey("objectType");
             string m_objectType = FwkTest<TKey, TVal>.CurrentTest.GetStringValue("objectType");
             FwkTest<TKey, TVal>.CurrentTest.ResetKey("versionNum");
             int m_versionNum = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("versionNum"); //random.Next(2) + 1;// (counter++ % 2) + 1;
             string sharedpath = (string)Util.BBGet("SharedPath", "sharedDir");
             m_sharePath = Path.Combine(sharedpath, "framework/csharp/bin");
             if (typeof(TVal) == typeof(string))
             {
                 tmpValue = (TVal)(object)builder.ToString();
             }
             else if (typeof(TVal) == typeof(byte[]))
             {
                 tmpValue = (TVal)(object)(Encoding.ASCII.GetBytes(builder.ToString()));
             }
             else if (m_objectType != null)
             {
                 if (m_objectType.Equals("PdxVersioned") && m_versionNum == 1)
                 {
                     PdxTests<TKey, TVal>.m_pdxVersionOneAsm = Assembly.LoadFrom(Path.Combine(m_sharePath, "PdxVersion1Lib.dll"));
                     Type pt = PdxTests<TKey, TVal>.m_pdxVersionOneAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
                     tmpValue = (TVal)pt.InvokeMember("PdxVersioned", BindingFlags.CreateInstance, null, null, new object[] { value.ToString() });
                 }
                 else if (m_objectType.Equals("PdxVersioned") && m_versionNum == 2)
                 {
                     PdxTests<TKey, TVal>.m_pdxVersionTwoAsm = Assembly.LoadFrom(Path.Combine(m_sharePath, "PdxVersion2Lib.dll"));
                     Type pt = PdxTests<TKey, TVal>.m_pdxVersionTwoAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
                     tmpValue = (TVal)pt.InvokeMember("PdxVersioned", BindingFlags.CreateInstance, null, null, new object[] { value.ToString() });
                 }
                 else if (m_objectType.Equals("Nested"))
                 {

                     tmpValue = (TVal)(object)new NestedPdx();
                 }
                 else if (m_objectType.Equals("PdxType"))
                 {
                     tmpValue = (TVal)(object)new PdxType();
                 }
                 /*else if (m_objectType.Equals("PdxInstanceFactory"))
                 {

                     tmpValue = createIpdxInstance();
                 }*/
                 else if (m_objectType.Equals("AutoSerilizer"))
                 {
                     tmpValue = (TVal)(object)new SerializePdx1(true);
                 }
             }
             else
                 tmpValue = (TVal)(object)value;
             return tmpValue;

         }
        private void checkContainsValueForKey(TKey key, bool expected, string logStr) {
           //RegionPtr regionPtr = getRegion();
           bool containsValue = m_region.ContainsValueForKey(key);
           if (containsValue != expected)
               FwkTest<TKey, TVal>.CurrentTest.FwkException("DoOpsTask::checkContainsValueForKey: Expected containsValueForKey(" + key + ") to be " + expected +
                        ", but it was " + containsValue + ": " + logStr);
        }
        bool addNewKey()
        {
            int numNewKeysCreated = (int)Util.BBGet("ImageBB","NUM_NEW_KEYS_CREATED");
	        Util.BBIncrement("ImageBB","NUM_NEW_KEYS_CREATED");
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("NumNewKeys");
            int numNewKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("NumNewKeys");
            if (numNewKeysCreated > numNewKeys) {
	              FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All new keys created; returning from addNewKey");
	              return true;
	        }
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("entryCount");
            int entryCount = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("entryCount");
	        entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
            TKey key = (TKey)(object)(entryCount + numNewKeysCreated);
	        checkContainsValueForKey(key, false, "before addNewKey");
            TVal value = GetValue((entryCount + numNewKeysCreated));
            m_region.Add(key,value);

	        return (numNewKeysCreated >= numNewKeys);
        }
        bool putAllNewKey()
        {
            int numNewKeysCreated = (int)Util.BBGet("ImageBB","NUM_NEW_KEYS_CREATED");
	        Util.BBIncrement("ImageBB","NUM_NEW_KEYS_CREATED");
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("NumNewKeys");
	        int numNewKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue( "NumNewKeys" );
                if (numNewKeysCreated > numNewKeys) {
		              FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All new keys created; returning from putAllNewKey");
		              return true;
		        }
                FwkTest<TKey, TVal>.CurrentTest.ResetKey("entryCount");
                int entryCount = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("entryCount");
		        entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
		        TKey key = (TKey)(object)(entryCount + numNewKeysCreated);
                checkContainsValueForKey(key, false, "before addNewKey");
                TVal value = GetValue((entryCount + numNewKeysCreated));
                IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();
                map.Clear();
                map.Add(key, value);
                
                m_region.PutAll(map);
                return (numNewKeysCreated >= numNewKeys);
        }
        bool invalidate()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_INVALIDATE");
            Util.BBIncrement("ImageBB","LASTKEY_INVALIDATE");
	        int firstKey = (int)Util.BBGet("ImageBB", "First_Invalidate");
	        int lastKey = (int)Util.BBGet("ImageBB", "Last_Invalidate");
	        if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All existing keys invalidated; returning from invalidate");
	           return true;
	        }
            TKey key = (TKey)(object)(nextKey);
	        
	        try {
                m_region.Invalidate(key);
	        } catch (EntryNotFoundException e) {
                throw new EntryNotFoundException(e.Message);
	        }
	        return (nextKey >= lastKey);
        }
        bool destroy()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_DESTROY");
	        Util.BBIncrement("ImageBB","LASTKEY_DESTROY");
	        int firstKey = (int)Util.BBGet("ImageBB", "First_Destroy");
	        int lastKey = (int)Util.BBGet("ImageBB", "Last_Destroy");
	        if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All destroys completed; returning from destroy");
	           return true;
	        }
            TKey key = (TKey)(object)(nextKey);
	        try{
                m_region.Remove(key);
            } catch (CacheWriterException e) {
              throw new CacheWriterException(e.Message);
            } catch (EntryNotFoundException e) {
              throw new EntryNotFoundException(e.Message);
            }
	        return (nextKey >= lastKey);
        }
        bool updateExistingKey()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_UPDATE_EXISTING_KEY");
	        Util.BBIncrement("ImageBB","LASTKEY_UPDATE_EXISTING_KEY");
	        int firstKey = (int)Util.BBGet("ImageBB", "First_UpdateExistingKey");
	        int lastKey = (int)Util.BBGet("ImageBB", "Last_UpdateExistingKey");
	        if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All existing keys updated; returning from updateExistingKey");
	           return true;
	        }
            TKey key = (TKey)(object)(nextKey);
	        TVal existingValue = m_region[key];
	              if (existingValue == null)
	    	          throw new Exception("Get of key "+ key + " returned unexpected null");
                  if (existingValue.GetType() == typeof(string))
	    	          throw new Exception("Trying to update a key which was already updated: " + existingValue);
                  TVal newValue = GetValue(("updated_" + nextKey));

            m_region[key] = newValue;
	        return (nextKey >= lastKey);
        }
        bool get()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_GET");
	        Util.BBIncrement("ImageBB","LASTKEY_GET");
	        int firstKey = (int)Util.BBGet("ImageBB", "First_Get");
	        int lastKey = (int)Util.BBGet("ImageBB", "Last_Get");
	        if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All gets completed; returning from get");
	           return true;
	        }
            TKey key = (TKey)(object)(nextKey);
            TVal existingValue = m_region[key];
	        if (existingValue == null)
                FwkTest<TKey, TVal>.CurrentTest.FwkSevere("Get of key " + key + " returned unexpected " + existingValue);
	        return (nextKey >= lastKey);
        }
        bool localInvalidate()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_LOCAL_INVALIDATE");
	        Util.BBIncrement("ImageBB","LASTKEY_LOCAL_INVALIDATE");
	        int firstKey = (int)Util.BBGet("ImageBB", "First_LocalInvalidate");
	        int lastKey = (int)Util.BBGet("ImageBB", "Last_LocalInvalidate");
	        if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All local invalidates completed; returning from localInvalidate");
	           return true;
	        }
            TKey key = (TKey)(object)(nextKey);
	        try {
                m_region.GetLocalView().Invalidate(key);
	        } catch (EntryNotFoundException e) {
	              throw new EntryNotFoundException(e.Message);
	        }

	        return (nextKey >= lastKey);
        }
        bool localDestroy()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_LOCAL_DESTROY");
            Util.BBIncrement("ImageBB","LASTKEY_LOCAL_DESTROY");
	        int firstKey = (int)Util.BBGet("ImageBB", "First_LocalDestroy");
	        int lastKey = (int)Util.BBGet("ImageBB", "Last_LocalDestroy");
	        if(!((nextKey >= firstKey) && (nextKey <= lastKey))) {
		        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All local destroys completed; returning from localDestroy");
	           return true;
	        }
            TKey key = (TKey)(object)(nextKey);
	        try {
                m_region.GetLocalView().Remove(key);
            } catch (EntryNotFoundException e) {
	          throw new EntryNotFoundException(e.Message);
            }
	        return (nextKey >= lastKey);
        }

        
    }

  //------------------------------------DoOpsTask end --------------------------

  //------------------------------ ExpectedRegionContents Start----------------
    public class ExpectedRegionContents
    {

        // instance fields
        // KeyIntevals.NONE
        private bool m_containsKey_none;            // expected value of m_containsKey for KeyIntervals.NONE
        private bool m_containsValue_none;          // expected value of m_containsValue for KeyIntervals.NONE
        private bool m_getAllowed_none;             // if true, then check the value with a get

        // KeyIntevals.INVALIDATE
        private bool m_containsKey_invalidate;
        private bool m_containsValue_invalidate;
        private bool m_getAllowed_invalidate;

        // KeyIntevals.LOCAL_INVALIDATE
        private bool m_containsKey_localInvalidate;
        private bool m_containsValue_localInvalidate;
        private bool m_getAllowed_localInvalidate;

        // KeyIntevals.DESTROY
        private bool m_containsKey_destroy;
        private bool m_containsValue_destroy;
        private bool m_getAllowed_destroy;

        // KeyIntevals.LOCAL_DESTROY
        private bool m_containsKey_localDestroy;
        private bool m_containsValue_localDestroy;
        private bool m_getAllowed_localDestroy;

        // KeyIntevals.UPDATE
        private bool m_containsKey_update;
        private bool m_containsValue_update;
        private bool m_getAllowed_update;
        private bool m_valueIsUpdated;

        // KeyIntervals.GET
        private bool m_containsKey_get;
        private bool m_containsValue_get;
        private bool m_getAllowed_get;

        // new keys
        private bool m_containsKey_newKey;
        private bool m_containsValue_newKey;
        private bool m_getAllowed_newKey;

        // region size specifications
        int m_exactSize;
        int m_minSize;
        int m_maxSize;

        // constructors
        public ExpectedRegionContents(bool m_containsKey, bool m_containsValue, bool m_getAllowed)
        {
            m_containsKey_none = m_containsKey;
            m_containsKey_invalidate = m_containsKey;
            m_containsKey_localInvalidate = m_containsKey;
            m_containsKey_destroy = m_containsKey;
            m_containsKey_localDestroy = m_containsKey;
            m_containsKey_update = m_containsKey;
            m_containsKey_get = m_containsKey;
            m_containsKey_newKey = m_containsKey;

            m_containsValue_none = m_containsValue;
            m_containsValue_invalidate = m_containsValue;
            m_containsValue_localInvalidate = m_containsValue;
            m_containsValue_destroy = m_containsValue;
            m_containsValue_localDestroy = m_containsValue;
            m_containsValue_update = m_containsValue;
            m_containsValue_get = m_containsValue;
            m_containsValue_newKey = m_containsValue;

            m_getAllowed_none = m_getAllowed;
            m_getAllowed_invalidate = m_getAllowed;
            m_getAllowed_localInvalidate = m_getAllowed;
            m_getAllowed_destroy = m_getAllowed;
            m_getAllowed_localDestroy = m_getAllowed;
            m_getAllowed_update = m_getAllowed;
            m_getAllowed_get = m_getAllowed;
            m_getAllowed_newKey = m_getAllowed;
            m_valueIsUpdated = false;

        }

        public ExpectedRegionContents(bool m_containsKeyNone, bool m_containsValueNone,
                                     bool m_containsKeyInvalidate, bool m_containsValueInvalidate,
                                     bool m_containsKeyLocalInvalidate, bool m_containsValueLocalInvalidate,
                                     bool m_containsKeyDestroy, bool m_containsValueDestroy,
                                     bool m_containsKeyLocalDestroy, bool m_containsValueLocalDestroy,
                                     bool m_containsKeyUpdate, bool m_containsValueUpdate,
                                     bool m_containsKeyGet, bool m_containsValueGet,
                                     bool m_containsKeyNewKey, bool m_containsValueNewKey,
                                     bool m_getAllowed,
                                     bool updated)
        {
            m_containsKey_none = m_containsKeyNone;
            m_containsValue_none = m_containsValueNone;

            m_containsKey_invalidate = m_containsKeyInvalidate;
            m_containsValue_invalidate = m_containsValueInvalidate;

            m_containsKey_localInvalidate = m_containsKeyLocalInvalidate;
            m_containsValue_localInvalidate = m_containsValueLocalInvalidate;

            m_containsKey_destroy = m_containsKeyDestroy;
            m_containsValue_destroy = m_containsValueDestroy;

            m_containsKey_localDestroy = m_containsKeyLocalDestroy;
            m_containsValue_localDestroy = m_containsValueLocalDestroy;

            m_containsKey_update = m_containsKeyUpdate;
            m_containsValue_update = m_containsValueUpdate;

            m_containsKey_get = m_containsKeyGet;
            m_containsValue_get = m_containsValueGet;

            m_containsKey_newKey = m_containsKeyNewKey;
            m_containsValue_newKey = m_containsValueNewKey;

            m_getAllowed_none = m_getAllowed;
            m_getAllowed_invalidate = m_getAllowed;
            m_getAllowed_localInvalidate = m_getAllowed;
            m_getAllowed_destroy = m_getAllowed;
            m_getAllowed_localDestroy = m_getAllowed;
            m_getAllowed_update = m_getAllowed;
            m_getAllowed_get = m_getAllowed;
            m_getAllowed_newKey = m_getAllowed;
            m_valueIsUpdated = updated;
        }

        //================================================================================
        // getter methods
        public bool containsKey_none()
        {
            return m_containsKey_none;
        }
        public bool containsValue_none()
        {
            return m_containsValue_none;
        }
        public bool getAllowed_none()
        {
            return m_getAllowed_none;
        }
        public bool containsKey_invalidate()
        {
            return m_containsKey_invalidate;
        }
        public bool containsValue_invalidate()
        {
            return m_containsValue_invalidate;
        }
        public bool getAllowed_invalidate()
        {
            return m_getAllowed_invalidate;
        }
        public bool containsKey_localInvalidate()
        {
            return m_containsKey_localInvalidate;
        }
        public bool containsValue_localInvalidate()
        {
            return m_containsValue_localInvalidate;
        }
        public bool getAllowed_localInvalidate()
        {
            return m_getAllowed_localInvalidate;
        }
        public bool containsKey_destroy()
        {
            return m_containsKey_destroy;
        }
        public bool containsValue_destroy()
        {
            return m_containsValue_destroy;
        }
        public bool getAllowed_destroy()
        {
            return m_getAllowed_destroy;
        }
        public bool containsKey_localDestroy()
        {
            return m_containsKey_localDestroy;
        }
        public bool containsValue_localDestroy()
        {
            return m_containsValue_localDestroy;
        }
        public bool getAllowed_localDestroy()
        {
            return m_getAllowed_localDestroy;
        }
        public bool containsKey_update()
        {
            return m_containsKey_update;
        }
        public bool containsValue_update()
        {
            return m_containsValue_update;
        }
        public bool getAllowed_update()
        {
            return m_getAllowed_update;
        }
        public bool valueIsUpdated()
        {
            return m_valueIsUpdated;
        }
        public bool containsKey_get()
        {
            return m_containsKey_get;
        }
        public bool containsValue_get()
        {
            return m_containsValue_get;
        }
        public bool getAllowed_get()
        {
            return m_getAllowed_get;
        }
        public bool containsKey_newKey()
        {
            return m_containsKey_newKey;
        }
        public bool containsValue_newKey()
        {
            return m_containsValue_newKey;
        }
        public bool getAllowed_newKey()
        {
            return m_getAllowed_newKey;
        }
        public int exactSize()
        {
            return m_exactSize;
        }
        public int minSize()
        {
            return m_minSize;
        }
        public int maxSize()
        {
            return m_maxSize;
        }

        //================================================================================
        // setter methods

        public void containsKey_none(bool abool)
        {
            m_containsKey_none = abool;
        }

        public void containsValue_none(bool abool)
        {
            m_containsValue_none = abool;
        }

        public void containsKey_invalidate(bool abool)
        {
            m_containsKey_invalidate = abool;
        }

        public void containsValue_invalidate(bool abool)
        {
            m_containsValue_invalidate = abool;
        }

        public void containsKey_localInvalidate(bool abool)
        {
            m_containsKey_localInvalidate = abool;
        }

        public void containsValue_localInvalidate(bool abool)
        {
            m_containsValue_localInvalidate = abool;
        }

        public void containsKey_destroy(bool abool)
        {
            m_containsKey_destroy = abool;
        }

        public void containsValue_destroy(bool abool)
        {
            m_containsValue_destroy = abool;
        }

        public void containsKey_localDestroy(bool abool)
        {
            m_containsKey_localDestroy = abool;
        }

        public void containsValue_localDestroy(bool abool)
        {
            m_containsValue_localDestroy = abool;
        }

        public void containsKey_update(bool abool)
        {
            m_containsKey_update = abool;
        }

        public void containsValue_update(bool abool)
        {
            m_containsValue_update = abool;
        }

        public void containsKey_get(bool abool)
        {
            m_containsKey_get = abool;
        }

        public void containsValue_get(bool abool)
        {
            m_containsValue_get = abool;
        }

        public void containsKey_newKey(bool abool)
        {
            m_containsKey_newKey = abool;
        }

        public void containsValue_newKey(bool abool)
        {
            m_containsValue_newKey = abool;
        }

        public void exactSize(int anInt)
        {
            m_exactSize = anInt;
        }

        public void minSize(int anInt)
        {
            m_minSize = anInt;
        }

        public void maxSize(int anInt)
        {
            m_maxSize = anInt;
        }
        public void valueIsUpdated(bool abool)
        {
            m_valueIsUpdated = abool;
        }

        public string toString() {
            StringBuilder attrsSB = new StringBuilder();
            attrsSB.Append(Environment.NewLine + "m_containsKey_none: " + m_containsKey_none);
            attrsSB.Append(Environment.NewLine + "m_containsValue_none: " + m_containsValue_none);
            attrsSB.Append(Environment.NewLine + "m_containsKey_invalidate: " + m_containsKey_invalidate);
            attrsSB.Append(Environment.NewLine + "m_containsValue_invalidate: " + m_containsValue_invalidate);
            attrsSB.Append(Environment.NewLine + "m_containsKey_localInvalidate: " + m_containsKey_localInvalidate);
            attrsSB.Append(Environment.NewLine + "m_containsValue_localInvalidate: " + m_containsValue_localInvalidate);
            attrsSB.Append(Environment.NewLine + "m_containsKey_destroy: " + m_containsKey_destroy);
            attrsSB.Append(Environment.NewLine + "m_containsValue_destroy: " + m_containsValue_destroy);
            attrsSB.Append(Environment.NewLine + "m_containsKey_localDestroy: " + m_containsKey_localDestroy);
            attrsSB.Append(Environment.NewLine + "m_containsValue_localDestroy: " + m_containsValue_localDestroy);
            attrsSB.Append(Environment.NewLine + "m_containsKey_update: " + m_containsKey_update);
            attrsSB.Append(Environment.NewLine + "m_containsValue_update: " + m_containsValue_update);
            attrsSB.Append(Environment.NewLine + "m_containsKey_get: " + m_containsKey_get);
            attrsSB.Append(Environment.NewLine + "m_containsValue_get: " + m_containsValue_get);
            attrsSB.Append(Environment.NewLine + "m_containsKey_newKey: " + m_containsKey_newKey);
            attrsSB.Append(Environment.NewLine + "m_containsValue_newKey: " + m_containsValue_newKey);
            attrsSB.Append(Environment.NewLine );
            return attrsSB.ToString();

        }
    }

//================================================================================
  public class CacheServer<TKey, TVal> : FwkTest<TKey, TVal>
  {
    #region Private constants and statics

    private const UInt32 QueryResponseTimeout = 600;
    private const string RegionName = "regionName";
    private const string ValueSizes = "valueSizes";
    private const string OpsSecond = "opsSecond";
    private const string EntryCount = "entryCount";
    private const string WorkTime = "workTime";
    private const string EntryOps = "entryOps";
    private const string LargeSetQuery = "largeSetQuery";
    private const string UnsupportedPRQuery = "unsupportedPRQuery";
    private const string ObjectType = "objectType";
    private const int NUM_EXTRA_KEYS = 100;
    
   // private const bool MultiRegion = "multiRegion";

    private static Dictionary<string, int> OperationsMap =
      new Dictionary<string, int>();
    private static Dictionary<string, int> ExceptionsMap =
      new Dictionary<string, int>();
    private static bool m_istransaction = false;
    private static bool isSerialExecution = false;
    protected static bool isEmptyClient = false;  // true if this is a bridge client with empty dataPolicy
    protected static bool isThinClient = false; // true if this is a bridge client with eviction to keep it small
    private static Dictionary<TKey, TVal> regionSnapshot = null;
    //  new Dictionary<TKey, TVal>();
    private static List<TKey> destroyedKeys = null;// = new List<TKey>();
    //private static List<TKey> destroyList = null;
    private int keyCount;
    private static List<TKey> m_KeysA = new List<TKey>();
    private static ExpectedRegionContents[] expectedRgnContents = new ExpectedRegionContents[5];
    private static int totalNumKeys = 0;
    //Assembly m_pdxVersionOneAsm;
    //Assembly m_pdxVersionTwoAsm;
    //protected TKey[] m_KeysA;
    #endregion
      /*
       Util.BBSet("OpsBB","CREATES",creates);
      Util.BBSet("OpsBB","UPDATES",puts);
      Util.BBSet("OpsBB","DESTROYS",dests);
      Util.BBSet("OpsBB","GETS",gets);
      Util.BBSet("OpsBB","INVALIDATES",invals);
       
       
       */
      public const string OPSBB="OpsBB";
      public const string CREATE = "CREATE";
      public const string UPDATES = "UPDATES";
      public const string DESTROYS = "DESTROYS";
      public const string GETS = "GETS";
      public const string INVALIDATES = "INVALIDATES";
      
    #region Private utility methods

    private IRegion<TKey, TVal> GetRegion()
    {
      return GetRegion(null);
    }

    protected IRegion<TKey, TVal> GetRegion(string regionName)
    {
      IRegion<TKey, TVal> region;
      if (regionName == null)
      {
        regionName = GetStringValue("regionName");
      }
      if (regionName == null)
      {
        region = (IRegion<TKey, TVal>)GetRootRegion();
        if (region == null)
        {
          IRegion<TKey, TVal>[] rootRegions = CacheHelper<TKey, TVal>.DCache.RootRegions<TKey, TVal>();
          if (rootRegions != null && rootRegions.Length > 0)
          {
            region = rootRegions[Util.Rand(rootRegions.Length)];
          }
        }
      }
      else
      {
        region = CacheHelper<TKey, TVal>.GetRegion(regionName);
      }
      return region;
    }

    private void AddValue(IRegion<TKey, TVal> region, int count, byte[] valBuf)
    {
      if (region == null)
      {
        FwkSevere("CacheServer::AddValue(): No region to perform add on.");
        return;
      }
      TKey key = (TKey)(object)count.ToString();
      TVal value = (TVal)(object)valBuf;
      BitConverter.GetBytes(count).CopyTo(valBuf, 0);
      BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
      try
      {
        region.Add(key, value);
        //FwkInfo("key: {0}  value: {1}", key, Encoding.ASCII.GetString(value.Value));
      }
      catch (Exception ex)
      {
        FwkException("CacheServer.AddValue() caught Exception: {0}", ex);
      }
    }

    private TKey GetKey(int max)
    {
      ResetKey(ObjectType);
      string objectType = GetStringValue(ObjectType);
      QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
      int numSet = 0;
      int setSize = 0;
      if (objectType != null && (objectType == "Portfolio" || objectType == "PortfolioPdx"))
      {
        setSize = qh.PortfolioSetSize;
        numSet = max / setSize;
        return (TKey)(object)String.Format("port{0}-{1}", Util.Rand(numSet), Util.Rand(setSize));
      }
      else if (objectType != null && (objectType == "Position" || objectType == "PositionPdx"))
      {
        setSize = qh.PositionSetSize;
        numSet = max / setSize;
        return (TKey)(object)String.Format("pos{0}-{1}", Util.Rand(numSet), Util.Rand(setSize));
      }
      return (TKey)(object)Util.Rand(max).ToString();
    }

    private TVal GetUserObject(string objType)
    {
      TVal usrObj = default(TVal);
      ResetKey(EntryCount);
      int numOfKeys = GetUIntValue(EntryCount);
      ResetKey(ValueSizes);
      int objSize = GetUIntValue(ValueSizes);
      QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
      int numSet = 0;
      int setSize = 0;
      if (objType != null && objType == "Portfolio")
      {
        setSize = qh.PortfolioSetSize;
        numSet = numOfKeys / setSize;
        usrObj = (TVal)(object) new Portfolio(Util.Rand(setSize), objSize);
      }
      else if (objType != null && objType == "Position")
      {
        setSize = qh.PositionSetSize;
        numSet = numOfKeys / setSize;
        int numSecIds = Portfolio.SecIds.Length;
        usrObj = (TVal)(object)new Position(Portfolio.SecIds[setSize % numSecIds], setSize * 100);
      }
      if (objType != null && objType == "PortfolioPdx")
      {
        setSize = qh.PortfolioSetSize;
        numSet = numOfKeys / setSize;
        usrObj = (TVal)(object)new PortfolioPdx(Util.Rand(setSize), objSize);
      }
      else if (objType != null && objType == "PositionPdx")
      {
        setSize = qh.PositionSetSize;
        numSet = numOfKeys / setSize;
        int numSecIds = PortfolioPdx.SecIds.Length;
        usrObj = (TVal)(object)new PositionPdx(PortfolioPdx.SecIds[setSize % numSecIds], setSize * 100);
      }
      return usrObj;
    }

    private bool AllowQuery(QueryCategory category, bool haveLargeResultset,
      bool islargeSetQuery, bool isUnsupportedPRQuery)
    {
      if (category == QueryCategory.Unsupported)
      {
        return false;
      }
      else if (haveLargeResultset != islargeSetQuery)
      {
        return false;
      }
      else if (isUnsupportedPRQuery &&
               ((category == QueryCategory.MultiRegion) ||
                (category == QueryCategory.NestedQueries)))
      {
        return false;
      }
      else
      {
        return true;
      }
    }
    private void remoteQuery(QueryStrings currentQuery, bool isLargeSetQuery, 
      bool isUnsupportedPRQuery, int queryIndex,bool isparam,bool isStructSet)
    {
      DateTime startTime;
      DateTime endTime;
      TimeSpan elapsedTime;
      QueryService<TKey, object> qs = CheckQueryService();
      if (AllowQuery(currentQuery.Category, currentQuery.IsLargeResultset,
            isLargeSetQuery, isUnsupportedPRQuery))
        {
          string query = currentQuery.Query;
          FwkInfo("CacheServer.RunQuery: ResultSet Query Category [{0}], " +
            "String [{1}].", currentQuery.Category, query);
          Query<object> qry = qs.NewQuery(query);
          object[] paramList = null;
          if (isparam)
          {
            Int32 numVal = 0;
            if (isStructSet)
            {
              paramList = new object[QueryStatics.NoOfQueryParamSS[queryIndex]];

              for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[queryIndex]; ind++)
              {
                try
                {
                  numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[queryIndex][ind]);
                  paramList[ind] = numVal;
                }
                catch (FormatException)
                {
                  paramList[ind] = (System.String)QueryStatics.QueryParamSetSS[queryIndex][ind];
                }
              }
            }
            else
            {
              paramList = new object[QueryStatics.NoOfQueryParam[queryIndex]];
              for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[queryIndex]; ind++)
              {
                try
                {
                  numVal = Convert.ToInt32(QueryStatics.QueryParamSet[queryIndex][ind]);
                  paramList[ind] = numVal;
                }
                catch (FormatException)
                {
                  paramList[ind] = (System.String)QueryStatics.QueryParamSet[queryIndex][ind];
                }
              }
            }
          }
          ISelectResults<object> results = null;
          startTime = DateTime.Now;
          if (isparam)
          {
            results = qry.Execute(paramList,600);
          }
          else
          {
            results = qry.Execute(600);
          }
          endTime = DateTime.Now;
          elapsedTime = endTime - startTime;
          FwkInfo("CacheServer.RunQuery: Time Taken to execute" +
            " the query [{0}]: {1}ms", query, elapsedTime.TotalMilliseconds);
        }
    }

    private void RunQuery()
    {
      FwkInfo("In CacheServer.RunQuery");

      try
      {
        ResetKey(EntryCount);
        int numOfKeys = GetUIntValue(EntryCount);
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
        int setSize = qh.PortfolioSetSize;
        if (numOfKeys < setSize)
        {
          setSize = numOfKeys;
        }
        int i = Util.Rand(QueryStrings.RSsize);
        ResetKey(LargeSetQuery);
        ResetKey(UnsupportedPRQuery);
        bool isLargeSetQuery = GetBoolValue(LargeSetQuery);
        bool isUnsupportedPRQuery = GetBoolValue(UnsupportedPRQuery);
        QueryStrings currentQuery = QueryStatics.ResultSetQueries[i];
        remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i,false,false);
        i = Util.Rand(QueryStrings.SSsize);
        int[] a = new int[] { 4, 6, 7, 9, 12, 14, 15, 16 };
        if ((typeof(TVal).Equals(typeof(GemStone.GemFire.Cache.Tests.NewAPI.PortfolioPdx)) ||
          (typeof(TVal).Equals(typeof(GemStone.GemFire.Cache.Tests.NewAPI.PositionPdx)))) && ((IList<int>)a).Contains(i))
        {
          FwkInfo("Skiping Query for pdx object [{0}]", QueryStatics.StructSetQueries[i]);
        }
        else
        {
          currentQuery = QueryStatics.StructSetQueries[i];
          remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, false, false);
        }
        i = Util.Rand(QueryStrings.RSPsize);
        currentQuery = QueryStatics.ResultSetParamQueries[i];
        remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i,true,false);
        i = Util.Rand(QueryStrings.SSPsize);
        if ((typeof(TVal).Equals(typeof(GemStone.GemFire.Cache.Tests.NewAPI.PortfolioPdx)) ||
          (typeof(TVal).Equals(typeof(GemStone.GemFire.Cache.Tests.NewAPI.PositionPdx)))) && ((IList<int>)a).Contains(i))
        {
          FwkInfo("Skiping Query for pdx object [{0}]", QueryStatics.StructSetParamQueries[i]);
        }
        else
        {
          currentQuery = QueryStatics.StructSetParamQueries[i];
          remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, true, true);
        }
      }
      catch (Exception ex)
      {
        FwkException("CacheServer.RunQuery: Caught Exception: {0}", ex);
      }
      FwkInfo("CacheServer.RunQuery complete.");
    }

    private void UpdateOperationsMap(string opCode, int numOps)
    {
      UpdateOpsMap(OperationsMap, opCode, numOps);
    }

    private void UpdateExceptionsMap(string opCode, int numOps)
    {
      UpdateOpsMap(ExceptionsMap, opCode, numOps);
    }

    private void UpdateOpsMap(Dictionary<string, int> map, string opCode,
      int numOps)
    {
      lock (((ICollection)map).SyncRoot)
      {
        int currentOps;
        if (!map.TryGetValue(opCode, out currentOps))
        {
          currentOps = 0;
        }
        map[opCode] = currentOps + numOps;
      }
    }

    private int GetOpsFromMap(Dictionary<string, int> map, string opCode)
    {
      int numOps;
      lock (((ICollection)map).SyncRoot)
      {
        if (!map.TryGetValue(opCode, out numOps))
        {
          numOps = 0;
        }
      }
      return numOps;
    }
    private void PutAllOps()
    {
      IRegion<TKey,TVal> region = GetRegion();
      IDictionary<TKey, TVal> map = new Dictionary<TKey,TVal>();
      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);
      string valBuf = null;
      ResetKey(ObjectType);
      string objectType = GetStringValue(ObjectType);
      if (objectType != null)
      {
        Int32 numSet = 0;
        Int32 setSize = 0;
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
        TVal port;
        setSize = qh.PortfolioSetSize;
        numSet = 200 / setSize;
        for (int set = 1; set <= numSet; set++)
        {
          for (int current = 1; current <= setSize; current++)
          {
            if (objectType != "PortfolioPdx")
            {
              port = (TVal)(object)new Portfolio(current, valSize);
            }
            else
            {
              port = (TVal)(object)new PortfolioPdx(current, valSize);
            }
            string Id = String.Format("port{0}-{1}", set,current); 
            TKey key = (TKey)(object)Id.ToString();
            map.Add(key, port);
          }
        }
      }
      else
      {
        valBuf = new string('A', valSize);
        for (int count = 0; count < 200; count++)
        {
          TKey key = (TKey)(object)count.ToString();
          TVal value = (TVal)(object)Encoding.ASCII.GetBytes(valBuf);
          map.Add(key, value);
        }
      }
      region.PutAll(map,60);
    }

    private void GetAllOps()
    {
      IRegion<TKey, TVal> region = GetRegion();
      List<TKey> keys  = new List<TKey>();
      keys.Clear();
      for (int count = 0; count < 200; count++)
      {
        TKey key = (TKey)(object)count.ToString();
        keys.Add(key);
      }    
      IDictionary<TKey, TVal> values = new Dictionary<TKey, TVal>();
      values.Clear();
      region.GetAll(keys.ToArray(), values, null, false);
    }
    #endregion

    #region Public methods
    public static ICacheListener<TKey, TVal> CreateSilenceListener()
    {
      return new SilenceListener<TKey, TVal>();
    }
    public virtual void DoCreatePool()
    {
      FwkInfo("In DoCreatePool()");
      try
      {
        CreatePool();
      }
      catch (Exception ex)
      {
        FwkException("DoCreatePool() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreatePool() complete.");
    }
   
    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try {
        IRegion<TKey, TVal> region = CreateRootRegion();
        ResetKey("useTransactions");
        m_istransaction = GetBoolValue("useTransactions");
        isEmptyClient = !(region.Attributes.CachingEnabled);
        isThinClient = region.Attributes.CachingEnabled;
        if (region == null) {
          FwkException("DoCreateRegion()  could not create region.");
        }
        
        FwkInfo("DoCreateRegion()  Created region '{0}'", region.Name);
      } catch (Exception ex) {
        FwkException("DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreateRegion() complete.");
    }

    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      CacheHelper<TKey, TVal>.Close();
    }
    public void DoRegisterAllKeys()
    {
      FwkInfo("In DoRegisterAllKeys()");
      try
      {
          IRegion<TKey, TVal> region = GetRegion();
          FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
          bool isDurable = GetBoolValue("isDurableReg");
          ResetKey("getInitialValues");
          bool isGetInitialValues = GetBoolValue("getInitialValues");
          bool checkReceiveVal = GetBoolValue("checkReceiveVal");
          bool isReceiveValues = true;
          if (checkReceiveVal)
          {
              ResetKey("receiveValue");
              isReceiveValues = GetBoolValue("receiveValue");
          }
          ICollection<TKey> keys = new List<TKey>();
          ResetKey("sleepBeforeRegisterInterest");
          int sleepTime = GetUIntValue("sleepBeforeRegisterInterest");
          sleepTime = sleepTime > 0 ? sleepTime : 0;
          FwkInfo("Sleeping for " + sleepTime + " millis");
          Thread.Sleep(sleepTime);
          region.GetSubscriptionService().RegisterAllKeys(isDurable, keys, isGetInitialValues, isReceiveValues);
          String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
          if (durableClientId.Length > 0)
          {
              CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
          }
      }
      catch (Exception ex)
      {
          FwkException("DoRegisterAllKeys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterAllKeys() complete.");
    }
    public void DoFeed()
    {
      FwkInfo("CacheServer.DoFeed() called.");

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int cnt = 0;
      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);
      string valBuf = new string('A', valSize);

      IRegion<TKey,TVal> region = GetRegion();
      PaceMeter pm = new PaceMeter(opsSec);
      while (cnt++ < entryCount)
      {
        AddValue(region, cnt, Encoding.ASCII.GetBytes(valBuf));
        pm.CheckPace();
      }
    }
    protected void removeDuplicates(List<TKey> destroyedKeys)
    {
      List<TKey> myStringList = new List<TKey>();
      foreach (TKey s in destroyedKeys)
      {
        if (!myStringList.Contains(s))
        {
          myStringList.Add(s);
        }
      }
      destroyedKeys = myStringList;
      //return myStringList;
    }
    // Tx code start
   
    protected void verifyContainsKey(IRegion<TKey, TVal> m_region,TKey key, bool expected)
    {
      //bool containsKey = m_region.GetLocalView().ContainsKey(key);

      bool containsKey = false;
      if (isEmptyClient || m_istransaction)
        containsKey = m_region.ContainsKey(key);
      else
        containsKey = m_region.GetLocalView().ContainsKey(key);
      if (containsKey != expected)
      {
        throw new Exception("Expected ContainsKey() for " + key + " to be " + expected +
                  " in " + m_region.FullPath + ", but it is " + containsKey);
      }
    }
    
    protected void verifyContainsValueForKey(IRegion<TKey, TVal> m_region,TKey key, bool expected)
    {
      
      //bool containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
      bool containsValueForKey = false;
      if (isEmptyClient ||m_istransaction)
        containsValueForKey = m_region.ContainsValueForKey(key);
      else
        containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
      if (containsValueForKey != expected)
      {
        throw new Exception("Expected ContainsValueForKey() for " + key + " to be " + expected +
                  " in " + m_region.FullPath + ", but it is " + containsValueForKey);
      }
       
    }
    protected void verifySize(IRegion<TKey, TVal> m_region,int expectedSize)
    {
      int size = 0;
      if (isEmptyClient || m_istransaction)
        size = m_region.Count;
      else
        size = m_region.GetLocalView().Count;
      if (size != expectedSize)
      {
        if (size < 1000)
        {
          StringBuilder sb = new StringBuilder();
          sb.Append("region has wrong size (").Append(size)
            .Append(").  Dump of region follows:")
            .Append("\n");
          ICollection<RegionEntry<TKey, TVal>> regionentry = m_region.GetEntries(false);
          foreach (RegionEntry<TKey, TVal> kvp in regionentry)
         {
           TKey key = kvp.Key;
           TVal Value = kvp.Value;
            sb.Append(key).Append(") -> ").Append(Value)
               .Append("\n");
          }
          FwkInfo(sb.ToString());
        }
        throw new Exception("Expected size of " + m_region.FullPath + " to be " +
           expectedSize + ", but it is " + size);
      }
      /*
      int size = m_region.Count;
      if (size != expectedSize)
      {
        throw new Exception("Expected size of " + m_region.FullPath + " to be " +
           expectedSize + ", but it is " + size);
      }*/
    }
    protected TKey GetNewKey()
    {
      ResetKey("distinctKeys");
      int numKeys = GetUIntValue("distinctKeys");
      keyCount = Util.BBIncrement("uniqueKey", "count");
      TKey key = default(TKey);
      if (typeof(TKey) == typeof(string))
      {
          String keybuf = String.Format("Key-{0}-{1}-{2}", Util.PID, Util.ThreadID, keyCount);
          key = (TKey)(object)(keybuf);
      }
      else
      {
          key = (TKey)(object)(keyCount);
      }
      return key;
    }
    protected TKey GetExistingKey(bool useServerKeys)
    {
      IRegion<TKey, TVal> region = GetRegion();
      TKey key = default(TKey);
      //if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
      //{
        if (useServerKeys)
        {
          int size = region.Count;
          TKey[] keys = (TKey[])region.Keys;
          key = keys[Util.Rand(0, keys.Length)];
        }
        else
        {
          int size = region.GetLocalView().Count;
          TKey[] keys = (TKey[])region.GetLocalView().Keys;
          key = keys[Util.Rand(0, size)];
        }
      /*}
      else
      {
        return key;
      }*/
      keyCount = Util.BBIncrement("uniqueKey", "count");
      return key;
    }
    protected TKey addEntry(IRegion<TKey, TVal> m_region)
    {
      TKey key = GetNewKey();
      TVal value = GetValue();
      int beforeSize = 0;
      if (isEmptyClient || m_istransaction)
        beforeSize = m_region.Count;
      else
        beforeSize = m_region.GetLocalView().Count;
      try
      {
        m_region.Add(key, value);
        
      }
      catch (EntryExistsException ex)
      {
        if (isSerialExecution)
        {
          // cannot get this exception; nobody else can have this key
          throw new Exception(ex.StackTrace);
        }
        else
        {
          Util.Log("Caught {0} (expected with concurrent execution); continuing with test", ex);
        }
      }
      catch (Exception ex)
      {
         throw new Exception(ex.Message);
      }
      // validation
      if (isSerialExecution)
      {
        if (isEmptyClient)
        {
          if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
          {
            verifySize(m_region,0);
          }
        }
        else if (isThinClient)
        { 
          if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
          {
            // new entry should be in the local region
            verifyContainsKey(m_region,key, true);
            verifyContainsValueForKey(m_region,key, true);
          }
        }
        else
        { // region has all keys/values
          verifyContainsKey(m_region,key, true);
          verifyContainsValueForKey(m_region,key, true);
          verifySize(m_region,beforeSize + 1);
        }
        regionSnapshot[key] = value;
        destroyedKeys.Remove(key);
      }
      return key;

    }
    protected void updateEntry(IRegion<TKey, TVal> r)
    {
      TKey key = GetExistingKey(isEmptyClient||isThinClient);
      if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
      {
        int size = r.Count;
        if (isSerialExecution && (size != 0))
          throw new Exception("getExistingKey returned " + key + ", but region size is " + size);
        FwkInfo("updateEntry: No keys in region");
        return;
      }
      int beforeSize = 0;
      if (isEmptyClient || m_istransaction)
        beforeSize = r.Count;
      else
        beforeSize = r.GetLocalView().Count;
      TVal value = GetValue();
      r[key] = value;
      
      // validation
      // cannot validate return value from put due to bug 36436; in peer configurations
      // we do not make any guarantees about the return value
      if (isSerialExecution)
      {
        if (isEmptyClient)
        {
          if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
          {
            verifySize(r, 0);
          }
        }
        else if (isThinClient)
        { // we have eviction
          if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
          {
            verifyContainsKey(r, key, true);
            verifyContainsValueForKey(r, key, true);
           
          }
        }
        else
        { // we have all keys/values
          verifyContainsKey(r, key, true);
          verifyContainsValueForKey(r, key, true);
          verifySize(r, beforeSize);
        }

        // record the current state
        regionSnapshot[key] = value;
        destroyedKeys.Remove(key);
      }
    }
    protected void invalidateEntry(IRegion<TKey, TVal> m_region,bool isLocalInvalidate)
    {
      int beforeSize = 0;
      if (isEmptyClient || m_istransaction)
        beforeSize = m_region.Count;
      else
        beforeSize = m_region.GetLocalView().Count;
      TKey key = GetExistingKey(isEmptyClient||isThinClient);
      if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
      {
        if (isSerialExecution && (beforeSize != 0))
          throw new Exception("getExistingKey returned " + key + ", but region size is " + beforeSize);
        FwkInfo("invalidateEntry: No keys in region");
      }
      bool containsKey = m_region.GetLocalView().ContainsKey(key);
      bool containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
      FwkInfo("containsKey for " + key + ": " + containsKey);
      FwkInfo("containsValueForKey for " + key + ": " + containsValueForKey);
      try
      {
        if (isLocalInvalidate)
        { // do a local invalidate
          m_region.GetLocalView().Invalidate(key);
        }
        else
        { // do a distributed invalidate
          m_region.Invalidate(key);
        }

        // validation
        if (isSerialExecution)
        {
          if (isEmptyClient)
          {
            if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
            {
              verifySize(m_region,0);
            }
          }
          else if (isThinClient)
          { // we have eviction
            if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
            {
              verifySize(m_region,beforeSize);
            }
          }
          else
          { // region has all keys/values
            verifyContainsKey(m_region,key, true);
            verifyContainsValueForKey(m_region,key, false);
            verifySize(m_region,beforeSize);
          }
          regionSnapshot[key] = default(TVal);
          //regionSnapshot.Remove(key);
          destroyedKeys.Remove(key);
        }
      }
      catch (EntryNotFoundException e)
      {
        if (isSerialExecution)
          throw new Exception(e.StackTrace);
        else
        {
          Util.Log("Caught {0} (expected with concurrent execution); continuing with test", e);
          return;
        }
      }
    }
/** Get an existing key in the given region if one is available,
 *  otherwise get a new key. 
 *
 *  @param aRegion The region to use for getting an entry.
 */
    protected void getKey(IRegion<TKey, TVal> aRegion)
    {
      TKey key = GetExistingKey(isEmptyClient || isThinClient);
      if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
      { // no existing keys; get a new key then
        int size = aRegion.Count;
        if (isSerialExecution && (size != 0))
          throw new Exception("getExistingKey returned " + key + ", but region size is " + size);
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("getKey: No keys in region");
        return;
      }
      int beforeSize = 0;
      if (isEmptyClient || m_istransaction)
        beforeSize = aRegion.Count;
      else
        beforeSize = aRegion.GetLocalView().Count;
      bool beforeContainsValueForKey = aRegion.ContainsValueForKey(key);
      bool beforeContainsKey = aRegion.ContainsKey(key);
      TVal anObj = default(TVal);
      try
      {
        anObj = aRegion[key];
      }
      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
      {
        if (!EqualityComparer<TVal>.Default.Equals(anObj, default(TVal)))
        {
          throw new GemStone.GemFire.Cache.Generic.KeyNotFoundException();
        }

      }
      
      // validation 
      if (isSerialExecution)
      {
        if (isEmptyClient)
        {
          if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
          {
            verifySize(aRegion, 0);
          }
        }
        else if (isThinClient)
        { // we have eviction
          if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
          {
            verifyContainsKey(aRegion, key, true);
          }
        }
        else
        { // we have all keys/values
          verifyContainsKey(aRegion, key, true);
          verifyContainsValueForKey(aRegion, key, (beforeContainsValueForKey));

          // check the expected value of the get
          TVal actualValue = anObj;
          TVal expectedValue = default(TVal);
          foreach( KeyValuePair<TKey, TVal> kvp in regionSnapshot )
         {
          TKey mapkey = kvp.Key;

          if (key.Equals(mapkey))
            {
              expectedValue = kvp.Value;
              if (!EqualityComparer<TVal>.Default.Equals(actualValue, default(TVal)))
              {
                if (!actualValue.Equals(expectedValue))
                {
                FwkException("getKey: expected value {0} is not same as actual value {1} for key {2}",
                  expectedValue, actualValue, key);
                }
              } 
            }
          }
          verifySize(aRegion, beforeSize);
        }

        // record the current state
        // in case the get works like a put because there is a cacheLoader
        regionSnapshot[key] = anObj;
        destroyedKeys.Remove(key);
      }
    }

    protected void destroyEntry(IRegion<TKey, TVal> m_region, bool isLocalDestroy)
    {
      TKey key = GetExistingKey(isEmptyClient || isThinClient);
      if (EqualityComparer<TKey>.Default.Equals(key,default(TKey)))
      {
        int size = m_region.Count;
        if (isSerialExecution && (size != 0))
          throw new Exception("getExistingKey returned " + key + ", but region size is " + size);
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("destroyEntry: No keys in region");
        return;
      }
      int beforeSize = 0;
      if (isEmptyClient || m_istransaction)
        beforeSize = m_region.Count;
      else
        beforeSize = m_region.GetLocalView().Count;
      try
      {
        if (isLocalDestroy)
        { // do a local invalidate
          m_region.GetLocalView().Remove(key);
        }
        else
        { // do a distributed invalidate
          m_region.Remove(key);
        }

        // validation
        if (isSerialExecution)
        {
          if (isEmptyClient)
          {
            if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
            {
              verifySize(m_region,0);
            }
          }
          else if (isThinClient)
          { // we have eviction
            if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
            {
              verifyContainsKey(m_region,key, false);
              verifyContainsValueForKey(m_region,key, false);
              int afterSize = m_region.Count;
              if ((afterSize != beforeSize) && (afterSize != beforeSize - 1))
              {
                throw new Exception("Expected region size " + afterSize + " to be either " +
                    beforeSize + " or " + (beforeSize - 1));
              }
            }
          }
          else
          { // region has all keys/values
            verifyContainsKey(m_region,key, false);
            verifyContainsValueForKey(m_region,key, false);
            verifySize(m_region,beforeSize - 1);
          }
          regionSnapshot.Remove(key);
          destroyedKeys.Add(key);
        }
      }
      catch (EntryNotFoundException e)
      {
        if (isSerialExecution)
          throw new Exception(e.StackTrace);
        else
        {
          Util.Log("Caught {0} (expected with concurrent execution); continuing with test", e);
          return;
        }
      }
    }
    protected void putAll(IRegion<TKey, TVal> r) 
    {
   // determine the number of new keys to put in the putAll
   int beforeSize = 0;
   if (isEmptyClient || m_istransaction)
        beforeSize = r.Count;
      else
        beforeSize = r.GetLocalView().Count;
   int localBeforeSize = r.GetLocalView().Count;
   ResetKey("numPutAllExistingKeys");
   ResetKey("numPutAllNewKeys");
   int numNewKeysToPut = GetUIntValue("numPutAllNewKeys");
  // get a map to put
   IDictionary<TKey, TVal> mapToPut = new Dictionary<TKey,TVal>();
      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);
      string valBuf = null;
      valBuf = new string('A', valSize);
      StringBuilder newKeys = new StringBuilder();
        for (int i = 0; i < numNewKeysToPut; i++)
        {
          TKey key = GetNewKey();
          TVal value = GetValue();
          mapToPut[key] = value;
          newKeys.Append(key + " ");
          if ((i % 10) == 0) {
            newKeys.Append("\n");
          }
        }
     

   // add existing keys to the map
   int numPutAllExistingKeys =  GetUIntValue("numPutAllExistingKeys");
   StringBuilder existingKeys = new StringBuilder();
   if (numPutAllExistingKeys > 0) {
         for (int i = 0; i < numPutAllExistingKeys; i++) { // put existing keys
           TKey key = GetExistingKey(isEmptyClient || isThinClient);
           TVal anObj = GetValue();
            mapToPut[key] = anObj;
            existingKeys.Append(key + " ");
            if (((i+1) % 10) == 0) {
               existingKeys.Append("\n");
            }
         }
   }
   FwkInfo("PR size is " + beforeSize + ", local region size is " +
       localBeforeSize + ", map to use as argument to putAll is " + 
       mapToPut.GetType().Name + " containing " + numNewKeysToPut + " new keys and " + 
       numPutAllExistingKeys + " existing keys (updates); total map size is " + mapToPut.Count +
       "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
   

   // do the putAll
   FwkInfo("putAll: calling putAll with map of " + mapToPut.Count + " entries");
   r.PutAll(mapToPut,60);

   FwkInfo("putAll: done calling putAll with map of " + mapToPut.Count + " entries");

   // validation
   if (isSerialExecution) {
     if (isEmptyClient) {
       if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
       {
         verifySize(r, 0);
       }
     }
     else { // we have all keys/values in the local region
      verifySize(r, beforeSize + numNewKeysToPut);
     }
     foreach (KeyValuePair<TKey, TVal> kvp in mapToPut)
     {
       TKey key = kvp.Key;
       TVal value = kvp.Value;
       if (!isEmptyClient && !isThinClient)
       {
         verifyContainsKey(r, key, true);
         verifyContainsValueForKey(r, key, true);
       }
       regionSnapshot[key] = value;
       destroyedKeys.Remove(key);
      } 
    }
}
    public void doEntryOps()
    {
      IRegion<TKey, TVal> region = GetRegion();
      string opcode = null;
      bool rolledback = false;
      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 10 : secondsToRun;

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);
      if (region == null)
      {
        FwkSevere("CacheServer.doEntryOps(): No region to perform operations on.");
        now = end; // Do not do the loop
      }
      int size = 0;
      int create = 0, update = 0, destroy = 0, invalidate = 0, localdestroy = 0, localinvalidate = 0,
        get = 0, putall=0;
      CacheTransactionManager txManager = null;
      while (now < end)
      {
        rolledback = false;
        try
        {
          if (isEmptyClient || m_istransaction)
            size = region.Count;
          else
            size = region.GetLocalView().Count;
        if (m_istransaction)
        {
          txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
          txManager.Begin();
        }
        opcode = GetStringValue("entryOps");
        if (opcode == null) opcode = "no-opcode";
        //int addKeyCount = Util.BBIncrement("CreateOpCount", "CreateKeyCount");
        
        if (((size < 1) && (opcode != "create")) || (opcode == "create"))
        {
          addEntry(region);
          create++;
        }
        else if (opcode == "update")
        {
          updateEntry(region);
          update++;
        }
        else if (opcode == "destroy")
        {
          destroyEntry(region,false);
          destroy++;
          //Util.BBDecrement("CreateOpCount", "CreateKeyCount");
        }
        else if (opcode == "localDestroy")
        {
          destroyEntry(region,true);
          localdestroy++;
        }
        else if (opcode == "invalidate")
        {
          invalidateEntry(region,false);
          invalidate++;
          //Util.BBDecrement("CreateOpCount", "CreateKeyCount");
        }
        else if (opcode == "localInvalidate")
        {
          invalidateEntry(region, true);
          localinvalidate++;
        }
        else if (opcode == "putAll")
        {
          putAll(region);
          putall++;
        }
        else if (opcode == "get")
        {
          getKey(region);
          get++;
        }
        else
        {
          FwkException("CacheServer.doEntryOps() Invalid operation " +
            "specified: {0}", opcode);
        }
        //removeDuplicates(destroyedKeys);
        if (m_istransaction && !rolledback)
        {
          try
          {
            txManager.Commit();
          }
          catch (CommitConflictException)
          {
            // can occur with concurrent execution
            Util.Log("Caught CommitConflictException. Expected with concurrent execution, continuing test.");
          }
          catch (TransactionDataNodeHasDepartedException e)
          {
            throw e;
          }
          catch (Exception ex)
          {
            throw new Exception("Caught unexpected in doEntry : {0}", ex);
          }
        }
        }
        catch (TransactionDataNodeHasDepartedException e)
        {
          if (!m_istransaction)
          {
            throw new Exception("Unexpected Exception : {0}",e);
          }
          else
          {
            FwkInfo("Caught Exception " + e + ".  Expected with HA, continuing test.");
            FwkInfo("Rolling back transaction.");
            try
            {
              txManager.Rollback();
              FwkInfo("Done Rolling back Transaction");
            }
            catch (Exception te)
            {
              FwkInfo("Caught exception {0} on rollback() after catching TransactionDataNodeHasDeparted during tx ops.  Expected, continuing test.",te);
            }
            rolledback = true;
          }
        } 
        catch (Exception ex)
        {
          FwkException("CacheServer.doEntryOps() Caught unexpected " +
            "exception during entry '{0}' operation: {1}.", opcode, ex);
        }
        now = DateTime.Now;
      }
      FwkInfo("DoEntryOP: create = {0}, update = {1}, destroy = {2}, localdestroy = {3}"
         + ",invalidate = {4},localinvalidate = {5}, get = {6}, putall = {7}",
        create, update, destroy, localinvalidate, invalidate, localinvalidate, get, putall);
    }
    public void DoRandomEntryOperation()
    {
      FwkInfo("In DoRandomEntryOperation");
      try
      {
        int timedInterval = GetTimeValue("timedInterval") * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey("distinctKeys");
        int numKeys = GetUIntValue("distinctKeys");
        isSerialExecution = GetBoolValue("serialExecution");
        string clntid = null;
        int roundPosition = 0;
        if (isSerialExecution)
        {
            regionSnapshot = new Dictionary<TKey, TVal>();
            destroyedKeys = new List<TKey>();
            Util.BBSet("RoundPositionBB", "roundPosition", 1);
            roundPosition = (int)Util.BBGet("RoundPositionBB", "roundPosition");
            clntid = String.Format("Client.{0}", roundPosition);
            
        }
        else
        {
            clntid = Util.ClientId;
        }
        int numClients = GetUIntValue("clientCount");
        string dummyClntid = null;
        bool isdone = false;
        Util.BBSet("RoundPositionBB", "done", false);
        while (true)
        {
           if (roundPosition > numClients)
            break;
          try
          {
            if (clntid.Equals(Util.ClientId))
            {
              //RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
              if (isSerialExecution)
              {
                  doEntryOps();
                  DoWaitForSilenceListenerComplete();
                  Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
                  Util.BBSet("DestroyedKeys", "destroyedKeys", destroyedKeys);

                  roundPosition = Util.BBIncrement("RoundPositionBB", "roundPosition");
                  Util.BBSet("RoundPositionBB", "roundPosition", roundPosition);
                  //clntid = String.Format("Client.{0}", roundPosition);
                  Util.BBSet("RoundPositionBB", "done", true);
                  Util.BBSet("RoundPositionBB", "VerifyCnt", 0);
              }
              else
              {
                  doEntryOps();
                  break;
              }
              Thread.Sleep(2000);
            }
            else// if(!Util.ClientId.Equals(dummyClntid))G1326
            {
                for (; ; )
                {
                    isdone = (bool)Util.BBGet("RoundPositionBB", "done");
                    if (isdone)
                        break;
                }
                if (isdone)
                {
                  Thread.Sleep(35000);
                  try
                  {
                      verifyFromSnapshot();
                      Util.BBSet("RoundPositionBB", "done", false);
                      Util.BBIncrement("RoundPositionBB", "VerifyCnt");     
                  }
                  catch (Exception ex)
                    {
                        Util.BBSet("RoundPositionBB", "done", false);
                        Util.BBIncrement("RoundPositionBB", "VerifyCnt");
                        Util.BBSet("RoundPositionBB", "roundPosition", numClients + 1);
                        FwkException("In DoRandomEntryOperation() caught excption {0}.",ex.Message);
                    }
                                 
                }
                Thread.Sleep(100);
              
              
            }
            if (isSerialExecution)
            {
                int verifyCnt = (int)Util.BBGet("RoundPositionBB", "VerifyCnt");
                while (verifyCnt < numClients - 1)
                {
                    verifyCnt = (int)Util.BBGet("RoundPositionBB", "VerifyCnt");
                    Thread.Sleep(100);
                }
                //Util.BBSet("RoundPositionBB", "VerifyCnt", 0);
                roundPosition = (int)Util.BBGet("RoundPositionBB", "roundPosition");
                clntid = String.Format("Client.{0}", roundPosition);
                dummyClntid = String.Format("Client.{0}", (roundPosition - 1));
            }

            Thread.Sleep(3000);
          }
          catch (ClientTimeoutException)
          {
            Util.BBSet("RoundPositionBB", "done", true);
            Util.BBSet("RoundPositionBB", "roundPosition", numClients + 1);
            FwkException("In DoRandomEntryOperation()  Timed run timed out.");
          }
          catch (Exception ex)
          {
            Util.BBSet("RoundPositionBB", "done", true);
            Util.BBSet("RoundPositionBB", "roundPosition", numClients + 1);
            FwkException("In DoRandomEntryOperation() caught excption {0}.",ex);
          }
          Thread.Sleep(3000);
          
        }
      }
      catch (Exception ex)
      {
        FwkException("DoRandomEntryOperation() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRandomEntryOperation() complete.");
    }
    public void doPopulateNewEntry()
    {
      IRegion<TKey,TVal> aRegion = GetRegion();
      ResetKey("distinctKeys");
      int numkeys = GetUIntValue("distinctKeys");
      for(int i = 0;i<numkeys;i++)
        addEntry(aRegion);
    }
    public void verifyFromSnapshot()
    {
      verifyFromSnapshotOnly();
      //verifyInternalPRState();
    }
    public void verifyFromSnapshotOnly()
    {
      IRegion<TKey, TVal> aRegion = GetRegion();
      if (isEmptyClient)
      {
        verifyServerKeysFromSnapshot();
        return;
      }/*
      else if (isThinClient)
      {
        //verifyThinClientFromSnapshot();
        verifyServerKeysFromSnapshot();
        return;
      }*/
      StringBuilder aStr = new StringBuilder();
      regionSnapshot = (Dictionary<TKey, TVal>)Util.BBGet("RegionSnapshot", "regionSnapshot");
      int snapshotSize = regionSnapshot.Count;
      int regionSize = 0;
      if (isEmptyClient || m_istransaction)
        regionSize = aRegion.Count;
      else
        regionSize = aRegion.GetLocalView().Count;
      //int regionSize = aRegion.Count;// rjk this need to be uncommented if transaction is true.
      //int regionSize = aRegion.GetLocalView().Count;
      FwkInfo("Verifying from snapshot containing " + snapshotSize + " entries...");
      if (snapshotSize != regionSize)
      {
        aStr.Append("Expected region " + aRegion.FullPath + " to be size " + snapshotSize +
             ", but it is " + regionSize.ToString() + "\n");
      }
      foreach( KeyValuePair<TKey, TVal> kvp in regionSnapshot )
      {
        TKey key = kvp.Key;
        TVal expectedValue = kvp.Value;
        try
        {
          verifyContainsKey(aRegion, key, true);
        }
        catch (Exception e)
        {
          aStr.Append(e.Message + "\n");
          //         anyFailures = true;
        }
        bool containsValueForKey = aRegion.GetLocalView().ContainsValueForKey(key);
        try
        {
          verifyContainsValueForKey(aRegion, key, (expectedValue != null));
        }
        catch (Exception e)
        {
          aStr.Append(e.Message + "\n");
        }

        // do a get on the partitioned region if a loader won't get invoked; test its value
        if (containsValueForKey)
        {
          // loader won't be invoked if we have a value for this key (whether or not a loader
          // is installed), or if we don't have a loader at all
          try
          {
            TVal actualValue = aRegion[key];
            //if (!EqualityComparer<TVal>.Default.Equals(actualValue, default(TVal)))
            //{
            if (!actualValue.Equals(expectedValue))
            {
              FwkException("verifyFromSnapshotOnly: expected value {0} is not same as actual value {1} for key {3}",
                expectedValue, actualValue, key);
            }
            //}
          }
          catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
          {
          }
          catch (Exception e)
          {
            aStr.Append(e.Message + "\n");
          }
        }
      }
      if (isSerialExecution)
      {
        // check that destroyedKeys are not in the region
        destroyedKeys = (List<TKey>)Util.BBGet("DestroyedKeys", "destroyedKeys");
        for (int i = 0; i < destroyedKeys.Count; i++)
        {
          TKey key = destroyedKeys[i];
          try
          {
            verifyContainsKey(aRegion, key, false);
          }
          catch (Exception e)
          {
            aStr.Append(e.Message + "\n");
          }
        }
      }
        try
        {
          verifyServerKeysFromSnapshot();
        }
        catch (Exception e)
        {
          aStr.Append(e.Message + "\n");
        }
      
      if (aStr.Length > 0)
      {
        // shutdownHook will cause all members to dump partitioned region info
        throw new Exception(aStr.ToString());
      }
      FwkInfo("Done verifying from snapshot containing " + snapshotSize.ToString() + " entries...");
    }
    
    public void verifyServerKeysFromSnapshot()
    {
      IRegion<TKey, TVal> aRegion = GetRegion();
      StringBuilder aStr = new StringBuilder();
      regionSnapshot = (Dictionary<TKey, TVal>)Util.BBGet("RegionSnapshot", "regionSnapshot");
      if(isSerialExecution)
        destroyedKeys = (List<TKey>)Util.BBGet("DestroyedKeys", "destroyedKeys");
      ICollection<TKey> serverKeys = new System.Collections.ObjectModel.Collection<TKey>();
      foreach (TKey key in aRegion.Keys)
        serverKeys.Add(key);

      int snapshotSize = regionSnapshot.Count;
      int numServerKeys = serverKeys.Count;
      //int numServerKeys = aRegion.Count;
      FwkInfo("Verifying server keys from snapshot containing " + snapshotSize.ToString() + " entries...");
      if (snapshotSize != numServerKeys)
      {
        aStr.Append("Expected number of keys on server to be " + snapshotSize.ToString() + ", but it is " + numServerKeys.ToString() + "\n");
      }
      foreach (KeyValuePair<TKey, TVal> kvp in regionSnapshot)
      { // iterating the expected keys
        TKey key = kvp.Key;
        TVal expectedValue = kvp.Value;
        if (!serverKeys.Contains(key))
        {
          aStr.Append("Expected key " + key + " to be in server keys set, but it is missing\n");
        }
        else
        {
          // only do a get if we will not invoke the silence listener on a get
          if ((!isThinClient && !isEmptyClient) ||
              (isThinClient && aRegion.GetLocalView().ContainsKey(key)))
          {
            try
            {
            TVal valueOnServer = aRegion[key];
            //if (!EqualityComparer<TVal>.Default.Equals(valueOnServer, default(TVal)))
              //{
                if (!valueOnServer.Equals(expectedValue))
                {
                  FwkException("verifyFromSnapshotOnly: expected value {0} is not same as actual value {1} for key {2}",
                    expectedValue, valueOnServer,key);
                }
              //}
            }
            catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
            {
            }
            catch (Exception e)
            {
              aStr.Append(e.Message + "\n");
            }
          }
        }
      }

      // check that destroyedKeys are not in the server keys
      if (isSerialExecution)
      {
        destroyedKeys = (List<TKey>)Util.BBGet("DestroyedKeys", "destroyedKeys");
        for (int i = 0; i < destroyedKeys.Count; i++)
        {
          TKey key = destroyedKeys[i];
          if (serverKeys.Contains(key))
          {
            aStr.Append("Destroyed key " + key + " was returned as a server key\n");
          }
        }


        foreach (TKey key in regionSnapshot.Keys)
        {
          FwkInfo("IsReadOnly {0}", serverKeys.IsReadOnly);
          FwkInfo("serverKeys  key {0}", key);
          serverKeys.Remove(key);
        }
        if (serverKeys.Count != 0)
        {
          foreach (TKey key in serverKeys)
          {
            aStr.Append("Found the following unexpected keys in server keys: " +
                        ": " + key + "\n");
          }
        }
      }
      if (aStr.Length > 0)
      {
        // shutdownHook will cause all members to dump partitioned region info
        throw new Exception(aStr.ToString());
      }
      FwkInfo("Done verifying server keys from snapshot containing " + snapshotSize.ToString() + " entries...");
    }
    public void dumpDataOnBB()
    {
      IRegion<TKey, TVal> aRegion = GetRegion();
      ICollection<TKey> serverkeys = aRegion.Keys;
      //int expectedKeys = GetUIntValue("expectedKeys");
      //if (expectedKeys != aRegion.Count)
      //{
      //  FwkException("CacheServerTest::dumpDataOnBB(): region count {0} is not same as expected size {0}", aRegion.Count, expectedKeys);
      //}
      regionSnapshot = new Dictionary<TKey, TVal>();
      foreach (TKey key in serverkeys)
      {
          TVal value = default(TVal);;
          try
          {
              value = aRegion[key];
          }catch(GemStone.GemFire.Cache.Generic.KeyNotFoundException){
              value = default(TVal);
          }
          regionSnapshot[key] = value;
      }
      Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
    }
    // Tx code end
    public void DoEntryOperations()
    {
      FwkInfo("CacheServer.DoEntryOperations() called.");

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);

      bool isCq = GetBoolValue("cq");
          
      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      TKey key;
      TVal value;
      TVal tmpValue;
      //TVal valBuf = (TVal)(object)Encoding.ASCII.GetBytes(new string('A', valSize));
      byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));
      string opcode = null;

      Int32 creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, query = 0, putAll = 0, getAll = 0;
      IRegion<TKey,TVal> region = GetRegion();
      if (region == null)
      {
        FwkSevere("CacheServer.DoEntryOperations(): No region to perform operations on.");
        now = end; // Do not do the loop
      }

      FwkInfo("CacheServer.DoEntryOperations() will work for {0}secs " +
        "using {1} byte values.", secondsToRun, valSize);

      PaceMeter pm = new PaceMeter(opsSec);
      string objectType = GetStringValue(ObjectType);
      bool multiRegion = GetBoolValue("multiRegion");
      while (now < end)
      {
        try
        {
          opcode = GetStringValue(EntryOps);
          if (opcode == null || opcode.Length == 0) 
          {
            opcode = "no-op";
          }
          if (multiRegion)
          {
            region = GetRegion();
            if (region == null)
            {
              FwkException("CacheServerTest::doEntryOperations(): No region to perform operation {0}" , opcode);
            }
          }

          key = (TKey)(object)GetKey(entryCount);
          if (opcode == "add")
          {
              if (isCq)
              {
                  if (region.ContainsKey(key))
                  {
                      if (objectType != null && objectType.Length > 0)
                      {
                          tmpValue = GetUserObject(objectType);
                      }
                      else
                      {
                          int keyVal = int.Parse(key.ToString());
                          int val = BitConverter.ToInt32(valBuf, 0);
                          val = (val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
                          BitConverter.GetBytes(val).CopyTo(valBuf, 0);
                          BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
                          tmpValue = (TVal)(object)valBuf;
                      }
                      region[key] = tmpValue;
                      puts++;
                      
                  }
                  else
                  {
                      if (objectType != null && objectType.Length > 0)
                      {
                          tmpValue = (TVal)(object)GetUserObject(objectType);
                      }
                      else
                      {
                          tmpValue = (TVal)(object)valBuf;
                      }
                      try
                      {
                          region.Add(key, tmpValue);
                          creates++;
                      }
                      catch (EntryExistsException ex)
                      {
                          FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                               "exception in add: {0}", ex.Message);
                      }
                  }
              }
          }
          else
          {
            if (opcode == "update")
            {
              if (objectType != null && objectType.Length > 0)
              {
                tmpValue = GetUserObject(objectType);
              }
              else
              {
                int keyVal = int.Parse(key.ToString());
                int val = BitConverter.ToInt32(valBuf, 0);
                val = (val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
                BitConverter.GetBytes(val).CopyTo(valBuf, 0);
                BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
                tmpValue = (TVal)(object)valBuf;
              }
              region[key] = tmpValue;
              puts++;
            }
            else if (opcode == "invalidate")
            {
                if (isCq)
                {
                    if (region.ContainsKey(key))
                    {
                        if (region.ContainsValueForKey(key))
                        {
                            try
                            {
                                region.Invalidate(key);
                                invals++;
                            }
                            catch (EntryNotFoundException ex)
                            {
                                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                                  "exception in invalidate: {0}", ex.Message);
                            }
                        }

                    }
                }
                else
                {
                  try
                  {
                    region.Invalidate(key);
                    invals++;
                  }
                  catch (EntryNotFoundException ex)
                  {
                    FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                      "exception in invalidate: {0}", ex.Message);
                  }
                }
            }
            else if (opcode == "destroy")
            {
              try
              {
                region.Remove(key);
                dests++;
              }
              catch (EntryNotFoundException ex)
              {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "exception in destroy: {0}", ex.Message);
              }
            }
            else if (opcode == "read")
            {
              try {
                value = (TVal)region[key];
                gets++;
              }
              catch (EntryNotFoundException ex) {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "exception in read: {0}", ex.Message);
              }
              catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex) {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "exception in read: {0}", ex.Message);
              }
            }
            else if (opcode == "read+localdestroy")
            {
              value = region[key];
              gets++;
              try
              {
                region.GetLocalView().Remove(key);
                dests++;
              }
              catch (EntryNotFoundException ex)
              {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "exception in localDestroy: {0}", ex.Message);
              }
            }
            else if (opcode == "query")
            {
              RunQuery();
              query += 4;
            }
            else if (opcode == "putAll")
            {
              PutAllOps();
              putAll++;
            }
            else if (opcode == "getAll")
            {
              GetAllOps();
              getAll++;
            }
            else
            {
              FwkException("CacheServer.DoEntryOperations() Invalid operation " +
                "specified: {0}", opcode);
            }
          }
        }
        catch (Exception ex)
        {
          FwkException("CacheServer.DoEntryOperations() Caught unexpected " +
            "exception during entry '{0}' operation: {1}.", opcode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      key = default(TKey);
      value = default(TVal);

      FwkInfo("CacheServer.DoEntryOperations() did {0} creates, {1} puts, " +
        "{2} gets, {3} invalidates, {4} destroys, {5} querys, {6} putAll, {7} getAll.",
        creates, puts, gets, invals, dests, query, putAll, getAll);
      Util.BBSet("OpsBB","CREATES",creates);
      Util.BBSet("OpsBB","UPDATES",puts);
      Util.BBSet("OpsBB","DESTROYS",dests);
      Util.BBSet("OpsBB","GETS",gets);
      Util.BBSet("OpsBB","INVALIDATES",invals);
    }

    

    public void DoEntryOperationsForSecurity()
    {
      FwkInfo("CacheServer.DoEntryOperationsForSecurity() called.");
      Util.RegisterTestCompleteDelegate(TestComplete);

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      TKey key;
      TVal value;
      TVal tmpValue;
      byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));

      string opCode = null;

      IRegion<TKey,TVal> region = GetRegion();
      if (region == null)
      {
        FwkException("CacheServer.DoEntryOperationsForSecurity(): " +
          "No region to perform operations on.");
      }

      FwkInfo("CacheServer.DoEntryOperationsForSecurity() will work for {0}secs " +
        "using {1} byte values.", secondsToRun, valSize);

      int cnt = 0;
      int creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, query = 0;
      PaceMeter pm = new PaceMeter(opsSec);
      string objectType = GetStringValue(ObjectType);
      while (now < end)
      {
        int addOps = 1;
        opCode = GetStringValue(EntryOps);
        
        try
        {
          UpdateOperationsMap(opCode, 1);
          if (opCode == null || opCode.Length == 0)
          {
            opCode = "no-op";
          }

          key = GetKey(entryCount);
          if (opCode == "create")
          {
            if (objectType != null && objectType.Length > 0)
            {
              tmpValue = GetUserObject(objectType);
            }
            else
            {
              tmpValue = (TVal)(object)valBuf;
            }
            region.Add(key, tmpValue);
            creates++;
          }
          else
          {
            if (opCode == "update")
            {
              if (objectType != null && objectType.Length > 0)
              {
                tmpValue = (TVal)(object)GetUserObject(objectType);
              }
              else
              {
                int keyVal = int.Parse(key.ToString());
                int val = BitConverter.ToInt32(valBuf, 0);
                val = (val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
                BitConverter.GetBytes(val).CopyTo(valBuf, 0);
                BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
                tmpValue = (TVal)(object)valBuf;
              }
              region[key] = tmpValue;
              puts++;
            }
            else if (opCode == "invalidate")
            {
              region.Invalidate(key);
              invals++;
            }
            else if (opCode == "destroy")
            {
              region.Remove(key);
              dests++;
            }
            else if (opCode == "get")
            {
              value = (TVal)(object)region[key];
              gets++;
            }

            else if (opCode == "getServerKeys")
            {
              ICollection<TKey> serverKeys = region.Keys;
            }

            else if (opCode == "read+localdestroy")
            {
              value = region[key];
              gets++;
              region.GetLocalView().Remove(key);
              dests++;
            }
            else if (opCode == "regNUnregInterest")
            {
              TKey[] keys = new TKey[] { key };
                region.GetSubscriptionService().RegisterKeys(keys);
                region.GetSubscriptionService().UnregisterKeys(keys);
            }
            else if (opCode == "query")
            {
              QueryService<TKey, object> qs = CheckQueryService();
              Query<object> qry = qs.NewQuery("select distinct * from /Portfolios where FALSE");
              ISelectResults<object> result = qry.Execute(600);
              query++;
            }
            else if (opCode == "cq")
            {
              string cqName = String.Format("cq-{0}-{1}", Util.ClientId ,cnt++);
              QueryService<TKey, object> qs = CheckQueryService();
              CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
              ICqListener<TKey, object> cqLstner = new MyCqListener<TKey, object>();
              cqFac.AddCqListener(cqLstner);
              CqAttributes<TKey, object> cqAttr = cqFac.Create();
              CqQuery<TKey, object> cq = qs.NewCq(cqName, "select * from /Portfolios where FALSE", cqAttr, false);
              cq.Execute();
              cq.Stop();
              cq.Execute();
              cq.Close();
            }
            else
            {
              FwkException("CacheServer.DoEntryOperationsForSecurity() " +
                "Invalid operation specified: {0}", opCode);
            }
          }
        }
        catch (NotAuthorizedException)
        {
          //FwkInfo("Got expected NotAuthorizedException for operation {0}: {1}",
          //  opCode, ex.Message);
          UpdateExceptionsMap(opCode, 1);
        }
        catch (EntryExistsException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (EntryNotFoundException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (EntryDestroyedException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (TimeoutException ex)
        {
          FwkSevere("Caught unexpected timeout exception during entry {0} " +
            " operation: {1}; continuing with test.", opCode, ex.Message);
        }
        catch (Exception ex)
        {
          FwkException("CacheServer.DoEntryOperationsForSecurity() Caught " +
            "unexpected exception during entry '{0}' operation: {1}.",
            opCode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      key = default(TKey);
      value = default(TVal);
      FwkInfo("CacheServer.DoEntryOperationsForSecurity() did {0} creates, {1} puts, " +
        "{2} gets, {3} invalidates, {4} destroys, {5} querys.",
        creates, puts, gets, invals, dests, query);
    }

    public void DoValidateEntryOperationsForSecurity()
    {
      bool isExpectedPass = GetBoolValue("isExpectedPass");
      string opCode;
      while ((opCode = GetStringValue(EntryOps)) != null)
      {
        int numOps = GetOpsFromMap(OperationsMap, opCode);
        int notAuthzCount = GetOpsFromMap(ExceptionsMap, opCode);
        if (isExpectedPass)
        {
          if (numOps != 0 && notAuthzCount == 0)
          {
            FwkInfo("Task passed sucessfully for operation {0} with total " +
              "operations = {1}", opCode, numOps);
          }
          else
          {
            FwkException("{0} NotAuthorizedExceptions found for operation {1} " +
              "while expected 0", notAuthzCount, opCode);
          }
        }
        else
        {
          if (numOps == notAuthzCount)
          {
            FwkInfo("Operation {0} passed sucessfully and got the expected " +
              "number of incorrect authorizations: {1}", opCode, numOps);
          }
          else
          {
            FwkException("For operation {0} expected number of " +
              "NotAuthorizedExceptions is {1} but found {2}", opCode,
              numOps, notAuthzCount);
          }
        }
      }
    }
    //------------------------------------------------doOps for rvv -----------------
      
      public void DoRegisterInterestList()
      {
          FwkInfo("In DoRegisterInterestList()");
          try
          {
              int entryCount = GetUIntValue("entryCount");
              int numNewKeys = GetUIntValue("NumNewKeys");
            /*
              TKey[] registerKeyList = new TKey[entryCount + numNewKeys + NUM_EXTRA_KEYS];
              for (int i = 0; i < entryCount + numNewKeys + NUM_EXTRA_KEYS; i++)
              {
                  registerKeyList[i] = m_KeysA[i];
              }*/
              IRegion<TKey, TVal> region = GetRegion();
              FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
              bool isDurable = GetBoolValue("isDurableReg");
              ResetKey("getInitialValues");
              bool isGetInitialValues = GetBoolValue("getInitialValues");
              bool isReceiveValues = true;
              bool checkReceiveVal = GetBoolValue("checkReceiveVal");
              if (checkReceiveVal)
              {
                  ResetKey("receiveValue");
                  isReceiveValues = GetBoolValue("receiveValue");
              }
              ResetKey("sleepBeforeRegisterInterest");
              int sleepTime = GetUIntValue("sleepBeforeRegisterInterest");
              sleepTime = sleepTime > 0 ? sleepTime : 0;
              FwkInfo("Sleeping for " + sleepTime + " millis");
              Thread.Sleep(sleepTime);
              region.GetSubscriptionService().RegisterKeys(m_KeysA, isDurable, isGetInitialValues, isReceiveValues);
              String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
              if (durableClientId.Length > 0)
              {
                  CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
              }
          }
          catch (Exception ex)
          {
              FwkException("DoRegisterInterestList() Caught Exception: {0}", ex);
          }
          FwkInfo("DoRegisterInterestList() complete.");
      }

      public void DoRegisterSingleKey()
      {
          FwkInfo("In DoRegisterSingleKey()");
          try
          {
              
            ExpectedRegionContents expected = null;
               
            string expectedcontentsRI_noops = GetStringValue("expectedRegionContents");
            if(expectedcontentsRI_noops == "static_RI_noops_keysValues")
                expected = expectedRgnContents[0];
            else
                expected = expectedRgnContents[2];
              
              int entryCount = GetUIntValue("entryCount");
              int numNewKeys = GetUIntValue("NumNewKeys");
              IRegion<TKey, TVal> region = GetRegion();
              FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
              bool isDurable = GetBoolValue("isDurableReg");
              ResetKey("getInitialValues");
              bool isGetInitialValues = GetBoolValue("getInitialValues");
              bool isReceiveValues = true;
              bool checkReceiveVal = GetBoolValue("checkReceiveVal");
              if (checkReceiveVal)
              {
                  ResetKey("receiveValue");
                  isReceiveValues = GetBoolValue("receiveValue");
              }
              ResetKey("sleepBeforeRegisterInterest");
              int sleepTime = GetUIntValue("sleepBeforeRegisterInterest");
              sleepTime = sleepTime > 0 ? sleepTime : 0;
              FwkInfo("Sleeping for " + sleepTime + " millis");
              Thread.Sleep(sleepTime);
              //TKey[] registerKeyList = new TKey[entryCount + numNewKeys + NUM_EXTRA_KEYS];
              List<TKey> templist = new List<TKey>();
              for (int i = 0; i < entryCount + numNewKeys + NUM_EXTRA_KEYS; i++)
              {
                  templist.Add(m_KeysA[i]);
                  region.GetSubscriptionService().RegisterKeys(templist, isDurable, isGetInitialValues, isReceiveValues);
                  if (expected != null)
                     verifyEntry(m_KeysA[i], expected);
                  templist.Clear();
              }
              String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
              if (durableClientId.Length > 0)
              {
                  CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
              }
          }
          catch (Exception ex)
          {
              FwkException("DoRegisterSingleKey() Caught Exception: {0}", ex);
          }
          FwkInfo("DoRegisterSingleKey() complete.");
      }

    public void doFeedInt()
    {
      FwkInfo("CacheServer.doFeedInt() called.");

      int entryCount = GetUIntValue("entryCount");
      entryCount = (entryCount < 1) ? 10000 : entryCount;
     try {
        IRegion<TKey,TVal> region = GetRegion();
        TVal value = default(TVal);
        for(int i = 0;i < entryCount;i++){
            value = GetValue((i + 1));
            if (value != null)
            {
                if (region.ContainsKey(m_KeysA[i]))
                    region[m_KeysA[i]] = value;
                else 
                region.Add(m_KeysA[i], value);
            }
        }
      }catch(Exception ex)
      {
        FwkException("doFeedInt() Caught Exception: {0}", ex);
      }
      FwkInfo("doFeedInt() complete.");
    }
    private TVal GetValue()
         {
             return GetValue(null);
         }

    private TVal GetValue(object value)
    {
        TVal tmpValue = default(TVal);
        FwkTest<TKey, TVal>.CurrentTest.ResetKey("valueSizes");
        int size = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("valueSizes");
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        char ch;
        for (int j = 0; j < size; j++)
        {
            ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
            builder.Append(ch);
        }
        FwkTest<TKey, TVal>.CurrentTest.ResetKey("objectType");
        string m_objectType = FwkTest<TKey, TVal>.CurrentTest.GetStringValue("objectType");
        //FwkTest<TKey, TVal>.CurrentTest.ResetKey("elementSize");
        //int elementSize = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("elementSize");
        FwkTest<TKey, TVal>.CurrentTest.ResetKey("versionNum");
        int m_versionNum = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("versionNum"); //random.Next(2) + 1;// (counter++ % 2) + 1;
        string sharedpath = (string)Util.BBGet("SharedPath", "sharedDir");
        string m_sharePath = Path.Combine(sharedpath, "framework/csharp/bin");
        if (typeof(TVal) == typeof(string))
        {
            tmpValue = (TVal)(object)builder.ToString();
        }
        else if (typeof(TVal) == typeof(byte[]))
        {
            tmpValue = (TVal)(object)(Encoding.ASCII.GetBytes(builder.ToString()));
        }
        else if (m_objectType != null)
        {
            if (m_objectType.Equals("PdxVersioned") && m_versionNum == 1)
            {
                PdxTests<TKey, TVal>.m_pdxVersionOneAsm = Assembly.LoadFrom(Path.Combine(m_sharePath, "PdxVersion1Lib.dll"));
                Type pt = PdxTests<TKey, TVal>.m_pdxVersionOneAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
                tmpValue = (TVal)pt.InvokeMember("PdxVersioned", BindingFlags.CreateInstance, null, null, new object[] { value.ToString() });
            }
            else if (m_objectType.Equals("PdxVersioned") && m_versionNum == 2)
            {
                PdxTests<TKey, TVal>.m_pdxVersionTwoAsm = Assembly.LoadFrom(Path.Combine(m_sharePath, "PdxVersion2Lib.dll"));
                Type pt = PdxTests<TKey, TVal>.m_pdxVersionTwoAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
                tmpValue = (TVal)pt.InvokeMember("PdxVersioned", BindingFlags.CreateInstance, null, null, new object[] { value.ToString() });
            }
            else if (m_objectType.Equals("Nested"))
            {

                tmpValue = (TVal)(object)new NestedPdx();
            }
            else if (m_objectType.Equals("PdxType"))
            {
                tmpValue = (TVal)(object)new PdxType();
            }
            /*else if (m_objectType.Equals("PdxInstanceFactory"))
            {

                tmpValue = createIpdxInstance();
            }*/
            else if (m_objectType.Equals("AutoSerilizer"))
            {
                tmpValue = (TVal)(object)new SerializePdx1(true);
            }
        }
        else
            tmpValue = (TVal)(object)value;
        return tmpValue;

    }
    public void DoInitInstance() {
	try {
     ResetKey("entryCount");
	int entryCount = GetUIntValue( "entryCount" );
	entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
    ResetKey("NumNewKeys");
	int numNewKeys = GetUIntValue( "NumNewKeys" );
	//m_KeysA = new List<TKey>();
	SetFisrtAndLastKeyOnBB(entryCount);
	for(int cnt = 0;cnt < entryCount;cnt++){
        m_KeysA.Add((TKey)(object)(cnt + 1));
	}
	 for (int i = entryCount; i < entryCount+numNewKeys; i++)
         m_KeysA.Add((TKey)(object)(i + 1));
	  // add some extra keys to the list that are never created by the test
	 for (int i = entryCount+numNewKeys;i<entryCount+numNewKeys + NUM_EXTRA_KEYS; i++) // do a few more keys than we really need
         m_KeysA.Add((TKey)(object)(i + 1));
	 //static_RI_noops_keysValues 
     //expectedRgnContents = new ExpectedRegionContents[4];
     expectedRgnContents[0] = new ExpectedRegionContents(
	       true,  true,  // none
	       true,  true,  // invalidate
	       true,  true,  // localInvalidate
	       true,  true,  // destroy
	       true,  true,  // localDestroy
	       true,  true,  // update
	       true,  true,  // get
	       false, false, // newKey
	       false,        // get allowed during validate
           false);       // update has occurred
        expectedRgnContents[0].exactSize(entryCount);
	/* static_RI_noops_keys = new ExpectedRegionContents(
	       true,  false,  // none
	       true,  false,  // invalidate
	       true,  false,  // localInvalidate
	       true,  false,  // destroy
	       true,  false,  // localDestroy
	       true,  false,  // update
	       true,  false,  // get
	       false, false, // newKey
	       false,        // get allowed during validate
	       false);
	 static_RI_noops_none = new ExpectedRegionContents(
	       false, false,  // none
	       false, false,  // invalidate
	       false, false,  // localInvalidate
	       false, false,  // destroy
	       false, false,  // localDestroy
	       false, false,  // update
	       false, false,  // get
	       false, false, // newKey
	       false,        // get allowed during validate
	       false);
      */
	 //static_RI_ops_keysValues 
     expectedRgnContents[1] = new ExpectedRegionContents( // This is also valid for relicated function executions
	 	       true,  true,  // none
	 	       true,  false, // invalidate
	 	       true,  true,  // localInvalidate
	 	       false, false, // destroy
	 	       true,  true,  // localDestroy
	 	       true,  true,  // update
	 	       true,  true,  // get
	 	       true,  true,  // newKey
	 	       true,         // get allowed during validate
	 	       true);        // update has occurred
     Int32 numDestroyed = (Int32)Util.BBGet("ImageBB", "Last_Destroy") - (Int32)Util.BBGet("ImageBB", "First_Destroy") + 1;
     Int32 numInvalidated = (Int32)Util.BBGet("ImageBB", "Last_Invalidate") - (Int32)Util.BBGet("ImageBB", "First_Invalidate") + 1;
     expectedRgnContents[1].exactSize(entryCount - numDestroyed + numNewKeys);
	/* static_RI_ops_keys = new ExpectedRegionContents(
	       true,  false, // none
	       true,  false, // invalidate
	       true,  false, // localInvalidate
	       false, false, // destroy
	       true,  false, // localDestroy
	       true,  true,  // update
	       true,  false, // get
	       true,  true,  // newKey
	       true,         // get allowed during validate
	       true);        // update has occurred
	 static_RI_ops_none = new ExpectedRegionContents(
	       false, false, // none
	       false, false, // invalidate
	       false, false, // localInvalidate
	       false, false, // destroy
	       false, false, // localDestroy
	       true,  true,  // update
	       false, false, // get
	       true,  true,  // newKey
	       true,         // get allowed during validate
	       true);        // update has occurred
	       */
	 //static_ops_RI_keysValues = 
     expectedRgnContents[2] = new ExpectedRegionContents(
	       true,  true,  // none
	       true,  false, // invalidate
	       true,  true,  // localInvalidate
	       false, false, // destroy
	       true,  true,  // localDestroy
	       true,  true,  // update
	       true,  true,  // get
	       true,  true,  // newKey
	       true,         // get allowed during validate
	       true);        // update has occurred
     expectedRgnContents[2].exactSize(entryCount - 2 * numDestroyed + numNewKeys + numInvalidated);
	  //dynamicKeysValues
           expectedRgnContents[3] = new ExpectedRegionContents(true, true, true);
           expectedRgnContents[3].containsValue_invalidate(false);
           expectedRgnContents[3].containsKey_destroy(false);
           expectedRgnContents[3].containsValue_destroy(false);
           expectedRgnContents[3].valueIsUpdated(true);
           expectedRgnContents[3].exactSize(entryCount - numDestroyed + numNewKeys);

           expectedRgnContents[4] = new ExpectedRegionContents( // Thit is for the same same client where we done ADD operation.
              true, true,  // none
              true, true,  // invalidate
              true, true,  // localInvalidate
              true, true,  // destroy
              true, true,  // localDestroy
              true, true,  // update
              true, true,  // get
              false, false, // newKey
              true,        // get allowed during validate
              false);       // update has occurred
           expectedRgnContents[4].exactSize(entryCount);
	 } catch ( Exception e ) {
         FwkException("Caught unexpected exception during DoInitInstance " + e + " exiting task.");
     }
	 
   }
      public void DoResetImageBB()
      {
          ResetKey("entryCount");
          int entryCount = GetUIntValue("entryCount");
          SetFisrtAndLastKeyOnBB(entryCount);
          Util.BBSet("ListenerBB", "lastEventTime", 0);
    
      }
public void SetFisrtAndLastKeyOnBB(int entrycount  /* similar to numKeyIntervals on hydra*/)
{
        FwkInfo("In CacheServerTest::SetFisrtAndLastKeyOnBB");
	   int count = 0; // 7 is num of ops ( create,update,get invalidate, local invalidate, destroy, local destroy )
        bool m_istransaction = GetBoolValue( "useTransactions" );
     if(m_istransaction){
          count  = entrycount/5; // 5 is num of ops ( create,update,get invalidate,destroy )
	  Util.BBSet("ImageBB", "First_LocalInvalidate",-1);
	  Util.BBSet("ImageBB", "Last_LocalInvalidate",-1);
	  Util.BBSet("ImageBB", "First_LocalDestroy",-1);
	  Util.BBSet("ImageBB", "Last_LocalDestroy",-1);
	  Util.BBSet("ImageBB","LASTKEY_LOCAL_INVALIDATE",1);
	  Util.BBSet("ImageBB","LASTKEY_LOCAL_DESTROY",1);
	  }
        else {
          count  = entrycount/7; // 7 is num of ops ( create,update,get invalidate, local invalidate, destroy, local destroy )
          Util.BBSet("ImageBB", "First_LocalInvalidate",(5*count) +1);
	  Util.BBSet("ImageBB", "Last_LocalInvalidate",6*count);
	  Util.BBSet("ImageBB", "First_LocalDestroy",(6*count) +1);
	  Util.BBSet("ImageBB", "Last_LocalDestroy",entrycount);
	  Util.BBSet("ImageBB","LASTKEY_LOCAL_INVALIDATE",(5*count) +1);
	  Util.BBSet("ImageBB","LASTKEY_LOCAL_DESTROY",(6*count) +1);
	}
	Util.BBSet("ImageBB", "First_None",1);
	Util.BBSet("ImageBB", "Last_None",count);
	Util.BBSet("ImageBB", "First_Invalidate",count +1);
	Util.BBSet("ImageBB", "Last_Invalidate",2*count);
    Util.BBSet("ImageBB", "First_Destroy",(2*count) +1);
	Util.BBSet("ImageBB", "Last_Destroy",3*count);
	Util.BBSet("ImageBB", "First_UpdateExistingKey",(3*count) +1);
	Util.BBSet("ImageBB", "Last_UpdateExistingKey",4*count);
	Util.BBSet("ImageBB", "First_Get",(4*count) +1);
	Util.BBSet("ImageBB", "Last_Get",5*count);
	Util.BBSet("ImageBB","NUM_NEW_KEYS_CREATED",1);
	Util.BBSet("ImageBB","LASTKEY_UPDATE_EXISTING_KEY",(3*count) +1);
	Util.BBSet("ImageBB","LASTKEY_INVALIDATE",(count) +1);
	Util.BBSet("ImageBB","LASTKEY_DESTROY",(2*count) +1);
	Util.BBSet("ImageBB","LASTKEY_GET",(4*count) +1);

	FwkInfo(printKeyIntervalsBBData());
}
     
public string printKeyIntervalsBBData( ) {
    ResetKey("NumNewKeys");
    ResetKey("entryCount");
	int numNewKeys = GetUIntValue( "NumNewKeys" );
	int numKeyIntervals = GetUIntValue( "entryCount" );
    int numDestroyed = (int)Util.BBGet("ImageBB", "Last_Destroy") - (int)Util.BBGet("ImageBB", "First_Destroy") + 1;
	totalNumKeys = numKeyIntervals + numNewKeys - numDestroyed;
    StringBuilder attrsSB = new StringBuilder();
    attrsSB.Append(Environment.NewLine + "keyIntervals read from blackboard = ");
    attrsSB.Append(Environment.NewLine + "none: firstKey:" + Util.BBGet("ImageBB", "First_None") + 
        ", lastKey: " + Util.BBGet("ImageBB", "Last_None"));
    attrsSB.Append(Environment.NewLine + "invalidate: firstKey: " + Util.BBGet("ImageBB", "First_Invalidate") +
        ", lastKey: " + Util.BBGet("ImageBB", "Last_Invalidate"));
    attrsSB.Append(Environment.NewLine + "localInvalidate:  firstKey: " + Util.BBGet("ImageBB", "First_LocalInvalidate") +
        ", lastKey: " + Util.BBGet("ImageBB", "Last_LocalInvalidate"));
    attrsSB.Append(Environment.NewLine + "destroy: firstKey: " + Util.BBGet("ImageBB", "First_Destroy")+
        ", lastKey: " + Util.BBGet("ImageBB", "Last_Destroy"));
    attrsSB.Append(Environment.NewLine + "localDestroy: firstKey: " + Util.BBGet("ImageBB", "First_LocalDestroy") +
        ", lastKey: " + Util.BBGet("ImageBB", "Last_LocalDestroy"));
    attrsSB.Append(Environment.NewLine + "updateExistingKey: firstKey: " + Util.BBGet("ImageBB", "First_UpdateExistingKey") +
        ", lastKey: " + Util.BBGet("ImageBB", "Last_UpdateExistingKey"));
    attrsSB.Append(Environment.NewLine + "get: firstKey: " + Util.BBGet("ImageBB", "First_Get")+
        ", lastKey: " + Util.BBGet("ImageBB", "Last_Get"));
    attrsSB.Append(Environment.NewLine + "numKeyIntervals is  " + numKeyIntervals);
    attrsSB.Append(Environment.NewLine + "numNewKeys is  " + numNewKeys);
    attrsSB.Append(Environment.NewLine + "numDestroyed is " + numDestroyed);
    attrsSB.Append(Environment.NewLine + "totalNumKeys is " + totalNumKeys);
    attrsSB.Append(Environment.NewLine);

    return attrsSB.ToString();
}

public void doRROps()
{
    FwkInfo("doRROps called.");
    try
    {
      IRegion<TKey, TVal> region = GetRegion();
      ResetKey("entryCount");
      int entryCount = GetUIntValue("entryCount");
      ResetKey("NumNewKeys");
      int numNewKeys = GetUIntValue("NumNewKeys");
      bool isdone = false;
      Util.BBSet("RoundPositionBB", "done", false);
      string clntid = null;
      int roundPosition = 0;
      bool isSerialEx = GetBoolValue("serialExecution");
      if (isSerialEx)
      {
        Util.BBSet("RoundPositionBB", "roundPosition", 1);
        roundPosition = (int)Util.BBGet("RoundPositionBB", "roundPosition");
        clntid = String.Format("Client.{0}", roundPosition);
      }
      else
      {
        clntid = Util.ClientId;
      }
      int numClients = GetUIntValue("clientCount");
      string dummyClntid = null;
      ResetKey("numThreads");
      int numThreads = GetUIntValue("numThreads");
      while (true)
      {
        if (roundPosition > numClients)
          break;
        try
        {
            FwkInfo("clntid is = {0} Util.ClientId id is = {1} ",clntid,Util.ClientId);
          if (clntid.Equals(Util.ClientId))
          {
            DoOpsTask<TKey, TVal> dooperation = new DoOpsTask<TKey, TVal>(region, m_istransaction);
            if (isSerialEx)
            {
              RunTask(dooperation, numThreads, entryCount + numNewKeys + NUM_EXTRA_KEYS, -1, -1, null);
              DoWaitForSilenceListenerComplete();
              Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
              Util.BBSet("DestroyedKeys", "destroyedKeys", destroyedKeys);
              roundPosition = Util.BBIncrement("RoundPositionBB", "roundPosition");
              Util.BBSet("RoundPositionBB", "roundPosition", roundPosition);
              Util.BBSet("RoundPositionBB", "done", true);
              Util.BBSet("RoundPositionBB", "VerifyCnt", 0);
            }
            else
            {
              RunTask(dooperation, numThreads, entryCount + numNewKeys + NUM_EXTRA_KEYS, -1, -1, null);
              break;
            }
            FwkInfo("Done DoOpsTask operation");
          }
          else// if(!Util.ClientId.Equals(dummyClntid))
          {
            for (; ; )
            {
              isdone = (bool)Util.BBGet("RoundPositionBB", "done");
              if (isdone)
                 break;
            }
            if (isdone)
            {
              //Thread.Sleep(35000);
              try
              {
                //verifyFromSnapshot();
                DoVerifyQueryResult();
                Util.BBSet("RoundPositionBB", "done", false);
                Util.BBIncrement("RoundPositionBB", "VerifyCnt");
              }
              catch (Exception ex)
              {
                Util.BBSet("RoundPositionBB", "done", false);
                Util.BBIncrement("RoundPositionBB", "VerifyCnt");
                Util.BBSet("RoundPositionBB", "roundPosition", numClients + 1);
                FwkException("In DoRandomEntryOperation() caught excption {0}.", ex.Message);
              }
            }
            Thread.Sleep(100);
            FwkInfo("Done Verification and verifyCount = {0}", (int)Util.BBGet("RoundPositionBB", "VerifyCnt"));
          }
          if (isSerialEx)
          {
            int verifyCnt = (int)Util.BBGet("RoundPositionBB", "VerifyCnt");
            FwkInfo("DoRandomOperation: verifyCnt {0}, numclient {1}", verifyCnt, numClients);
            while (verifyCnt < numClients - 1)
            {
              verifyCnt = (int)Util.BBGet("RoundPositionBB", "VerifyCnt");
              Thread.Sleep(100);
            }
            roundPosition = (int)Util.BBGet("RoundPositionBB", "roundPosition");
            clntid = String.Format("Client.{0}", roundPosition);
            dummyClntid = String.Format("Client.{0}", (roundPosition - 1));
          }
          Thread.Sleep(3000);
        }
        catch (ClientTimeoutException)
        {
          Util.BBSet("RoundPositionBB", "done", true);
          Util.BBSet("RoundPositionBB", "roundPosition", numClients + 1);
          FwkException("In DoRandomEntryOperation()  Timed run timed out.");
        }
        catch (Exception ex)
        {
          Util.BBSet("RoundPositionBB", "done", true);
          Util.BBSet("RoundPositionBB", "roundPosition", numClients + 1);
          FwkException("In DoRandomEntryOperation() caught excption {0}.", ex.Message);
        }
        Thread.Sleep(3000);
      }
    }
    catch (Exception ex)
    {
        FwkException("last DoRandomEntryOperation() Caught Exception: {0}", ex.Message);
    }
    FwkInfo("DoRandomEntryOperation() complete.");
}

public void doOps()
{
    FwkInfo("doOps called.");

    ResetKey("entryCount");
    int entryCount = GetUIntValue("entryCount");
    ResetKey("NumNewKeys");
    int numNewKeys = GetUIntValue("NumNewKeys");

    //TestClient * clnt = TestClient::getTestClient();
    IRegion<TKey, TVal> regionPtr = GetRegion();
    if (regionPtr == null)
    {
        FwkSevere("CacheServerTest::doOps(): No region to perform operations on.");
        //now = end; // Do not do the loop
    }
    ResetKey("useTransactions");
    bool m_istransaction = GetBoolValue("useTransactions");
    try
    {
        DoOpsTask<TKey, TVal> dooperation = new DoOpsTask<TKey, TVal>(regionPtr, m_istransaction);
        ResetKey("numThreads");
        int numThreads = GetUIntValue("numThreads");
        RunTask(dooperation, numThreads, entryCount + numNewKeys + NUM_EXTRA_KEYS, -1, -1, null);
    }
    catch (ClientTimeoutException)
    {
        FwkException("In doOps()  Timed run timed out.");
    }
    catch (Exception e)
    {
        FwkException("Caught unexpected exception during doOps: " + e);
    }
    FwkInfo("Done in doOps");
    Thread.Sleep(10000);
}


public void DoVerifyQueryResult()
{
  FwkInfo("verifyQueryResult() called");
  string query = GetStringValue("query");
  IRegion<TKey, TVal> region = GetRegion();
  Int32 resultSize = region.Count;
  bool isSerial = GetBoolValue("serialExecution");
  Int32 localInvalidate = 0;
  if (isSerial)
  { localInvalidate = (Int32)Util.BBGet("ImageBB", "Last_LocalInvalidate") - (Int32)Util.BBGet("ImageBB", "First_LocalInvalidate") + 1; }
  FwkInfo("localInvalidate size is {0}", localInvalidate);
  while (query != null && query.Length > 0)
  {
      QueryService<TKey, object> qs = CheckQueryService();
      DateTime startTime;
      TimeSpan elapsedTime;
      ISelectResults<object> results;
      string CqName = String.Format("_default{0}", query);
      CqQuery<TKey, object> cq = qs.GetCq(CqName);
      try
      {
        if (cq.IsRunning())
        {
          cq.Stop();
        }
      }
      catch (IllegalStateException ex)
      {
        FwkException("Caught {0} while stopping cq", ex.Message);
      }
      startTime = DateTime.Now;
      results = cq.ExecuteWithInitialResults(QueryResponseTimeout);
      elapsedTime = DateTime.Now - startTime;
      if ((resultSize - localInvalidate) != results.Size)
      {
        FwkSevere("ReadQueryString: Result size found {0}, expected {1}.",
           results.Size, (resultSize - localInvalidate));
      }
      FwkInfo("ReadQueryString: Time Taken to execute the CqQuery [{0}]:" +
        "{1}ms ResultSize Size = {2}", query, elapsedTime.TotalMilliseconds, results.Size);
      FwkInfo("ReadQueryString: Got expected result size {0}.",
          results.Size);
      query = GetStringValue("query");
    } 
 }


/*public void doOps()
{
  FwkInfo( "doOps called." );

  ResetKey("entryCount");
  int entryCount = GetUIntValue( "entryCount" );
  ResetKey("NumNewKeys");
  int numNewKeys = GetUIntValue( "NumNewKeys" );

  //TestClient * clnt = TestClient::getTestClient();
  IRegion<TKey,TVal> regionPtr = GetRegion();
   if (regionPtr == null) {
    FwkSevere( "CacheServerTest::doOps(): No region to perform operations on." );
    //now = end; // Do not do the loop
  }
  ResetKey("useTransactions");
  bool m_istransaction = GetBoolValue("useTransactions");
  try
  {
      DoOpsTask<TKey, TVal> dooperation = new DoOpsTask<TKey, TVal>(regionPtr, m_istransaction);
      ResetKey("numThreads");
      int numThreads = GetUIntValue("numThreads");
      RunTask(dooperation, numThreads, entryCount+numNewKeys + NUM_EXTRA_KEYS, -1, -1, null);
  }
  catch (ClientTimeoutException)
  {
      FwkException("In doOps()  Timed run timed out.");
  }
  catch (Exception e)
  {
      FwkException("Caught unexpected exception during doOps: " + e);
  }
 

  FwkInfo("Done in doOps");
  Thread.Sleep( 10000 );
 }
*/
private CacheableHashSet verifyRegionSize(ExpectedRegionContents expectedRgnContents)
{
	IRegion<TKey,TVal> region = GetRegion();
    int numKeys = region.GetLocalView().Count;
	  //int expectedNumKeys = getIntValue( "expectedValue" );
      ResetKey("entryCount");
      ResetKey("NumNewKeys");
	  int entryCount = GetUIntValue( "entryCount" );
	  int numNewKeys = GetUIntValue( "NumNewKeys" );
      FwkInfo("Expecting exact size of region " + expectedRgnContents.exactSize());
      if (expectedRgnContents.exactSize() != numKeys)
      {
          FwkException("Expected " + expectedRgnContents.exactSize() + " keys, but there are " + numKeys);
	  }
    
	  CacheableHashSet keysToCheck = CacheableHashSet.Create();
	  for(int i = 0;i<entryCount+numNewKeys + NUM_EXTRA_KEYS;i++)
		    keysToCheck.Add(m_KeysA[i]);// check all keys in the keyList (including "extra" keys not in server)

	  ICollection<TKey> keyVec = region.GetLocalView().Keys;
      foreach (TKey cKey in keyVec)
	  	  keysToCheck.Add(cKey);// also check any keys in the region that are not in the keyList
	  /*if (isGetInitialValues)
	       expected = static_RI_ops_keysValues;
	     else
	       expected = static_RI_ops_none;
      */
	  return keysToCheck;
}
      public void DoVerifyRegionContentsBeforeOpsOnFeederClient()
      {
          FwkInfo("DoVerifyRegionContentsBeforeOpsOnFeederClient called.");
          FwkInfo("DoVerifyRegionContentsBeforeOpsOnFeederClient called expected ." + expectedRgnContents[4].toString());
          IRegion<TKey, TVal> region = GetRegion();
          int numKeys = region.Count;
          ResetKey("entryCount");
          int entryCount = GetUIntValue("entryCount");
          if (expectedRgnContents[4].exactSize() != numKeys)
          {
              FwkException("Expected " + expectedRgnContents[4].exactSize() + " keys, but there are " + numKeys);
          }
          CacheableHashSet keysToCheck = CacheableHashSet.Create();
          for (int i = 0; i < entryCount; i++)
              keysToCheck.Add(m_KeysA[i]);
          foreach (TKey key in keysToCheck)
          {
              try
              {
                  verifyEntry(key, expectedRgnContents[4]);
              }
              catch (Exception e)
              {
                  FwkException("Caught unexpected exception for key " + key + " during DoVerifyRegionContentsBeforeOpsOnFeederClient " + e);
              }
          }

      }

public void DoVerifyRegionContentsBeforeOps()
{
  FwkInfo( "verifyRegionContentsBeforeOps called." );
  FwkInfo("DoVerifyRegionContentsBeforeOps called expected ." + expectedRgnContents[0].toString());
  CacheableHashSet keysToCheck = verifyRegionSize(expectedRgnContents[0]);
  foreach (TKey key in keysToCheck)
  {
	  try {
          verifyEntry(key, expectedRgnContents[0]);
	   } catch (Exception e) {
           FwkException("Caught unexpected exceptionfor key " + key + " during verifyRegionContentsBeforeOps " + e);
	   }
  }
 
}

public void doVerifyRegionContentsAfterLateOps()
{
  FwkInfo( "verifyRegionContentsAfterLateOps called." );
  FwkInfo("doVerifyRegionContentsAfterLateOps called expected ." + expectedRgnContents[1].toString());
  CacheableHashSet keysToCheck = verifyRegionSize(expectedRgnContents[1]);

  foreach (TKey key in keysToCheck)
  {
	  try {
          verifyEntry(key, expectedRgnContents[1]);
	   } catch (Exception e) {
           FwkException("Caught unexpected exception  for key " + key + " during verifyRegionContentsAfterLateOps " + e);
	   }
  }
}

      public void doVerifyRegionContentsAfterOpsRI()
      {
          FwkInfo("doVerifyRegionContentsAfterOpsRI called.");
          if (expectedRgnContents != null)
              FwkInfo("doVerifyRegionContentsAfterOpsRI called expected ." + expectedRgnContents[2].toString());
          else
              FwkInfo("doVerifyRegionContentsAfterOpsRI called expected is null");
          CacheableHashSet keysToCheck = verifyRegionSize(expectedRgnContents[2]);
          foreach (TKey key in keysToCheck)
          {
              try
              {
                  verifyEntry(key, expectedRgnContents[2]);
              }
              catch (Exception e)
              {
                  FwkException("Caught unexpected exception  for key " + key + " during doVerifyRegionContentsAfterOpsRI " + e);
              }
          }
      }
      public void doVerifyRegionContentsDynamic()
      {
          FwkInfo("doVerifyRegionContentsDynamic called.");
          FwkInfo("doVerifyRegionContentsDynamic called expected ." + expectedRgnContents[3].toString());
          CacheableHashSet keysToCheck = verifyRegionSize(expectedRgnContents[3]);
          foreach (TKey key in keysToCheck)
          {
              try
              {
                  verifyEntry(key, expectedRgnContents[3]);
              }
              catch (Exception e)
              {
                  FwkException("Caught unexpected exception  for key " + key + " during doVerifyRegionContentsDynamic " + e);
              }
          }
      }

      public void DoVerifyRegionContents()
      {

          ResetKey("entryCount");
          ResetKey("NumNewKeys");
          int numNewKeys = GetUIntValue("NumNewKeys");
          int entryCount = GetUIntValue("entryCount");
          Int32 numDestroyed = (Int32)Util.BBGet("ImageBB", "Last_Destroy") - (Int32)Util.BBGet("ImageBB", "First_Destroy") + 1;

          totalNumKeys = entryCount + numNewKeys - numDestroyed;
          IRegion<TKey, TVal> regionPtr = GetRegion();
 
          TKey[] keys = (TKey[])regionPtr.Keys;
          //Int32 numKeys = regionPtr.Count;
          if (totalNumKeys != keys.Length)
          {
              FwkException("Expected " + totalNumKeys + " keys, but there are " + keys.Length);
          }
          try
          {
              for (int ii = 0; ii < (entryCount + numNewKeys); ii++)
              {
                  TKey key = m_KeysA[ii];
                  int i = ii + 1;
                  if ((i >= (int)Util.BBGet("ImageBB", "First_None")) &&
                            (i <= (int)Util.BBGet("ImageBB", "Last_None")) ||
                            (i >= (int)Util.BBGet("ImageBB", "First_Get")) &&
                             (i <= (int)Util.BBGet("ImageBB", "Last_Get")))
                  {
                      checkContainsKey(key, true, "key was untouched");
                      checkContainsValueForKey(key, true, "key was untouched");
                      TVal value = regionPtr[key];
                      checkValue(key, value);

                  }

                  else if ((i >= (int)Util.BBGet("ImageBB", "First_Invalidate")) &&
                          (i <= (int)Util.BBGet("ImageBB", "Last_Invalidate")))
                  {
                      checkContainsKey(key, true, "key was invalidated ");
                      checkContainsValueForKey(key, false, "key was invalidated ");

                  }
                  else if ((i >= (int)Util.BBGet("ImageBB", "First_LocalInvalidate")) &&
                        (i <= (int)Util.BBGet("ImageBB", "Last_LocalInvalidate")))
                  {
                      // this key was locally invalidated
                      checkContainsKey(key, true, "key was locally invalidated");
                      checkContainsValueForKey(key, true, "key was locally invalidated");

                      try
                      {
                          TVal value = regionPtr[key];
                          checkValue(key, value);
                      }
                      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
                      { }

                  }
                  else if ((i >= (int)Util.BBGet("ImageBB", "First_Destroy")) &&
                     (i <= (int)Util.BBGet("ImageBB", "Last_Destroy")))
                  {
                      // this key was destroyed
                      checkContainsKey(key, false, "key was destroyed");
                      checkContainsValueForKey(key, false, "key was destroyed");

                  }
                  else if ((i >= (int)Util.BBGet("ImageBB", "First_LocalDestroy")) &&
                        (i <= (int)Util.BBGet("ImageBB", "Last_LocalDestroy")))
                  {
                      // this key was locally destroyed
                      checkContainsKey(key, true, "key was locally destroyed");
                      checkContainsValueForKey(key, true, "key was locally destroyed");

                      TVal value = regionPtr[key];
                      checkValue(key, value);

                  }
                  else if ((i >= (int)Util.BBGet("ImageBB", "First_UpdateExistingKey")) &&
                        (i <= (int)Util.BBGet("ImageBB", "Last_UpdateExistingKey")))
                  {
                      // this key was updated
                      checkContainsKey(key, true, "key was updated");
                      checkContainsValueForKey(key, true, "key was updated");

                      TVal value = regionPtr[key];
                      checkUpdatedValue(key, value);

                  }
                  else if ((i > entryCount) && (i <= (entryCount + numNewKeys)))
                  {
                      // key was newly added
                      checkContainsKey(key, true, "key was new");
                      checkContainsValueForKey(key, true, "key was new");

                      TVal value = regionPtr[key];
                      checkValue(key, value);

                  }
                  else
                  { // key is outside of keyIntervals and new keys; it was never loaded
                      // key was never loaded
                      checkContainsKey(key, false, "key was never used");
                      checkContainsValueForKey(key, false, "key was never used");
                  }
              }
          }
          catch (Exception e)
          {
              FwkException("Caught unexpected exception  during doVerifyRegionContents: " + e);
          }
      }
      public void verifyEntry(TKey key, ExpectedRegionContents expectedRgnContents)
      {
        int i = 0;
        if(typeof(TKey) == typeof(int))
            i = (int)(object)key;
        
        
        int numNewKeys = GetUIntValue( "NumNewKeys" );

        int entryCount = GetUIntValue("entryCount");
        ResetKey("entryCount");
        ResetKey("NumNewKeys");
        IRegion<TKey, TVal> regionPtr = GetRegion();
        if ((i >= (int)Util.BBGet("ImageBB", "First_None")) &&
                  (i <= (int)Util.BBGet("ImageBB", "Last_None")))
        {
            checkContainsKey(key, expectedRgnContents.containsKey_none(), "key was untouched");
          checkContainsValueForKey(key, expectedRgnContents.containsValue_none(), "key was untouched");
          if (expectedRgnContents.getAllowed_none())
          {
              TVal value = regionPtr[key];
              checkValue(key, value);
          }
      }
      else if ((i >= (int)Util.BBGet("ImageBB", "First_Get")) &&
                   (i <= (int)Util.BBGet("ImageBB", "Last_Get")))
      {
          // this key was untouched after its creation
          checkContainsKey(key, expectedRgnContents.containsKey_get(), "get key");
          checkContainsValueForKey(key, expectedRgnContents.containsValue_get(), "get key");
          if (expectedRgnContents.getAllowed_get())
          {
              TVal value = regionPtr[key];
             checkValue(key, value);
          }
      }
      else if ((i >= (int)Util.BBGet("ImageBB", "First_Invalidate")) &&
    (i <= (int)Util.BBGet("ImageBB", "Last_Invalidate")))
      {
          checkContainsKey(key, expectedRgnContents.containsKey_invalidate(), "key was invalidated (Bug 35303)");
          bool expectValue = expectedRgnContents.containsValue_invalidate();
          checkContainsValueForKey(key, expectValue, "key was invalidated (Bug 35303)");
          if (expectedRgnContents.getAllowed_invalidate())
          {
              try
              {
                  TVal value = regionPtr[key];
                  if (expectValue)
                  {
                      checkValue(key, value);
                  }
                  else
                  {
                      if (value != null)
                      {
                          throw new Exception("Bug 35303, after calling get " + key.ToString() + ", expected invalidated value to be null but it is " + value.ToString());
                      }
                  }
              }
              catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
              {}
          }
      }
      else if ((i >= (int)Util.BBGet("ImageBB", "First_LocalInvalidate")) &&
    (i <= (int)Util.BBGet("ImageBB", "Last_LocalInvalidate")))
      {
          // this key was locally invalidated
          checkContainsKey(key, expectedRgnContents.containsKey_localInvalidate(), "key was locally invalidated");
          checkContainsValueForKey(key, expectedRgnContents.containsValue_localInvalidate(), "key was locally invalidated");
          if (expectedRgnContents.getAllowed_localInvalidate())
          {
              try
              {
                  TVal value = regionPtr[key];
                  checkValue(key, value);
              }
              catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
              { }
          }
      }
      else if ((i >= (int)Util.BBGet("ImageBB", "First_Destroy")) &&
    (i <= (int)Util.BBGet("ImageBB", "Last_Destroy")))
      {
          // this key was destroyed
          checkContainsKey(key, expectedRgnContents.containsKey_destroy(), "key was destroyed");
          bool expectValue = expectedRgnContents.containsValue_destroy();
          checkContainsValueForKey(key, expectValue, "key was destroyed");
          if (expectedRgnContents.getAllowed_destroy())
          {
              try
              {
                  TVal value = regionPtr[key];
                  if (expectValue)
                  {
                      checkValue(key, value);
                  }
                  else
                  {
                      if (value != null)
                      {
                          throw new Exception("Expected value for " + key.ToString() + " to be null");
                      }
                  }
              }
              catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
              { }
          }
      }
      else if ((i >= (int)Util.BBGet("ImageBB", "First_LocalDestroy")) &&
    (i <= (int)Util.BBGet("ImageBB", "Last_LocalDestroy")))
      {
          // this key was locally destroyed
          checkContainsKey(key, expectedRgnContents.containsKey_localDestroy(), "key was locally destroyed");
          checkContainsValueForKey(key, expectedRgnContents.containsValue_localDestroy(), "key was locally destroyed");
          if (expectedRgnContents.getAllowed_localDestroy())
          {
              TVal value = regionPtr[key];
             checkValue(key, value);
          }
      }
      else if ((i >= (int)Util.BBGet("ImageBB", "First_UpdateExistingKey")) &&
    (i <= (int)Util.BBGet("ImageBB", "Last_UpdateExistingKey")))
      {
          // this key was updated
          checkContainsKey(key, expectedRgnContents.containsKey_update(), "key was updated");
          checkContainsValueForKey(key, expectedRgnContents.containsValue_update(), "key was updated");
          if (expectedRgnContents.getAllowed_update())
          {
              TVal value = regionPtr[key];
              if (expectedRgnContents.valueIsUpdated())
              {
                checkUpdatedValue(key, value);
             } else {
                checkValue(key, value);
             }
          }
       } else if ((i > entryCount) && (i <= (entryCount + numNewKeys))) {
          // key was newly added
           checkContainsKey(key, expectedRgnContents.containsKey_newKey(), "key was new");
           checkContainsValueForKey(key, expectedRgnContents.containsValue_newKey(), "key was new");
           if (expectedRgnContents.getAllowed_newKey())
           {
              TVal value = regionPtr[key];
             checkValue(key, value);
          }
       } else { // key is outside of keyIntervals and new keys; it was never loaded
         // key was never loaded
         checkContainsKey(key, false, "key was never used");
         checkContainsValueForKey(key, false, "key was never used");
       }
}

private void checkContainsKey(TKey key, bool expected, string logStr){
   IRegion<TKey,TVal> region = GetRegion();
   bool containsKey = false;
   if (region.Attributes.CachingEnabled == true)
       containsKey = region.GetLocalView().ContainsKey(key);
   else
       containsKey = region.ContainsKey(key);
   if (containsKey != expected) {
	   FwkException("Expected containsKey(" + key.ToString() + ") to be " + expected +
			   ", but it was " + containsKey + ": " + logStr);
   }

}
private void checkContainsValueForKey(TKey key, bool expected,string logStr) {
   IRegion<TKey,TVal> region = GetRegion();
    bool containsValue = false;
    if (region.Attributes.CachingEnabled == true)
        containsValue = region.GetLocalView().ContainsValueForKey(key);
    else
        containsValue = region.ContainsValueForKey(key);
   if (containsValue != expected){
      FwkException("Expected containsValueForKey(" + key.ToString() + ") to be " + expected +
                ", but it was " + containsValue + ": " + logStr);
  }
}

private void checkValue(TKey key, TVal value)
{
    ResetKey("versionNum");
    int versionnum = GetUIntValue("versionNum");
    Type typ = value.GetType();
    if (typ == typeof(int))
    {
        if (!key.Equals(value))
            throw new Exception("Inconsistent Value for key " + key.ToString() + " Expected Value should be " + value.ToString());
    }
    else if (versionnum ==1 &&  (typ == PdxTests<TKey, TVal>.m_pdxVersionOneAsm.GetType("PdxVersionTests.PdxVersioned")))
    {
        string expectedValue = string.Format("PdxVersioned {0}",  key);
        //typ val = (typ.)value;
        object val = typ.GetProperty("PString").GetValue(value, null);
        if (!expectedValue.Equals(val.ToString()))
        {
            throw new Exception("Inconsistent Value for key " + key.ToString() + " Expected Value should be " + expectedValue +
                " but recevied value is " + val.ToString());
        }
    }
    else if (versionnum == 2 && (typ == PdxTests<TKey, TVal>.m_pdxVersionTwoAsm.GetType("PdxVersionTests.PdxVersioned")))
    {
        string expectedValue = string.Format("PdxVersioned {0}", key);
        //typ val = (typ.)value;
        object val = typ.GetProperty("PString").GetValue(value, null);
        if (!expectedValue.Equals(val.ToString()))
        {
            throw new Exception("Inconsistent Value for key " + key.ToString() + " Expected Value should be " + expectedValue +
                " but recevied value is " + val.ToString());
        }
    }
    else
        throw new Exception("Expected Value for key " + key.ToString() + " is not the same type of " + typ);
}

private void checkUpdatedValue(TKey key, TVal value)
{
    ResetKey("versionNum");
    int versionnum = GetUIntValue("versionNum");
    Type typ = value.GetType();
    if (typ == typeof(string))
    {
        string expectedStr = "updated_" + key;
        if (!expectedStr.Equals(value))
            throw new Exception("Inconsistent Value for key " + key.ToString() + " Expected Value should be " + expectedStr.ToString());
    }
    else if (versionnum == 1 && (typ == PdxTests<TKey, TVal>.m_pdxVersionOneAsm.GetType("PdxVersionTests.PdxVersioned")))
    {
        string expectedValue = string.Format("PdxVersioned updated_{0}", key);
        //typ val = (typ.)value;
        object val = typ.GetProperty("PString").GetValue(value, null);
        if (!expectedValue.Equals(val.ToString()))
        {
            throw new Exception("Inconsistent Value for key " + key.ToString() + " Expected Value should be " + expectedValue +
                " but recevied value is " + val.ToString());
        }
    }
    else if (versionnum == 2 && (typ == PdxTests<TKey, TVal>.m_pdxVersionTwoAsm.GetType("PdxVersionTests.PdxVersioned")))
    {
        string expectedValue = string.Format("PdxVersioned updated_{0}", key);
        //typ val = (typ.)value;
        object val = typ.GetProperty("PString").GetValue(value, null);
        if (!expectedValue.Equals(val.ToString()))
        {
            throw new Exception("Inconsistent Value for key " + key.ToString() + " Expected Value should be " + expectedValue +
                " but recevied value is " + val.ToString());
        }
    }
    else
    {
        throw new Exception("Expected Value for key " + key.ToString() + " is not the same type of " + typ);
    }

}
      public void DoWaitForSilenceListenerComplete()
      {
          int desiredSilenceSec = 30;
          int sleepMS = 2000;
          FwkInfo("Waiting for a period of silence for " + desiredSilenceSec + " seconds...");
          long desiredSilenceMS = desiredSilenceSec * 1000;
          DateTime startTime = DateTime.Now;
          long silenceStartTime = SmokePerf<TKey, TVal>.GetDateTimeMillis(startTime);
          long currentTime = SmokePerf<TKey, TVal>.GetDateTimeMillis(startTime);
          long lastEventTime = (long)Util.BBGet("ListenerBB", "lastEventTime");

          while (currentTime - silenceStartTime < desiredSilenceMS)
          {
              try
              {
                  Thread.Sleep(sleepMS);
              }
              catch (Exception e)
              {
                  FwkException("PerfTest::waitForSilence() Caught exception:" + e.Message);
              }
              lastEventTime = (long)Util.BBGet("ListenerBB", "lastEventTime");
              if (lastEventTime > silenceStartTime)
              {
                  // restart the wait
                  silenceStartTime = lastEventTime;
              }
              startTime = DateTime.Now;
              currentTime = SmokePerf<TKey, TVal>.GetDateTimeMillis(startTime);
          }
          long duration = currentTime - silenceStartTime;
          FwkInfo("Done waiting, clients have been silent for " + duration + " ms");

      }
    //-------------------------------------------------doOps for rvv end-------------

    public static void TestComplete()
    {
      OperationsMap.Clear();
      ExceptionsMap.Clear();
    }

    #endregion
  }

  public class Multiusersecurity<TKey, TVal> : FwkTest<TKey, TVal>
  {
    #region Private constants and statics

    private static Cache m_cache = null;
    private const string RegionName = "regionName";
    private const string ValueSizes = "valueSizes";
    private const string MultipleUser = "MultiUsers";
    private const string OpsSecond = "opsSecond";
    private const string EntryCount = "entryCount";
    private const string WorkTime = "workTime";
    private const string EntryOps = "entryOps";
    private const string isDurable = "isDurable";
    //private const string MultiUserMode = "multiUserMode";
    private const string KeyStoreFileProp = "security-keystorepath";
    private const string KeyStoreAliasProp = "security-alias";
    private const string KeyStorePasswordProp = "security-keystorepass";
    private const CredentialGenerator.ClassCode DefaultSecurityCode =
      CredentialGenerator.ClassCode.LDAP;
    private static Dictionary<string, IRegion<TKey, TVal>> proxyRegionMap = new Dictionary<string, IRegion<TKey, TVal>>();
    private static Dictionary<string, IRegionService> authCacheMap = new Dictionary<string, IRegionService>();
    private static Dictionary<string, Dictionary<string, int>> operationMap=new Dictionary<string,Dictionary<string,int>>();
    private static Dictionary<string, Dictionary<string, int>> exceptionMap=new Dictionary<string,Dictionary<string,int>>();

    private static ArrayList userList, readerList, queryList, writerList, adminList;  
    private static Dictionary<string, ArrayList> userToRolesMap=new Dictionary<string,ArrayList>();
  
     #endregion

    #region Private utility methods

    public virtual IRegion<TKey,TVal> DoCreateDCRegion()
    {
      FwkInfo("In DoCreateDCRegion() Durable");

      ClearCachedKeys();

      string rootRegionData = GetStringValue("regionSpec");
      string tagName = GetStringValue("TAG");
      string endpoints = Util.BBGet(JavaServerBB, EndPointTag + tagName)
        as string;
      if (rootRegionData != null && rootRegionData.Length > 0)
      {
        string rootRegionName;
        rootRegionName = GetRegionName(rootRegionData);
        if (rootRegionName != null && rootRegionName.Length > 0)
        {
          IRegion<TKey, TVal> region;
          if ((region = CacheHelper<TKey, TVal>.GetRegion(rootRegionName)) == null)
          {
            bool isDC = GetBoolValue("isDurable");
            string m_isPool = null;
            // Check if this is a thin-client region; if so set the endpoints
            int redundancyLevel = 0;
            if (endpoints != null && endpoints.Length > 0)
            {
              redundancyLevel = GetUIntValue(RedundancyLevelKey);
              if (redundancyLevel < 0)
                redundancyLevel = 0;
              string conflateEvents = GetStringValue(ConflateEventsKey);
              string durableClientId = "";
              int durableTimeout = 300;
              if (isDC)
              {
                durableTimeout = GetUIntValue("durableTimeout");
                bool isFeeder = GetBoolValue("isFeeder");
                if (isFeeder)
                {
                  durableClientId = "Feeder";
                  // VJR: Setting FeederKey because listener cannot read boolean isFeeder
                  // FeederKey is used later on by Verify task to identify feeder's key in BB
                  Util.BBSet("DURABLEBB", "FeederKey", "ClientName_" + Util.ClientNum + "_Count");
                }
                else
                {
                  durableClientId = String.Format("ClientName_{0}", Util.ClientNum);
                }
              }
              FwkInfo("DurableClientID is {0} and DurableTimeout is {1}", durableClientId, durableTimeout);
              CacheHelper<TKey, TVal>.InitConfigForPoolDurable(durableClientId, durableTimeout, conflateEvents, false);
              

            }
            RegionFactory rootAttrs = CacheHelper<TKey, TVal>.DCache.CreateRegionFactory(RegionShortcut.PROXY);
            SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
            rootAttrs = CreatePool(rootAttrs, redundancyLevel);
            FwkInfo("Entering CacheHelper.CreateRegion()");
            region = CacheHelper<TKey, TVal>.CreateRegion(rootRegionName, rootAttrs);
            GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> regAttr = region.Attributes;
            FwkInfo("Region attributes for {0}: {1}", rootRegionName,
              CacheHelper<TKey, TVal>.RegionAttributesToString(regAttr));
            if (isDC)
            {
              CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
            }
          }
          return region;
        }
        
      }
      else
      {
        FwkSevere("DoCreateDCRegion() failed to create region");
      }

      FwkInfo("DoCreateDCRegion() complete.");
      return null;
    }

    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        IRegion<TKey, TVal> region;
        if (operationMap.Count > 0 || exceptionMap.Count > 0)
        {
          operationMap.Clear();
          exceptionMap.Clear();
        }
        ResetKey(isDurable);
        bool isDC = GetBoolValue(isDurable);
        if (isDC)
        {
          region = DoCreateDCRegion();
        }
        else
        {
          region = CreateRootRegion();
        }
        FwkInfo("The region name is {0}",region.Name);

        if (region != null)
        {
          string poolName = region.Attributes.PoolName;

          if (poolName != null)
          {
            Pool pool = PoolManager.Find(poolName);
            if (pool.MultiuserAuthentication)
            {
              FwkInfo("pool is in multiuser mode and entering CreateMultiUserCacheAndRegion");
              CreateMultiUserCacheAndRegion(pool, region);
            }
            else
              FwkInfo(" pool is not in multiuser mode ");
          }
          else
            FwkInfo("poolName is null ");
        }
        else 
          FwkInfo(" returing null region");
        //return null;
      }
      catch (Exception ex)
      {
        FwkException("DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreateRegion() complete.");
    }

    void CreateMultiUserCacheAndRegion(Pool pool,IRegion<TKey,TVal> region) 
    {
      FwkInfo("In CreateMultiUserCacheAndRegion()");
      string userName;
      CredentialGenerator gen = GetCredentialGenerator();
      CredentialGenerator.ClassCode SecurityCode = gen.GetClassCode();
      gen = CredentialGenerator.Create(SecurityCode, true);
      if (gen == null)
      {
        FwkInfo("Skipping security scheme {0} with no generator. Using  default security scheme.{1}", SecurityCode, DefaultSecurityCode);
        SecurityCode = DefaultSecurityCode;
      }
      string scheme = SecurityCode.ToString();
      string user = GetStringValue(SecurityCode.ToString());
      string regionName=region.Name;
      ResetKey(MultipleUser);
      Int32 numOfMU = GetUIntValue(MultipleUser);
      userList = new ArrayList();
     //populating the list with no. of multiusers.
      for (Int32 i = 1; i <= numOfMU; i++)
      {
        string userStr = string.Format("{0}{1}",user,i);
        userList.Add(userStr);
      }

      Int32 userSize=userList.Count;
      if (SecurityCode == CredentialGenerator.ClassCode.LDAP)
      {
        for (Int32 i = 0; i < userSize; i++)
        {
          Properties<string, string> userProp = new Properties<string, string>();
          userName = (String)userList[i];

          userProp.Insert("security-username", userName);
          userProp.Insert("security-password", userName);
          IRegionService mu_cache = CacheHelper<TKey, TVal>.DCache.CreateAuthenticatedView(CacheHelper<TKey, TVal>.GetPkcsCredentialsForMU(userProp), pool.Name);
          authCacheMap[userName] = mu_cache;
          //mu_cache = pool.CreateSecureUserCache(userProp);
          IRegion<TKey,TVal> m_region = mu_cache.GetRegion<TKey,TVal>(regionName);
          proxyRegionMap[userName] =  m_region;
          Dictionary<string, int> opMAP=new Dictionary<string,int>();
          Dictionary<string, int> expMAP=new Dictionary<string,int>();
          operationMap[userName] = opMAP;
          exceptionMap[userName] = expMAP;
          Utility.GetClientProperties(gen.AuthInit, null, ref userProp);
          FwkInfo("Security properties entries: {0}", userProp);
          switch (i)
          {
            case 0:
            case 1:
              setAdminRole(userName);
             break;
            case 2:
            case 3:
            case 4:
              setReaderRole(userName);
              break;
            case 5:
            case 6:
            case 7:
              setWriterRole(userName);
              break;
            case 8:
            case 9:
              setQueryRole(userName);
              break;
          };
        }
      }
     else
      {
        FwkInfo("Security Scheme is {0}", SecurityCode);
        for (Int32 i = 0; i < userSize; i++)
        {
          Properties<string, string> userProp = new Properties<string, string>();
          PkcsAuthInit pkcs = new PkcsAuthInit();
          if (pkcs == null) {
            FwkException("NULL PKCS Credential Generator");
          }
          userName = (String)userList[i];
          string dataDir = Util.GetFwkLogDir(Util.SystemType) + "/data";
          userProp.Insert(KeyStoreFileProp, GetKeyStoreDir(dataDir) +
            userName + ".keystore");
          userProp.Insert(KeyStoreAliasProp, userName);
          userProp.Insert(KeyStorePasswordProp, "gemfire");
          //mu_cache = pool.CreateSecureUserCache(userProp);
          //IRegionService mu_cache = CacheHelper.DCache.CreateAuthenticatedView(userProp, pool.Name);
          IRegionService mu_cache = CacheHelper<TKey, TVal>.DCache.CreateAuthenticatedView(
            CacheHelper<TKey, TVal>.GetPkcsCredentialsForMU(
              pkcs.GetCredentials(userProp, "0:0")), pool.Name);
          authCacheMap.Add(userName, mu_cache);
          IRegion<TKey, TVal> m_region = mu_cache.GetRegion<TKey, TVal>(regionName);
          proxyRegionMap.Add(userName, m_region);
          Dictionary<string, int> opMAP = new Dictionary<string, int>();
          Dictionary<string, int> expMAP = new Dictionary<string, int>();
          operationMap[userName] =  opMAP;
          exceptionMap[userName] = expMAP;
          Utility.GetClientProperties(gen.AuthInit, null, ref userProp);
          FwkInfo("Security properties entries: {0}", userProp);
         switch (i)
          {
            case 0:
            case 1:
              setAdminRole(userName);
              break;
            case 2:
            case 3:
            case 4:
              setReaderRole(userName);
              break;
            case 5:
            case 6:
            case 7:
              setWriterRole(userName);
              break;
            case 8:
            case 9:
              setQueryRole(userName);
              break;
          };
        }
      }
    }

    public string GetKeyStoreDir(string dataDir)
    {
      string keystoreDir = dataDir;
      if (keystoreDir != null && keystoreDir.Length > 0)
      {
        keystoreDir += "/keystore/";
      }
      return keystoreDir;
    }


    public void setAdminRole(string userName)
    {
      adminList = new ArrayList();
      adminList.Add("create");
      adminList.Add("update");
      adminList.Add("get");
      adminList.Add("getServerKeys");
      adminList.Add("destroy");
      adminList.Add("query");
      adminList.Add("cq");
      adminList.Add("putAll");
      adminList.Add("executefunction");
      userToRolesMap.Add(userName,adminList);
    }

    public void setReaderRole(string userName)
    {
      readerList = new ArrayList();
      readerList.Add("get");
      readerList.Add("getServerKeys");  
      readerList.Add("cq");
      userToRolesMap.Add(userName,readerList);
    }

    public void setWriterRole(string userName)
    {
      writerList = new ArrayList();
      writerList.Add("create");
      writerList.Add("update");
      writerList.Add("destroy");
      writerList.Add("putAll");
      writerList.Add("executefunction");
      userToRolesMap.Add(userName, writerList);
    }

    public void setQueryRole(string userName)
    {
      queryList = new ArrayList();
      queryList.Add("query");
      queryList.Add("cq");
      userToRolesMap.Add(userName, queryList);
    }
   
       #region Public methods
   
    public void DoFeedTask()
    {
      FwkInfo("MultiUserSecurity.DoFeed() called.");
     
      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int cnt = 0;
      string userName=(String)userList[0];
      IRegion<TKey,TVal> region= proxyRegionMap[userName];
      while (cnt++ < entryCount)
      {
        string keyStr = cnt.ToString();
        string value = "Value";
        TKey key = (TKey)(object)keyStr;
        TVal val = (TVal)(object)value;
        region[key] = val;
      }
      FwkInfo("MultiUserSecurity.DoFeed() completed");
    }

    public void DoCqForMU()
    {
      FwkInfo("MultiUserSecurity.DoCqForMU() called ");
      CqQuery<TKey, object> cq;
      string uName = (String)userList[1];
      IRegion<TKey,TVal> region = proxyRegionMap[uName];
      IRegionService authCache = authCacheMap[uName];
      for (Int32 i = 0; i < userList.Count; i++)
      {
        string userName = (String)userList[i];
        string cqName = String.Format("cq-{0}", userName);
        QueryService<TKey, object> qs = authCache.GetQueryService<TKey, object>();
        CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
        ICqListener<TKey, object> cqLstner = new MyCqListener<TKey, object>();
        cqFac.AddCqListener(cqLstner);
        CqAttributes<TKey, object> cqAttr = cqFac.Create();
        cq = qs.NewCq(cqName, "select * from /Portfolios where TRUE", cqAttr, true);
        cq.Execute();
      }
      FwkInfo("MultiUserSecurity.DoCqForMU() completed");
    }

    public void DoCloseCacheAndReInitialize() {
      FwkInfo("MultiUserSecurity.DoCloseCacheAndReInitialize() called");
      try
      {
        //if (mu_cache != null)
        {
          IRegion<TKey, TVal>[] vregion = CacheHelper<TKey, TKey>.DCache.RootRegions<TKey, TVal>();
          try
          {
            for (Int32 i = 0; i < vregion.Length; i++)
            {
              IRegion<TKey, TVal> region = (IRegion<TKey, TVal>)vregion.GetValue(i);
              region.GetLocalView().DestroyRegion();
            }
          }
          catch (RegionDestroyedException ignore)
          {
            string message = ignore.Message;
            Util.Log(message);
          }
          catch (Exception ex)
          {
            FwkException("Caught unexpected exception during region local destroy {0}", ex);
          }
          bool keepalive = GetBoolValue("keepAlive");
          bool isDurable = GetBoolValue("isDurable");
          if (isDurable)
          {
            FwkInfo("KeepAlive is {0}", keepalive);
            CacheHelper<TKey, TVal>.DCache.Close(keepalive);
            //m_cache.Close(keepalive);            
          }
          else
            m_cache.Close();
          m_cache = null;
          FwkInfo("Cache Close");
        }
        proxyRegionMap.Clear();
        authCacheMap.Clear();
        operationMap.Clear();
        exceptionMap.Clear();
        userToRolesMap.Clear();
        userList.Clear();
        CacheHelper<TKey, TVal>.Close();
          DoCreateRegion();
        bool isCq = GetBoolValue("cq");
        if (isCq)
          DoCqForMU();
      }
      catch (CacheClosedException ignore)
      {
        string message = ignore.Message;
        Util.Log(message);
      }
      catch (Exception ex)
      {
        FwkException("Caught unexpected exception during CacheClose {0}", ex);
      }
    }

    public void DoValidateCqOperationsForPerUser() {
      FwkInfo("MultiUserSecurity.DoValidateCqOperationsForPerUser() called.");
      try
      {
        string uName = (String)userList[0];
        IRegion<TKey, TVal> region = proxyRegionMap[uName];
        IRegionService authCache = authCacheMap[uName];
        QueryService<TKey, object> qs = authCache.GetQueryService<TKey, object>();
        ICqListener<TKey, object> cqLstner = new MyCqListener<TKey, object>();
        for (Int32 i = 0; i < userList.Count; i++)
        {
          string userName = (String)userList[i];
          string cqName = String.Format("cq-{0}", userName);
          CqQuery<TKey, object> cq = qs.GetCq(cqName);
          CqStatistics cqStats = cq.GetStatistics();
          CqAttributes<TKey, object> cqAttr = cq.GetCqAttributes();
          ICqListener<TKey, object>[] vl = cqAttr.getCqListeners();
          cqLstner = vl[0];
          MyCqListener<TKey, object> myLisner = (MyCqListener<TKey, object>)cqLstner;
          Util.Log("My count for cq {0} Listener : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4}", cq.Name, myLisner.NumInserts(), myLisner.NumDestroys(), myLisner.NumUpdates(), myLisner.NumEvents());
          Util.Log("Cq {0} from CqStatistics : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4} ", cq.Name, cqStats.numInserts(), cqStats.numDeletes(), cqStats.numUpdates(), cqStats.numEvents());
          if (myLisner.NumInserts() == cqStats.numInserts() && myLisner.NumUpdates() == cqStats.numUpdates() && myLisner.NumDestroys() == cqStats.numDeletes() && myLisner.NumEvents() == cqStats.numEvents())
            Util.Log("Accumulative event count is correct");
          else
            Util.Log("Accumulative event count is incorrect");
        }
      }
      catch (Exception ex)
      {
        FwkException("Query.DoVerifyCQListenerInvoked() Caught Exception : {0}", ex);
      }
    }

    public void DoEntryOperationsForMU()
    {
      FwkInfo("MultiUserSecurity.DoEntryOperationsForMU() called.");
      Util.RegisterTestCompleteDelegate(TestComplete);
      // string bb = "GFE_BB" ;
     // string ky="scheme" ;
    //  string securitySch=(String)Util.BBGet(bb,ky);
      
      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      TKey key;
      TVal value;
      //IGFSerializable val;

      CqQuery<TKey, object> cq = null;
      string opCode = null;

      PaceMeter pm = new PaceMeter(opsSec);
      while (now < end)
      {
        int addOps = 1;
        Random rdm = new Random();
        int userSize = userList.Count;
        string userName = (String)userList[(rdm.Next(userSize))];
        FwkInfo("The userName is {0}",userName);
        IRegion<TKey,TVal> region = proxyRegionMap[userName];
        IRegionService authCache = authCacheMap[userName];
        opCode = GetStringValue(EntryOps);
        try
        {
          UpdateOperationMap(opCode,userName,1);
          if (opCode == null || opCode.Length == 0)
          {
            opCode = "no-op";
          }

          if (opCode == "create")
          {
            key = (TKey)(object)(rdm.Next(entryCount).ToString());
            string tempval = "Value";
            value = (TVal)(object)tempval;
            region.Add(key, value);
          }
          else
          {
            key = (TKey)(object)(rdm.Next(entryCount).ToString());
            if (opCode == "update")
            {
              value = (TVal)(object)"Value_";
              region[key] = value;
            }
            else if (opCode == "destroy")
            {
              region.Remove(key);
            }  
           
            else if (opCode == "get")
            {
                try
                {
                   value = region[key];
                }
                catch (EntryNotFoundException ex)
                {
                    FwkInfo("MultiUser.DoEntryOperationsForMU() Caught non-fatal " +
                      "ex-ception in read: {0}", ex.Message);
                }
                catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex)
                {
                    FwkInfo("MultiUser.DoEntryOperationsForMU() Caught non-fatal " +
                      "ex-ception in read: {0}", ex.Message);
                }
            }

            else if (opCode == "getServerKeys")
            {
              ICollection<TKey> serverKeys = region.Keys;
            }
             
            else if (opCode == "query")
            {
              QueryService<TKey, object> qs = authCache.GetQueryService<TKey, object>();
              Query<object> qry = qs.NewQuery("select distinct * from /Portfolios where FALSE");
              ISelectResults<object> result = qry.Execute(600);
            }
            else if (opCode == "cq")
            {
              string cqName = String.Format("cq-{0}",userName);
              QueryService<TKey, object> qs = authCache.GetQueryService<TKey, object>();
              CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
              ICqListener<TKey, object> cqLstner = new MyCqListener<TKey, object>();
              cqFac.AddCqListener(cqLstner);
              CqAttributes<TKey, object> cqAttr = cqFac.Create();
              cq = qs.NewCq(cqName, "select * from /Portfolios where FALSE", cqAttr, false);
              cq.Execute();
              cq.Stop();
              cq.Execute();
              cq.Close();
            }
            else if (opCode == "executefunction")
            {
              bool getResult = true;
              string funcName = null;
              Random rdn = new Random();
              int num = rdn.Next(3);
              GemStone.GemFire.Cache.Generic.Execution<object> exc = null;
              ICollection<object> executeFunctionResult = null;
              ArrayList args = new ArrayList();
              Object[] filterObj = new Object[1];
              filterObj[0] = rdm.Next(entryCount).ToString();
              //args.Add(filterObj[0]);
              Util.Log("Inside FE num = {0}",num);
              if (num == 0)
              {
                args.Add(filterObj[0]);
                args.Add("addKey"); 
                funcName = "RegionOperationsFunction";
                exc = Generic.FunctionService<object>.OnRegion(region);
                executeFunctionResult = exc.WithArgs(args).WithFilter(filterObj).Execute(funcName, 15).GetResult();
              }
              else if (num == 1)
              {
                args.Add(filterObj[0]);
                funcName = "ServerOperationsFunction";
                //exc = region.Cache.GetFunctionService().OnServer();
                exc = Generic.FunctionService<object>.OnServer(authCache);
                executeFunctionResult = exc.WithArgs(args).Execute(funcName, 15).GetResult();
              }
              else
              {
                try
                {
                  args.Add(filterObj[0]);
                  funcName = "ServerOperationsFunction";
                  //exc = region.Cache.GetFunctionService().OnServers();
                  exc = Generic.FunctionService<object>.OnServers(authCache);
                  executeFunctionResult = exc.WithArgs(args).Execute(funcName, 15).GetResult();
                }
                catch (FunctionExecutionException)
                {
                  //expected exception
                  Util.Log("Inside FunctionExecutionException");
                }
              }
            }
            else if (opCode == "putAll")
            {
              IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();
              for (int count = 0; count < 200; count++)
              {
                key = (TKey)(object)count.ToString();
                value = (TVal)(object)"Value_";
                map.Add(key, value);
              }
              region.PutAll(map);
            }
            else
            {
              FwkException("CacheServer.DoEntryOperationsForSecurity() " +
                "Invalid operation specified: {0}", opCode);
            }
          }
        }
        catch (NotAuthorizedException ex)
        {
         
          FwkInfo("Got expected NotAuthorizedException for operation {0}: {1}",
            opCode, ex.Message);
          if (opCode == "cq")
          {
            if (cq.IsStopped())
              cq.Close();
          }
          UpdateExceptionMap(opCode,userName,1);
        }
        catch (EntryExistsException)
        {
          addOps = -1;
          
       UpdateOperationMap(opCode,userName,addOps);
        }
        catch (EntryNotFoundException)
        {
          addOps = -1;
        UpdateExceptionMap(opCode,userName,addOps);
        }
        catch (EntryDestroyedException)
        {
          addOps = -1;
          UpdateExceptionMap(opCode, userName, addOps);
        }
        catch (CqExistsException)
        {
          Util.Log("Inside CqExistsException ");
        }
        catch (TimeoutException ex)
        {
          FwkSevere("Caught unexpected timeout exception during entry {0} " +
            " operation: {1}; continuing with test.", opCode, ex.Message);
        }
        catch (Exception ex)
        {
          FwkException("MultiUserSecurity.DoEntryOperationsForSecurity() Caught " +
            "unexpected exception during entry '{0}' operation: {1}.",
            opCode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      key = default(TKey);
      value = default(TVal);
      FwkInfo("MultiUserSecurity.DoEntryOperationsForSecurity() complete");
    }

    public void UpdateOperationMap(string opcode, string userName,int numOps)
    {
      FwkInfo("Inside updateOperationMap");
      lock (((IDictionary)operationMap).SyncRoot)
      {
        Dictionary<string, int> opMap = operationMap[userName];
        int currentOps;
        if (!opMap.TryGetValue(opcode, out currentOps))
        {
          currentOps = 0;
        }
        opMap[opcode] = currentOps + numOps;
        operationMap[userName] = opMap;
      }
    }

    public void UpdateExceptionMap(string opcode, string userName, int numOps)
    {
      lock (((IDictionary)operationMap).SyncRoot)

      {
        Dictionary<string, int> expMap = exceptionMap[userName];
        int currentOps;
        if (!expMap.TryGetValue(opcode, out currentOps))
        {
          currentOps = 0;
        }
        expMap[opcode] = currentOps + numOps;
        exceptionMap[userName]=expMap;
      }
    }
    public int GetOpsFromMap(Dictionary<string, int> map, string opCode)
    {
      int numOps;
      lock (((ICollection)map).SyncRoot)
      {
        if (!map.TryGetValue(opCode, out numOps))
        {
          numOps = 0;
        }
      }
      return numOps;
    }

    public void DoValidateEntryOperationsForPerUser()
    {
      FwkInfo("Multiusersecurity.DoValidateEntryOperationsForPerUser() started");
      bool opFound = false;
      for (int i = 0; i < userList.Count; i++)
      {
        Dictionary<string, int> validateOpMap;
        Dictionary<string, int> validateExpMap;
        string userName = (String)userList[i];
        validateOpMap = operationMap[userName];
        validateExpMap = exceptionMap[userName];
        foreach (string opcode in validateOpMap.Keys) 
        {
          FwkInfo("opcode is {0} for user {1}", opcode,userName);
          int totalOpCnt = GetOpsFromMap(validateOpMap,opcode);
          int totalNotAuthCnt = GetOpsFromMap(validateExpMap, opcode);
          ArrayList roleList = new ArrayList();
          roleList = userToRolesMap[userName];
          for (int j = 0; j < roleList.Count; j++)
          {
            if ((String)roleList[j] == opcode)
            {
              opFound = true;
              break;
            }
            else
              opFound = false;
          }
          if (opFound)
          {
            if ((totalOpCnt != 0) && (totalNotAuthCnt == 0))
            {
              FwkInfo("Task passed sucessfully with total operation = {0} for user {1}", totalOpCnt, userName);
            }
            else
            {
              FwkException("Task failed for user {0} NotAuthorizedException found for operation = {1} while expected was 0", userName, totalNotAuthCnt);
            }
          }
          else
          {
            if (totalOpCnt == totalNotAuthCnt)
            {
              FwkInfo("Task passed sucessfully and got the expected number of notAuth exception = {0} with total number of operation = {1}", totalOpCnt, totalNotAuthCnt);
            }
            else
            {
              FwkException("Task failed ,Expected NotAuthorizedException cnt to be = {0} but found = {1}",totalOpCnt,totalNotAuthCnt);
            }
          }
        }
      }
    }

    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      userToRolesMap.Clear();
      proxyRegionMap.Clear();
      authCacheMap.Clear();
      CacheHelper<TKey,TVal>.Close();
    }
    public static void TestComplete()
    {
      operationMap.Clear();
      exceptionMap.Clear();
    }

    #endregion
 
  }
   #endregion
 
}

