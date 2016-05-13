//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Collections;
using System.Collections.ObjectModel;
using System.Text;
using System.Threading;
using System.Reflection;
using PdxTests;


namespace GemStone.GemFire.Cache.FwkLib
{
    using GemStone.GemFire.DUnitFramework;
    using GemStone.GemFire.Cache.Tests.NewAPI;
    using GemStone.GemFire.Cache.Generic;
    using QueryCategory = GemStone.GemFire.Cache.Tests.QueryCategory;
    using QueryStrings = GemStone.GemFire.Cache.Tests.QueryStrings;
    using QueryStatics = GemStone.GemFire.Cache.Tests.QueryStatics;
    using System.IO;

    public class PDXSilenceListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>
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
    public class PDXMyCqListener<TKey, TResult> : ICqListener<TKey, TResult>
    {
        private UInt32 m_updateCnt;
        private UInt32 m_createCnt;
        private UInt32 m_destroyCnt;
        private UInt32 m_eventCnt;

        public PDXMyCqListener()
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
            }
            else if (opType == CqOperationType.OP_TYPE_UPDATE)
            {
                m_updateCnt++;
            }
            else if (opType == CqOperationType.OP_TYPE_DESTROY)
            {
                m_destroyCnt++;
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
    public class PdxEntryTask<TKey, TVal> : ClientTask
    {
        #region Private members
        /*
        private IRegion<TKey, TVal> m_region;
        private int m_MaxKeys;
        private List<IDictionary<TKey, TVal>> m_maps;
        private Int32 m_create;
        private Int32 m_update;
        private Int32 m_destroy;
        private Int32 m_invalidate;
        private Int32 m_cnt;
        bool m_isDestroy;
        private object CLASS_LOCK = new object();
        */
        private IRegion<TKey, TVal> m_region;
        private int m_MaxKeys;
        private static bool m_istransaction = false;
        private static bool isSerialExecution = false;
        protected static bool isEmptyClient = false;  // true if this is a bridge client with empty dataPolicy
        protected static bool isThinClient = false; // true if this is a bridge client with eviction to keep it small
        private static Dictionary<TKey, TVal> regionSnapshot = null;
        private static List<TKey> destroyedKeys = null;// = new List<TKey>();
        private static int keyCount = 0;
        private static string m_sharePath;
        private Int32 create;
        private Int32 update;
        private Int32 destroy;
        private Int32 invalidate;
        private Int32 localdestroy;
        private Int32 localinvalidate;
        private Int32 putall;
        private Int32 get;
        private string m_objectType;
        private int m_versionNum;
        private object CLASS_LOCK = new object();
        #endregion

        public PdxEntryTask(IRegion<TKey, TVal> region, int keyCnt,
            Dictionary<TKey, TVal> maps, List<TKey> listval, bool serialexe,
            string objecttype, int versionnum)
            : base()
        {
            m_region = region;
            m_MaxKeys = keyCnt;
            regionSnapshot = maps;
            destroyedKeys = listval;
            isSerialExecution = serialexe;
            create = 0;
            update = 0;
            destroy = 0;
            invalidate = 0;
            localdestroy = 0;
            localinvalidate = 0;
            putall = 0;
            get = 0;
            m_objectType = objecttype;
            m_versionNum = versionnum;

        }
        protected TVal createIpdxInstance()
        {
            PdxType pt = new PdxType();

            IPdxInstanceFactory pif = CacheHelper<TKey, TVal>.DCache.CreatePdxInstanceFactory("PdxType");

            pif.WriteInt("m_int32", pt.Int32);
            pif.WriteString("m_string", pt.PString);
            pif.WriteObject("m_arraylist", pt.Arraylist);
            pif.WriteChar("m_char", pt.Char);
            pif.WriteBoolean("m_bool", pt.Bool);
            pif.WriteByte("m_sbyte", pt.Sbyte);
            pif.WriteByte("m_byte", pt.Byte);
            pif.WriteShort("m_int16", pt.Int16);
            pif.WriteByteArray("m_byteArray", pt.ByteArray);
            pif.WriteLong("m_long", pt.Long);
            pif.WriteFloat("m_float", pt.Float);
            pif.WriteDouble("m_double", pt.Double);
            pif.WriteBooleanArray("m_boolArray", pt.BoolArray);
            pif.WriteByteArray("m_sbyteArray", pt.SbyteArray);
            pif.WriteCharArray("m_charArray", pt.CharArray);
            pif.WriteDate("m_dateTime", pt.DateTime);
            pif.WriteShortArray("m_int16Array", pt.Int16Array);
            pif.WriteIntArray("m_int32Array", pt.Int32Array);
            pif.WriteLongArray("m_longArray", pt.LongArray);
            pif.WriteFloatArray("m_floatArray", pt.FloatArray);
            pif.WriteDoubleArray("m_doubleArray", pt.DoubleArray);
            pif.WriteArrayOfByteArrays("m_byteByteArray", pt.ByteByteArray);
            pif.WriteStringArray("m_stringArray", pt.StringArray);
            pif.WriteObject("m_map", pt.Map);
            pif.WriteObject("m_hashtable", pt.Hashtable);
            pif.WriteObject("m_vector", pt.Vector);
            pif.WriteObject("m_chs", pt.Chs);
            pif.WriteObject("m_clhs", pt.Clhs);
            pif.WriteInt("m_uint32", pt.Uint32);
            pif.WriteLong("m_ulong", pt.Ulong);
            pif.WriteShort("m_uint16", pt.Uint16);
            pif.WriteIntArray("m_uint32Array", pt.Uint32Array);
            pif.WriteLongArray("m_ulongArray", pt.UlongArray);
            pif.WriteShortArray("m_uint16Array", pt.Uint16Array);
            pif.WriteByteArray("m_byte252", pt.Byte252);
            pif.WriteByteArray("m_byte253", pt.Byte253);
            pif.WriteByteArray("m_byte65535", pt.Byte65535);
            pif.WriteByteArray("m_byte65536", pt.Byte65536);
            pif.WriteObject("m_pdxEnum", pt.PdxEnum);

            pif.WriteObject("m_address", pt.AddressArray);
            pif.WriteObjectArray("m_objectArray", pt.ObjectArray);
            IPdxInstance pi = pif.Create();
            return (TVal)(object)pi;
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
            string sharedpath = (string)Util.BBGet("SharedPath", "sharedDir");
            m_sharePath = Path.Combine(sharedpath ,"framework/csharp/bin");
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
                    PdxTests<TKey,TVal>.m_pdxVersionOneAsm = Assembly.LoadFrom(Path.Combine(m_sharePath ,"PdxVersion1Lib.dll"));
                    Type pt = PdxTests<TKey,TVal>.m_pdxVersionOneAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
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
                else if (m_objectType.Equals("PdxInstanceFactory"))
                {

                    tmpValue = createIpdxInstance();
                }
                else if (m_objectType.Equals("AutoSerilizer"))
                {
                    tmpValue = (TVal)(object)new SerializePdx1(true);
                }
            }
            else
                tmpValue = (TVal)(object)value;
            return tmpValue;

        }
        protected void verifyContainsKey(IRegion<TKey, TVal> m_region, TKey key, bool expected)
        {
            bool containsKey = false;
            if (isEmptyClient || m_istransaction)
            {
                containsKey = m_region.ContainsKey(key);
            }
            else
            {
                containsKey = m_region.GetLocalView().ContainsKey(key);
            }
            if (containsKey != expected)
            {
                throw new Exception("Expected ContainsKey() for " + key + " to be " + expected +
                          " in " + m_region.FullPath + ", but it is " + containsKey);
            }
        }

        protected void verifyContainsValueForKey(IRegion<TKey, TVal> m_region, TKey key, bool expected)
        {

           bool containsValueForKey = false;
            if (isEmptyClient || m_istransaction)
                containsValueForKey = m_region.ContainsValueForKey(key);
            else
                containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
            if (containsValueForKey != expected)
            {
                throw new Exception("Expected ContainsValueForKey() for " + key + " to be " + expected +
                          " in " + m_region.FullPath + ", but it is " + containsValueForKey);
            }

        }
        protected void verifySize(IRegion<TKey, TVal> m_region, int expectedSize)
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
                    FwkTest<TKey, TVal>.CurrentTest.FwkInfo(sb.ToString());
                }
                throw new Exception("Expected size of " + m_region.FullPath + " to be " +
                   expectedSize + ", but it is " + size);
            }
            
        }
        
        protected TKey GetNewKey()
        {
                FwkTest<TKey, TVal>.CurrentTest.ResetKey("distinctKeys");
                int numKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("distinctKeys");
                String keybuf = String.Format("Key-{0}", keyCount);
                TKey key = (TKey)(object)(keybuf);
                keyCount++;
                return key;
            
        }
        protected TKey GetExistingKey(bool useServerKeys, int index)
        {
            IRegion<TKey, TVal> region = m_region;
            TKey key = default(TKey);
            TKey[] keys = null;
            int size = 0;
            if (useServerKeys)
            {
                size = region.Count;
                keys = (TKey[])region.Keys;
                index = Util.Rand(0, keys.Length);
                try
                {
                  if (keys.Length > 0)
                  {
                    key = keys[index];
                  }
                }
                catch (Exception e) { FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught {0} during GetExistingKey()",e.Message); }
            }
            else
            {
                size = region.GetLocalView().Count;
                keys = (TKey[])region.GetLocalView().Keys;
                index = Util.Rand(0, keys.Length);
                if (keys.Length > 0)
                {
                  key = keys[index];
                }
            }
           
            return key;
        }
        protected void validate(TKey key,TVal value,int beforeSize)
        {
            if (isSerialExecution)
            {
                if (isEmptyClient)
                {
                    if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
                    {
                        verifySize(m_region, 0);
                    }
                }
                else if (isThinClient)
                {
                    if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
                    {
                        // new entry should be in the local region
                        verifyContainsKey(m_region, key, true);
                        verifyContainsValueForKey(m_region, key, true);
                    }
                }
                else
                { // region has all keys/values
                    verifyContainsKey(m_region, key, true);
                    verifyContainsValueForKey(m_region, key, true);
                    verifySize(m_region, beforeSize);
                }
                regionSnapshot[key] = value;
                destroyedKeys.Remove(key);
            }
        }
        protected void addEntry(IRegion<TKey, TVal> m_region, int index)
        {
                object keybuf = null;
                if (typeof(TKey) == typeof(int))
                    keybuf = (int)(object)index;
                else
                keybuf = String.Format("Key-{0}", index);
                TKey key = (TKey)(object)(keybuf);
                TVal value = GetValue(key);
                int beforeSize = 0;
                if (isEmptyClient || m_istransaction)
                    beforeSize = m_region.Count;
                else
                    beforeSize = m_region.GetLocalView().Count;

               if (!m_region.ContainsKey(key))
                m_region.Add(key, value);
               else
                {
                    value = GetValue(("updated_" + key));
                    m_region[key] = value;
                }
                // validation
                validate(key,value,beforeSize+1);
            
       
        }
        
        protected void updateEntry(IRegion<TKey, TVal> r,int index)
        {
            TKey key = GetExistingKey(isEmptyClient || isThinClient, index);
            if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
            {
                int size = r.Count;
                if (isSerialExecution && (size != 0))
                    throw new Exception("getExistingKey returned " + key + ", but region size is " + size);
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("updateEntry: No keys in region");
                return;
            }
            int beforeSize = 0;
            if (isEmptyClient || m_istransaction)
                beforeSize = r.Count;
            else
                beforeSize = r.GetLocalView().Count;
            TVal value = GetValue(("updated_" + key));
            r[key] = value;

            // validation
            // cannot validate return value from put due to bug 36436; in peer configurations
            // we do not make any guarantees about the return value
            validate(key,value,beforeSize);
           
        }
        
        protected void invalidateEntry(IRegion<TKey, TVal> m_region, bool isLocalInvalidate, int index)
        {
            int beforeSize = 0;
            if (isEmptyClient || m_istransaction)
                beforeSize = m_region.Count;
            else
                beforeSize = m_region.GetLocalView().Count;
            TKey key = GetExistingKey(isEmptyClient || isThinClient, index);
            if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
            {
                if (isSerialExecution && (beforeSize != 0))
                    throw new Exception("getExistingKey returned " + key + ", but region size is " + beforeSize);
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("invalidateEntry: No keys in region");
                return;
            }
            bool containsKey = m_region.GetLocalView().ContainsKey(key);
            bool containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
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
                            verifySize(m_region, 0);
                        }
                    }
                    else if (isThinClient)
                    { // we have eviction
                        if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
                        {
                            verifySize(m_region, beforeSize);
                        }
                    }
                    else
                    { // region has all keys/values
                        verifyContainsKey(m_region, key, true);
                        verifyContainsValueForKey(m_region, key, false);
                        verifySize(m_region, beforeSize);
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
            catch (Exception e)
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkException("Invalide operation caught {0}",e.Message);
            }
        }
        /** Get an existing key in the given region if one is available,
         *  otherwise get a new key. 
         *
         *  @param aRegion The region to use for getting an entry.
         */
        protected void getKey(IRegion<TKey, TVal> aRegion,int index)
        {
            TKey key = GetExistingKey(isEmptyClient || isThinClient,index);
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
                anObj = (TVal)aRegion[key];
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
                    foreach (KeyValuePair<TKey, TVal> kvp in regionSnapshot)
                    {
                        TKey mapkey = kvp.Key;

                        if (key.Equals(mapkey))
                        {
                            expectedValue = kvp.Value;
                            if (!EqualityComparer<TVal>.Default.Equals(actualValue, default(TVal)))
                            {
                                if (!actualValue.Equals(expectedValue))
                                {
                                    FwkTest<TKey, TVal>.CurrentTest.FwkException("getKey: expected value {0} is not same as actual value {1} for key {2}",
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
        protected void destroyEntry(IRegion<TKey, TVal> m_region, bool isLocalDestroy, int index)
        {
            TKey key = GetExistingKey(isEmptyClient || isThinClient, index);
            if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
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
                            verifySize(m_region, 0);
                        }
                    }
                    else if (isThinClient)
                    { // we have eviction
                        if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
                        {
                            verifyContainsKey(m_region, key, false);
                            verifyContainsValueForKey(m_region, key, false);
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
                        verifyContainsKey(m_region, key, false);
                        verifyContainsValueForKey(m_region, key, false);
                        verifySize(m_region, beforeSize - 1);
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
        protected void putAll(IRegion<TKey, TVal> r, int index)
        {
            // determine the number of new keys to put in the putAll
            int beforeSize = 0;
            if (isEmptyClient || m_istransaction)
                beforeSize = r.Count;
            else
                beforeSize = r.GetLocalView().Count;
            int localBeforeSize = r.GetLocalView().Count;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("numPutAllExistingKeys");
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("numPutAllNewKeys");
            int numNewKeysToPut = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("numPutAllNewKeys");
            // get a map to put
            IDictionary<TKey, TVal> mapToPut = new Dictionary<TKey, TVal>();
            int valSize = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("valueSizes");
            int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
            valSize = ((valSize < minValSize) ? minValSize : valSize);
            string valBuf = null;
            valBuf = new string('A', valSize);
            StringBuilder newKeys = new StringBuilder();
            for (int i = 0; i < numNewKeysToPut; i++)
            {
                TKey key = GetNewKey();
                TVal value = GetValue(key);
                mapToPut[key] = value;
                newKeys.Append(key + " ");
                if ((i % 10) == 0)
                {
                    newKeys.Append("\n");
                }
            }


            // add existing keys to the map
            int numPutAllExistingKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("numPutAllExistingKeys");
            StringBuilder existingKeys = new StringBuilder();
            if (numPutAllExistingKeys > 0)
            {
                for (int i = 0; i < numPutAllExistingKeys; i++)
                { // put existing keys
                    TKey key = GetExistingKey(isEmptyClient || isThinClient,i);
                    TVal anObj = GetValue(key);
                    mapToPut[key] = anObj;
                    existingKeys.Append(key + " ");
                    if (((i + 1) % 10) == 0)
                    {
                        existingKeys.Append("\n");
                    }
                }
            }
            FwkTest<TKey, TVal>.CurrentTest.FwkInfo("PR size is " + beforeSize + ", local region size is " +
                localBeforeSize + ", map to use as argument to putAll is " +
                mapToPut.GetType().Name + " containing " + numNewKeysToPut + " new keys and " +
                numPutAllExistingKeys + " existing keys (updates); total map size is " + mapToPut.Count +
                "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
   
            // do the putAll
            FwkTest<TKey, TVal>.CurrentTest.FwkInfo("putAll: calling putAll with map of " + mapToPut.Count + " entries");
            r.PutAll(mapToPut, 60);

            FwkTest<TKey, TVal>.CurrentTest.FwkInfo("putAll: done calling putAll with map of " + mapToPut.Count + " entries");

            // validation
            if (isSerialExecution)
            {
                if (isEmptyClient)
                {
                    if (!CacheHelper<TKey, TVal>.DCache.CacheTransactionManager.Exists())
                    {
                        verifySize(r, 0);
                    }
                }
                else
                { // we have all keys/values in the local region
                    verifySize(r, beforeSize + numNewKeysToPut);
                }
                foreach (KeyValuePair<TKey, TVal> kvp in regionSnapshot)
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
        public override void DoTask(int iters, object data)
        {

            int offset = Util.Rand(m_MaxKeys);
            int count = offset;
            int size = 0;
            Util.Log("PdxEntryTask::DoTask: starting {0} iterations and maxkey {1} count {2}.", iters, m_MaxKeys, count);
            isEmptyClient = !(m_region.Attributes.CachingEnabled);
            isThinClient = m_region.Attributes.CachingEnabled;
            while (Running && (iters-- != 0))
            {
                int idx = count % m_MaxKeys;
                
                size = m_region.Count;
               
                //FwkTest<TKey, TVal>.CurrentTest.ResetKey("entryOps");
                string opcode = FwkTest<TKey, TVal>.CurrentTest.GetStringValue("entryOps");
                Util.Log("OpCode is {0}",opcode);
                lock (CLASS_LOCK)
                {
                    //if (((size < 1) && (opcode != "create")) || (opcode == "create"))
                    if(opcode == "create")
                    {
                        try
                        {
                            addEntry(m_region, idx+1);
                            create++;
                        }
                        catch (EntryExistsException e)
                        {
                            if (isSerialExecution)
                        {
                                FwkTest<TKey, TVal>.CurrentTest.FwkException(e.Message);
                            }
                            else
                                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Got {0} which is expected with concurrent execution",e.Message);
                        }
                    }
                    else if (opcode == "update")
                    {
                        updateEntry(m_region, idx);
                        update++;
                    }
                    else if (opcode == "destroy")
                    {
                        destroyEntry(m_region, false, idx);
                        destroy++;
                    }
                    else if (opcode == "localDestroy")
                    {
                        destroyEntry(m_region, true, idx);
                        localdestroy++;
                    }
                    else if (opcode == "invalidate")
                    {
                        invalidateEntry(m_region, false, idx);
                        invalidate++;
                   }
                    else if (opcode == "localInvalidate")
                    {
                        invalidateEntry(m_region, true, idx);
                        localinvalidate++;
                    }
                    else if (opcode == "putAll")
                    {
                        putAll(m_region, idx);
                        putall++;
                    }
                    else if (opcode == "get")
                    {
                        getKey(m_region, idx);
                        get++;
                    }
                    else
                    {
                        FwkTest<TKey, TVal>.CurrentTest.FwkException("PdxEntryTask:DoTask() Invalid operation " +
                        "specified: {0}", opcode);
                    }
                }
                count++;
            }
            Interlocked.Add(ref m_iters, count - offset);
            FwkTest<TKey, TVal>.CurrentTest.FwkInfo("DoEntryOP: create = {0}, update = {1}, destroy = {2}, localdestroy = {3},"
                + "invalidate = {4},localinvalidate = {5}, get = {6}, putall = {7} and RegionSize = {8}",
                create, update, destroy, localinvalidate, invalidate, localinvalidate, get, putall,m_region.Count);
        }

    }

    public class PdxTypeMapper : IPdxTypeMapper
    {

        public string ToPdxTypeName(string localTypeName)
        {
            return localTypeName;
        }

        public string FromPdxTypeName(string pdxTypeName)
        {
            Util.Log("pdxTypeName = {0}", pdxTypeName);
            if (pdxTypeName.Equals("PdxTests.PdxVersioned"))
            {
               return "PdxVersionTests.PdxVersioned";

            }
            else if (pdxTypeName.Equals("PdxTests.pdxEnumTest"))
            {
               return "PdxVersionTests.pdxEnumTest";
            }
            else
                return pdxTypeName;
        }
    }

    public class PdxTests<TKey, TVal> : FwkTest<TKey, TVal>
    {
        #region Private constants and statics

        protected const string ClientCount = "clientCount";
        protected const string TimedInterval = "timedInterval";
        protected const string DistinctKeys = "distinctKeys";
        protected const string NumThreads = "numThreads";
        protected const string ValueSizes = "valueSizes";
        protected const string KeyType = "keyType";
        protected const string KeySize = "keySize";
        private const string RegionName = "regionName";
        private const string OpsSecond = "opsSecond";
        private const string EntryCount = "entryCount";
        private const string WorkTime = "workTime";
        private const string EntryOps = "entryOps";
        private const string LargeSetQuery = "largeSetQuery";
        private const string UnsupportedPRQuery = "unsupportedPRQuery";
        private const string ObjectType = "objectType";
        
        private static Dictionary<string, int> OperationsMap =
          new Dictionary<string, int>();
        private static Dictionary<string, int> ExceptionsMap =
          new Dictionary<string, int>();
        private static bool m_istransaction = false;
        private static bool isSerialExecution = false;
        protected static bool isEmptyClient = false;  // true if this is a bridge client with empty dataPolicy
        protected static bool isThinClient = false; // true if this is a bridge client with eviction to keep it small
        private static Dictionary<TKey, TVal> regionSnapshot = null;
        private static List<TKey> destroyedKeys = null;// = new List<TKey>();
        public static Assembly m_pdxVersionOneAsm;
        public static Assembly m_pdxVersionTwoAsm;
        private static string m_sharePath;

        protected static string objectType = null;
        protected static int versionnum = 1;
        #endregion

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
            FwkTest<TKey, TVal>.CurrentTest.ResetKey(ObjectType);
            string objectType = GetStringValue(ObjectType);
            QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
            int numSet = 0;
            int setSize = 0;
            if (objectType != null && objectType == "Portfolio")
            {
                setSize = qh.PortfolioSetSize;
                numSet = max / setSize;
                return (TKey)(object)String.Format("port{0}-{1}", Util.Rand(numSet), Util.Rand(setSize));
            }
            else if (objectType != null && objectType == "Position")
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
            FwkTest<TKey, TVal>.CurrentTest.ResetKey(EntryCount);
            int numOfKeys = GetUIntValue(EntryCount);
            FwkTest<TKey, TVal>.CurrentTest.ResetKey(ValueSizes);
            int objSize = GetUIntValue(ValueSizes);
            QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
            int numSet = 0;
            int setSize = 0;
            if (objType != null && objType == "Portfolio")
            {
                setSize = qh.PortfolioSetSize;
                numSet = numOfKeys / setSize;
                usrObj = (TVal)(object)new Portfolio(Util.Rand(setSize), objSize);
            }
            else if (objType != null && objType == "Position")
            {
                setSize = qh.PositionSetSize;
                numSet = numOfKeys / setSize;
                int numSecIds = Portfolio.SecIds.Length;
                usrObj = (TVal)(object)new Position(Portfolio.SecIds[setSize % numSecIds], setSize * 100);
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
                    results = qry.Execute(paramList, 600);
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
                FwkTest<TKey, TVal>.CurrentTest.ResetKey(EntryCount);
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
                remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, false, false);
                i = Util.Rand(QueryStrings.SSsize);
                currentQuery = QueryStatics.StructSetQueries[i];
                remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, false, false);
                i = Util.Rand(QueryStrings.RSPsize);
                currentQuery = QueryStatics.ResultSetParamQueries[i];
                remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, true, false);
                i = Util.Rand(QueryStrings.SSPsize);
                currentQuery = QueryStatics.StructSetParamQueries[i];
                remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, true, true);
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
            IRegion<TKey, TVal> region = GetRegion();
            IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();
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
                        port = (TVal)(object)new Portfolio(current, valSize);
                        string Id = String.Format("port{0}-{1}", set, current);
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
            region.PutAll(map, 60);
        }

        private void GetAllOps()
        {
            IRegion<TKey, TVal> region = GetRegion();
            List<TKey> keys = new List<TKey>();
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
        public static ICacheListener<TKey, TVal> CreateSilenceListenerPdx()
        {
            return new PDXSilenceListener<TKey, TVal>();
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
            try
            {
                IRegion<TKey, TVal> region = CreateRootRegion();
                ResetKey("useTransactions");
                m_istransaction = GetBoolValue("useTransactions");
                isEmptyClient = !(region.Attributes.CachingEnabled);
                isThinClient = region.Attributes.CachingEnabled;
                objectType = GetStringValue(ObjectType);
                versionnum = GetUIntValue("versionNum");
                string sharedpath = (string)Util.BBGet("SharedPath", "sharedDir");
                m_sharePath = Path.Combine(sharedpath , "framework/csharp/bin");
                if (objectType != null)
                {
                    if (objectType.Equals("PdxVersioned") && versionnum == 1)
                    {
                        m_pdxVersionOneAsm = Assembly.LoadFrom(Path.Combine(m_sharePath, "PdxVersion1Lib.dll"));
                        Serializable.RegisterPdxType(registerPdxTypeOne);
                        Serializable.SetPdxTypeMapper(new PdxTypeMapper());
                    }
                    else if (objectType.Equals("PdxVersioned") && versionnum == 2)
                    {
                        m_pdxVersionTwoAsm = Assembly.LoadFrom(Path.Combine(m_sharePath, "PdxVersion2Lib.dll"));
                        Serializable.RegisterPdxType(registerPdxTypeTwo);
                        Serializable.SetPdxTypeMapper(new PdxTypeMapper());
                    }
                    else if (objectType.Equals("Nested"))
                    {
                        Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
                        Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);
                    }
                    else if (objectType.Equals("PdxType"))
                    {
                        Serializable.RegisterPdxType(PdxType.CreateDeserializable);
                    }
                    else if (objectType.Equals("AutoSerilizer"))
                    {
                        Serializable.RegisterPdxSerializer(new ReflectionBasedAutoSerializer());
                    }
                }
                isSerialExecution = GetBoolValue("serialExecution");
                if (isSerialExecution)
                {
                    regionSnapshot = new Dictionary<TKey, TVal>();
                    destroyedKeys = new List<TKey>();
                }
                if (region == null)
                {
                    FwkException("DoCreateRegion()  could not create region.");
                }

                FwkInfo("DoCreateRegion()  Created region '{0}'", region.Name);
            }
            catch (Exception ex)
            {
                FwkException("DoCreateRegion() Caught Exception: {0}", ex);
            }
            FwkInfo("DoCreateRegion() complete.");
        }
        IPdxSerializable registerPdxTypeOne()
        {
            Type pt = m_pdxVersionOneAsm.GetType("PdxVersionTests.PdxVersioned");

            object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

            return (IPdxSerializable)ob;
        }
        IPdxSerializable registerPdxTypeTwo()
        {
            Type pt = m_pdxVersionTwoAsm.GetType("PdxVersionTests.PdxVersioned");

            object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

            return (IPdxSerializable)ob;
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
                region.GetSubscriptionService().RegisterAllKeys(isDurable, null, isGetInitialValues, isReceiveValues);
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

            IRegion<TKey, TVal> region = GetRegion();
            PaceMeter pm = new PaceMeter(opsSec);
            while (cnt++ < entryCount)
            {
                AddValue(region, cnt, Encoding.ASCII.GetBytes(valBuf));
                pm.CheckPace();
            }
        }

        // ----------------------- begin pdx test related tasks -----------

        /*
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
        protected TVal GetValue()
        {
            TVal tmpValue = default(TVal);
            ResetKey("valueSizes");
            int size = GetUIntValue("valueSizes");
            StringBuilder builder = new StringBuilder();
            Random random = new Random();
            char ch;
            for (int j = 0; j < size; j++)
            {
              ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
              builder.Append(ch);
            }
            ResetKey("objectType");
            string objectType = GetStringValue(ObjectType);
            ResetKey("elementSize");
            int elementSize = GetUIntValue("elementSize");
            ResetKey("versionNum");
            int versionnum = GetUIntValue("versionNum"); //random.Next(2) + 1;// (counter++ % 2) + 1;
            if (typeof(TVal) == typeof(string))
            {
              tmpValue = (TVal)(object)builder.ToString();
            }
            else if (typeof(TVal) == typeof(byte[]))
            {
              tmpValue = (TVal)(object)(Encoding.ASCII.GetBytes(builder.ToString()));
            }
            else if (objectType.Equals("PdxVersioned") && versionnum == 1)
            {
              FwkInfo("rjk ---11222 {0}", elementSize);
              m_pdxVesionOneAsm = Assembly.LoadFrom(m_sharePath + "/PdxVersion1Lib.dll");
              Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
              FwkInfo("rjk ---11223");
              tmpValue = (TVal)pt.InvokeMember("PdxVersioned", BindingFlags.CreateInstance, null, null, null);
              FwkInfo("rjk ---11224 {0}", tmpValue);
            }
            else if (objectType.Equals("PdxVersioned") && versionnum == 2)
            {
              m_pdxVesionTwoAsm = Assembly.LoadFrom(m_sharePath + "/PdxVersion2Lib.dll");
              Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxVersioned", true, true);
              tmpValue = (TVal)pt.InvokeMember("PdxVersioned", BindingFlags.CreateInstance, null, null, null);
             }
             FwkInfo("rjk: PdxVersioned {0}", versionnum);
            return tmpValue;
      
        }
           */
        protected void verifyContainsKey(IRegion<TKey, TVal> m_region, TKey key, bool expected)
        {
            //bool containsKey = m_region.GetLocalView().ContainsKey(key);

            bool containsKey = false;
            if (isEmptyClient || m_istransaction)
            {
                containsKey = m_region.ContainsKey(key);
            }
            else
            {
                containsKey = m_region.GetLocalView().ContainsKey(key);
            }
            if (containsKey != expected)
            {
                throw new Exception("Expected ContainsKey() for " + key + " to be " + expected +
                          " in " + m_region.FullPath + ", but it is " + containsKey);
            }
        }

        protected void verifyContainsValueForKey(IRegion<TKey, TVal> m_region, TKey key, bool expected)
        {

            //bool containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
            bool containsValueForKey = false;
            if (isEmptyClient || m_istransaction)
                containsValueForKey = m_region.ContainsValueForKey(key);
            else
                containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
            Util.Log("Expected val is {0} and containsValueForKey is {1}", expected, containsValueForKey);
            if (containsValueForKey != expected)
            {
                throw new Exception("Expected ContainsValueForKey() for " + key + " to be " + expected +
                          " in " + m_region.FullPath + ", but it is " + containsValueForKey);
            }

        }
        protected void verifySize(IRegion<TKey, TVal> m_region, int expectedSize)
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
            // rjk:need to comment this block
            //int size = m_region.Count;
            //if (size != expectedSize)
            //{
            //  throw new Exception("Expected size of " + m_region.FullPath + " to be " +
            //      expectedSize + ", but it is " + size);
            // }
        }
        /*
       protected TKey GetNewKey()
       {
         ResetKey("distinctKeys");
         int numKeys = GetUIntValue("distinctKeys");
         String keybuf = String.Format("Key-{0}-{1}-{2}",Util.PID,Util.ThreadID, keyCount);
         FwkInfo("rjk: key is {0}", keybuf);
         TKey key = (TKey)(object)(keybuf);
         keyCount++;
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
             FwkInfo("rjk: GetExistingKey useServerKeys size = {0} ", size);
             key = keys[Util.Rand(0, size)];
           }
           else
           {
             int size = region.GetLocalView().Count;
             TKey[] keys = (TKey[])region.GetLocalView().Keys;
             FwkInfo("rjk: GetExistingKey size = {0} ", size);
             key = keys[Util.Rand(0, size)];
           }
      
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
           FwkInfo("rjk:addEntry - key = {0} value = {1}", key, value);
           m_region.Add(key, value);
        
         }
         catch (EntryExistsException ex)
         {
           if (isSerialExecution)
           {
             // cannot get this exception; nobody else can have this key
             FwkInfo("rjk:addEntry is throwing exception");
             throw new Exception(ex.StackTrace);
           }
           else
           {
             Util.Log("Caught {0} (expected with concurrent execution); continuing with test", ex);
           }
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
         FwkInfo("rjk:updateEntry - key = {0} value = {1}", key, value);
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
         FwkInfo("invalidateEntry:containsKey for " + key + ": " + containsKey);
         FwkInfo("invalidateEntry:containsValueForKey for " + key + ": " + containsValueForKey);
         try
         {
           if (isLocalInvalidate)
           { // do a local invalidate
               FwkInfo("rjk:invalidateEntry: doing local invalidate");
             m_region.GetLocalView().Invalidate(key);
           }
           else
           { // do a distributed invalidate
               FwkInfo("rjk:invalidateEntry: doing invalidate");
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
   // Get an existing key in the given region if one is available,
    //  otherwise get a new key. 
    //
   //  @param aRegion The region to use for getting an entry.
    //
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
             TVal value = (TVal)(object)Encoding.ASCII.GetBytes(valBuf);
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
               TVal anObj = (TVal)(object)Encoding.ASCII.GetBytes(valBuf);
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
        foreach (KeyValuePair<TKey, TVal> kvp in regionSnapshot)
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
           FwkSevere("PdxTests.doEntryOps(): No region to perform operations on.");
           now = end; // Do not do the loop
         }
         int size = 0;
         int create = 0, update = 0, destroy = 0, invalidate = 0, localdestroy = 0, localinvalidate = 0,
           get = 0, putall=0;
         CacheTransactionManager txManager = null;
         while (now < end)
         {
           try
           {
             if (isEmptyClient || m_istransaction)
               size = region.Count;
             else
               size = region.GetLocalView().Count;
           if (m_istransaction)
           {
             FwkInfo("rjk:doEntryOps transaction begin");
             txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
             txManager.Begin();
           }
           opcode = GetStringValue("entryOps");
           FwkInfo("rjk:doEntryOps ops is {0} and size is {1}", opcode,size);
           if (opcode == null) opcode = "no-opcode";
           //int addKeyCount = Util.BBIncrement("CreateOpCount", "CreateKeyCount");
        
           if (((size < 1) && (opcode != "create")) || (opcode == "create"))
           {
             FwkInfo("rjk:doEntryOps ops is - create and opcode is {0}", opcode);
             opcode = "create";
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
             FwkException("PdxTests.doEntryOps() Invalid operation " +
               "specified: {0}", opcode);
           }
           //removeDuplicates(destroyedKeys);
           if (m_istransaction && !rolledback)
           {
             try
             {
               FwkInfo("rjk:doEntryOps starting transaction commit");
               txManager.Commit();
               FwkInfo("rjk:doEntryOps transaction commit");
             }
             catch (CommitConflictException)
             {
               // can occur with concurrent execution
               Util.Log("Caught CommitConflictException. Expected with concurrent execution, continuing test.");
             }
             catch (Exception ex)
             {
               throw new Exception("Caught unexpected in doEntry : {0}", ex);
             }
           }
           }
           catch (Exception ex)
           {
             FwkException("PdxTests.doEntryOps() Caught unexpected " +
               "exception during entry '{0}' operation: {1}.", opcode, ex);
           }
           now = DateTime.Now;
         }
         FwkInfo("DoEntryOP: create = {0}, update = {1}, destroy = {2}, localdestroy = {3}"
            + ",invalidate = {4},localinvalidate = {5}, get = {6}, putall = {7}",
           create, update, destroy, localinvalidate, invalidate, localinvalidate, get, putall);
       }*/
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
        public void DoRandomEntryOperation()
        {
            FwkInfo("In DoRandomEntryOperation");
            try
            {
                IRegion<TKey, TVal> region = GetRegion();
                int timedInterval = GetTimeValue("timedInterval") * 1000;
                if (timedInterval <= 0)
                {
                    timedInterval = 5000;
                }
                int maxTime = 10 * timedInterval;

                // Loop over key set sizes
                ResetKey("distinctKeys");
                int numKeys = GetUIntValue("distinctKeys");
                bool isdone = false;
                Util.BBSet("RoundPositionBB", "done", false);
                string clntid = null;
                int roundPosition = 0;
                if (isSerialExecution)
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
                    if (clntid.Equals(Util.ClientId))
                    {
                      PdxEntryTask<TKey, TVal> entrytask = new PdxEntryTask<TKey, TVal>(region, numKeys, regionSnapshot,
                                destroyedKeys, isSerialExecution, objectType, versionnum);
                      if (isSerialExecution)
                              {
                                  RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
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
                                  RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
                                  break;
                              }
                              FwkInfo("Done PdxEntryTask operation");

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
                                        verifyFromSnapshot();
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
                                FwkInfo("Done Verification and verifyCount = {0}",(int)Util.BBGet("RoundPositionBB", "VerifyCnt"));
                            
                        }
                        if (isSerialExecution)
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
        public void verifyFromSnapshot()
        {
            try
            {
                verifyFromSnapshotOnly();
            }
            catch (Exception e)
            {
                FwkException(e.Message);
            }
            //verifyInternalPRState();
        }
        public void verifyFromSnapshotOnly()
        {
            IRegion<TKey, TVal> aRegion = GetRegion();
            if (isEmptyClient)
            {
                verifyServerKeysFromSnapshot();
                return;
            }
            StringBuilder aStr = new StringBuilder();
            regionSnapshot = (Dictionary<TKey, TVal>)Util.BBGet("RegionSnapshot", "regionSnapshot");
            int snapshotSize = regionSnapshot.Count;
            int regionSize = 0;
            if (isEmptyClient || m_istransaction)
                regionSize = aRegion.Count;
            else
                regionSize = aRegion.GetLocalView().Count;
            //int regionSize = aRegion.GetLocalView().Count;
            FwkInfo("Verifying from snapshot containing " + snapshotSize + " entries...");
            if (snapshotSize != regionSize)
            {
                FwkException("Expected region " + aRegion.FullPath + " to be size " + snapshotSize +
                     ", but it is " + regionSize.ToString() + "\n");
            }
            foreach (KeyValuePair<TKey, TVal> kvp in regionSnapshot)
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
                    verifyContainsValueForKey(aRegion, key, !(EqualityComparer<TVal>.Default.Equals(expectedValue, default(TVal))));
                }
                catch (Exception e)
                {
                    FwkException(e.Message + "\n");
                }

                // do a get on the partitioned region if a loader won't get invoked; test its value
                if (containsValueForKey)
                {
                    // loader won't be invoked if we have a value for this key (whether or not a loader
                    // is installed), or if we don't have a loader at all
                    try
                    {
                        TVal actualValue = aRegion[key];
                        if (!actualValue.Equals(expectedValue))
                        {
                            FwkException("verifyFromSnapshotOnly: expected value {0} is not same as actual value {1} for key {3}",
                              expectedValue, actualValue, key);
                        }
                       
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
        public void dumpDataOnBB()
        {
            IRegion<TKey, TVal> aRegion = GetRegion();
            ICollection<TKey> serverkeys = aRegion.Keys;
            regionSnapshot = new Dictionary<TKey, TVal>();
            foreach (TKey key in serverkeys)
            {
                TVal value = default(TVal); ;
                try
                {
                    value = aRegion[key];
                }
                catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
                {
                    value = default(TVal);
                }
                regionSnapshot[key] = value;
            }
            Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
        }
        public void verifyServerKeysFromSnapshot()
        {
            IRegion<TKey, TVal> aRegion = GetRegion();
            StringBuilder aStr = new StringBuilder();
            regionSnapshot = (Dictionary<TKey, TVal>)Util.BBGet("RegionSnapshot", "regionSnapshot");
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
                TVal expectedValue = (TVal)(Object)kvp.Value;
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
                            TVal valueOnServer = (TVal)(Object)aRegion[key];

                            //if (!EqualityComparer<TVal>.Default.Equals(valueOnServer, default(TVal)))
                            //{
                            if (!valueOnServer.Equals(expectedValue))
                            {
                                FwkException("verifyServerKeysFromSnapshot: expected value {0} is not same as actual value {1} for key {2}",
                                  expectedValue, valueOnServer, key);
                            }
                            FwkInfo("verifyServerKeysFromSnapshot: expected value {0} is same as actual value {1} for key {2}",
                                expectedValue, valueOnServer, key);
                            //}
                        }
                        catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
                        {
                        }
                        catch (EntryNotFoundException)
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
        
        public void DoPopulateRegion()
        {
            FwkInfo("In DoPopulateRegion()");
            try
            {
                IRegion<TKey, TVal> region = GetRegion();
                ResetKey("distinctKeys");
                int numKeys = GetUIntValue("distinctKeys");
                ResetKey("versionNum");
                versionnum = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("versionNum");
                PdxEntryTask<TKey, TVal> addtask = new PdxEntryTask<TKey, TVal>(region, numKeys,
                    regionSnapshot, destroyedKeys, isSerialExecution, objectType, versionnum);
                FwkInfo("Populating region for {0} keys.", numKeys);
                RunTask(addtask, 1, numKeys, -1, -1, null);
                Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
                Util.BBSet("DestroyedKeys", "destroyedKeys", destroyedKeys);
            }
            catch (Exception ex)
            {
                FwkException("DoPopulateRegion() Caught Exception: {0}", ex);
            }
            FwkInfo("DoPopulateRegion() complete.");
        }

        public void DoPuts()
        {
            FwkInfo("In DoPuts()");
            try
            {
                IRegion<TKey, TVal> region = GetRegion();
                int timedInterval = GetTimeValue("timedInterval") * 1000;
                if (timedInterval <= 0)
                {
                    timedInterval = 5000;
                }
                int maxTime = 10 * timedInterval;

                // Loop over key set sizes
                ResetKey("versionNum");
                versionnum = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("versionNum");
                ResetKey(DistinctKeys);
                int numKeys;
                while ((numKeys = GetUIntValue(DistinctKeys)) > 0)
                { // keys loop
                    // Loop over value sizes
                    ResetKey(ValueSizes);
                    int valSize;
                    while ((valSize = GetUIntValue(ValueSizes)) > 0)
                    { // value loop
                        // Loop over threads
                        ResetKey(NumThreads);
                        int numThreads;
                        while ((numThreads = GetUIntValue(NumThreads)) > 0)
                        {
                            try
                            {
                              PdxEntryTask<TKey, TVal> puts = new PdxEntryTask<TKey, TVal>(region, numKeys,
                                    regionSnapshot, destroyedKeys, isSerialExecution, objectType, versionnum);

                                FwkInfo("Running warmup task for {0} iterations.", numKeys);
                                RunTask(puts, 1, numKeys, -1, -1, null);
                                // Running the warmup task
                                Thread.Sleep(3000);
                                // And we do the real work now
                                FwkInfo("Running timed task for {0} secs and {1} threads; numKeys[{2}]",
                                  timedInterval / 1000, numThreads, numKeys);
                                //SetTaskRunInfo(label, "Puts", m_maxKeys, numClients,
                                //  valSize, numThreads);
                                RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
                                Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
                                Util.BBSet("DestroyedKeys", "destroyedKeys", destroyedKeys);
                                //AddTaskRunRecord(puts.Iterations, puts.ElapsedTime);

                            }
                            catch (ClientTimeoutException)
                            {
                                FwkException("In DoPuts()  Timed run timed out.");
                            }
                            Thread.Sleep(3000); // Put a marker of inactivity in the stats
                        }
                        Thread.Sleep(3000); // Put a marker of inactivity in the stats
                    } // value loop
                    Thread.Sleep(3000); // Put a marker of inactivity in the stats
                } // keys loop
            }
            catch (Exception ex)
            {
                FwkException("DoPuts() Caught Exception: {0}", ex);
            }
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
            FwkInfo("DoPuts() complete.");
        }
        public void DoGets()
        {
            FwkInfo("In DoGets()");
            try
            {
                IRegion<TKey, TVal> region = GetRegion();
                int numClients = GetUIntValue(ClientCount);
                string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
                int timedInterval = GetTimeValue(TimedInterval) * 1000;
                if (timedInterval <= 0)
                {
                    timedInterval = 5000;
                }
                int maxTime = 10 * timedInterval;

                ResetKey(DistinctKeys);
                int numKeys = GetUIntValue(DistinctKeys);
                ResetKey("versionNum");
                versionnum = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("versionNum");
                
                int valSize = GetUIntValue(ValueSizes);
                // Loop over threads
                ResetKey(NumThreads);
                int numThreads;
                while ((numThreads = GetUIntValue(NumThreads)) > 0)
                { // thread loop

                    // And we do the real work now
                    PdxEntryTask<TKey, TVal> gets = new PdxEntryTask<TKey, TVal>(region, numKeys,
                        regionSnapshot, destroyedKeys, isSerialExecution, objectType, versionnum);
                    FwkInfo("Running warmup task for {0} iterations.", numKeys);
                    RunTask(gets, 1, numKeys, -1, -1, null);
                    region.GetLocalView().InvalidateRegion();
                    Thread.Sleep(3000);
                    FwkInfo("Running timed task for {0} secs and {1} threads.",
                      timedInterval / 1000, numThreads);
                    //SetTaskRunInfo(label, "Gets", m_maxKeys, numClients, valSize, numThreads);
                    try
                    {
                        RunTask(gets, numThreads, -1, timedInterval, maxTime, null);
                    }
                    catch (ClientTimeoutException)
                    {
                        FwkException("In DoGets()  Timed run timed out.");
                    }
                    //AddTaskRunRecord(gets.Iterations, gets.ElapsedTime);
                    // real work complete for this pass thru the loop
                    Util.BBSet("RegionSnapshot", "regionSnapshot", regionSnapshot);
                    Util.BBSet("DestroyedKeys", "destroyedKeys", destroyedKeys);
                    Thread.Sleep(3000);
                } // thread loop
            }
            catch (Exception ex)
            {
                FwkException("DoGets() Caught Exception: {0}", ex);
            }
            Thread.Sleep(3000);
            FwkInfo("DoGets() complete.");
        }

        void doAccessPdxInstanceAndVerify()
        {
            Serializable.RegisterPdxType(PdxType.CreateDeserializable);
            FwkInfo("In doAccessPdxInstanceAndVerify");
            IRegion<TKey, TVal> region0 = GetRegion();
            PdxType dPdxType = new PdxType();
            ResetKey(DistinctKeys);
            int numKeys = GetUIntValue(DistinctKeys);
            int size = region0.Keys.Count;
            TKey[] keys = (TKey[])region0.Keys;
            TKey key = default(TKey);
            try
            {
                if(size != numKeys){
                    FwkException("doAccessPdxInstanceAndVerify() number of entries {0} on server is not same as expected {1}", size, numKeys);
                }
                for (int index = 0; index < numKeys; index++)
                {
                    key = keys[index];
                    bool containsValueForKey = false;
                    if (isEmptyClient || m_istransaction)
                        containsValueForKey = region0.ContainsValueForKey(key);
                    else
                        containsValueForKey = region0.GetLocalView().ContainsValueForKey(key);
                    IPdxInstance ret = (IPdxInstance)region0[key];
                    string retStr = (string)ret.GetField("m_string");
                    PdxType.GenericValCompare(dPdxType.PString, retStr);

                    PdxType.GenericValCompare((char)ret.GetField("m_char"), dPdxType.Char);

                    byte[][] baa = (byte[][])ret.GetField("m_byteByteArray");
                    PdxType.compareByteByteArray(baa, dPdxType.ByteByteArray);

                    PdxType.GenericCompare((char[])ret.GetField("m_charArray"), dPdxType.CharArray);

                    bool bl = (bool)ret.GetField("m_bool");
                    PdxType.GenericValCompare(bl, dPdxType.Bool);
                    PdxType.GenericCompare((bool[])ret.GetField("m_boolArray"), dPdxType.BoolArray);

                    PdxType.GenericValCompare((sbyte)ret.GetField("m_byte"), dPdxType.Byte);
                    PdxType.GenericCompare((byte[])ret.GetField("m_byteArray"), dPdxType.ByteArray);


                    List<object> tmpl = (List<object>)ret.GetField("m_arraylist");

                    PdxType.compareCompareCollection(tmpl, dPdxType.Arraylist);

                    IDictionary<object, object> tmpM = (IDictionary<object, object>)ret.GetField("m_map");
                    if (tmpM.Count != dPdxType.Map.Count)
                        throw new IllegalStateException("Not got expected value for type: " + dPdxType.Map.GetType().ToString());

                    Hashtable tmpH = (Hashtable)ret.GetField("m_hashtable");

                    if (tmpH.Count != dPdxType.Hashtable.Count)
                        throw new IllegalStateException("Not got expected value for type: " + dPdxType.Hashtable.GetType().ToString());

                    ArrayList arrAl = (ArrayList)ret.GetField("m_vector");

                    if (arrAl.Count != dPdxType.Vector.Count)
                        throw new IllegalStateException("Not got expected value for type: " + dPdxType.Vector.GetType().ToString());

                    CacheableHashSet rmpChs = (CacheableHashSet)ret.GetField("m_chs");

                    if (rmpChs.Count != dPdxType.Chs.Count)
                        throw new IllegalStateException("Not got expected value for type: " + dPdxType.Chs.GetType().ToString());

                    CacheableLinkedHashSet rmpClhs = (CacheableLinkedHashSet)ret.GetField("m_clhs");

                    if (rmpClhs.Count != dPdxType.Clhs.Count)
                        throw new IllegalStateException("Not got expected value for type: " + dPdxType.Clhs.GetType().ToString());


                    PdxType.GenericValCompare((string)ret.GetField("m_string"), dPdxType.String);

                    PdxType.compareData((DateTime)ret.GetField("m_dateTime"), dPdxType.DateTime);

                    PdxType.GenericValCompare((double)ret.GetField("m_double"), dPdxType.Double);

                    PdxType.GenericCompare((long[])ret.GetField("m_longArray"), dPdxType.LongArray);
                    PdxType.GenericCompare((Int16[])ret.GetField("m_int16Array"), dPdxType.Int16Array);
                    PdxType.GenericValCompare((sbyte)ret.GetField("m_sbyte"), dPdxType.Sbyte);
                    PdxType.GenericCompare((byte[])ret.GetField("m_sbyteArray"), dPdxType.SbyteArray);
                    PdxType.GenericCompare((string[])ret.GetField("m_stringArray"), dPdxType.StringArray);
                    PdxType.GenericValCompare((Int16)ret.GetField("m_uint16"), dPdxType.Uint16);
                    PdxType.GenericValCompare((int)ret.GetField("m_uint32"), dPdxType.Uint32);
                    PdxType.GenericValCompare((long)ret.GetField("m_ulong"), dPdxType.Ulong);
                    PdxType.GenericCompare((int[])ret.GetField("m_uint32Array"), dPdxType.Uint32Array);

                    PdxType.GenericCompare((double[])ret.GetField("m_doubleArray"), dPdxType.DoubleArray);
                    PdxType.GenericValCompare((float)ret.GetField("m_float"), dPdxType.Float);
                    PdxType.GenericCompare((float[])ret.GetField("m_floatArray"), dPdxType.FloatArray);
                    PdxType.GenericValCompare((Int16)ret.GetField("m_int16"), dPdxType.Int16);
                    PdxType.GenericValCompare((Int32)ret.GetField("m_int32"), dPdxType.Int32);
                    PdxType.GenericValCompare((long)ret.GetField("m_long"), dPdxType.Long);
                    PdxType.GenericCompare((int[])ret.GetField("m_int32Array"), dPdxType.Int32Array);

                    PdxType.GenericCompare((long[])ret.GetField("m_ulongArray"), dPdxType.UlongArray);
                    PdxType.GenericCompare((Int16[])ret.GetField("m_uint16Array"), dPdxType.Uint16Array);

                    byte[] retbA = (byte[])ret.GetField("m_byte252");
                    if (retbA.Length != 252)
                        throw new Exception("Array len 252 not found");

                    retbA = (byte[])ret.GetField("m_byte253");
                    if (retbA.Length != 253)
                        throw new Exception("Array len 253 not found");

                    retbA = (byte[])ret.GetField("m_byte65535");
                    if (retbA.Length != 65535)
                        throw new Exception("Array len 65535 not found");

                    retbA = (byte[])ret.GetField("m_byte65536");
                    if (retbA.Length != 65536)
                        throw new Exception("Array len 65536 not found");

                    pdxEnumTest ev = (pdxEnumTest)ret.GetField("m_pdxEnum");
                    if (ev != dPdxType.PdxEnum)
                        throw new Exception("Pdx enum is not equal");

                    IPdxInstance[] addreaaPdxI = (IPdxInstance[])ret.GetField("m_address");
                    if (addreaaPdxI.Length != dPdxType.AddressArray.Length)
                        throw new Exception("Address array not mateched ");
                    

                    List<object> objArr = (List<object>)ret.GetField("m_objectArray");
                    
                    if (objArr.Count != dPdxType.ObjectArray.Count)
                       throw new Exception("Object array not mateched ");
                    

                }
            }
            catch (Exception ex)
            {
                FwkException("doAccessPdxInstanceAndVerify() Caught Exception: {0}", ex);
            }
        }
        public void exceptionThrow(IPdxInstance newpdxins, IPdxInstance pdxins)
        {
            if (pdxins.Equals(newpdxins))
            {
                throw new Exception("PdxInstance should not be equal");
            }
        }
        public void doModifyPdxInstance()
        {
            FwkInfo("In doModifyPdxInstance");
            IRegion<TKey, TVal> region0 = GetRegion();
            int size;
            if (isEmptyClient || m_istransaction)
            {
                size = region0.Keys.Count;
            }
            else
            {
                size = region0.Count;
            }
            ResetKey(DistinctKeys);
            int numKeys = GetUIntValue(DistinctKeys);
            //int size = region0.Count;
            TKey[] keys = (TKey[])region0.Keys;
            TKey key = default(TKey);
            try
            {
                if(size != numKeys){
                    FwkException("doModifyPdxInstance() number of entries {0} on server is not same as expected {1}",size,numKeys);
                }
                for (int index = 0; index < numKeys; index++)
                {
                    
                    key = keys[index];
                    IPdxInstance newpdxins;
                    IPdxInstance pdxins = (IPdxInstance)region0[key];

                    int oldVal = (int)pdxins.GetField("m_int32");
                    IWritablePdxInstance iwpi = pdxins.CreateWriter();
                    StringBuilder builder = new StringBuilder();
                    Random random = new Random();
                    char ch;
                    for (int j = 0; j < 10; j++)
                    {
                        ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                        builder.Append(ch);
                    }
                    string changeStr = builder.ToString();
                    //string changeStr = "change the string";
                    iwpi.SetField("m_int32", oldVal + 1);
                    iwpi.SetField("m_string", changeStr);
                    region0[key] =(TVal)(object)iwpi;

                    newpdxins = (IPdxInstance)region0[key];

                    int newVal = (int)newpdxins.GetField("m_int32");
                    if ((oldVal + 1) != newVal)
                    {
                        throw new Exception("PdxInstance field m_int32 of PdxType not get modified and " +
                            "the previous value was " + oldVal + " and new value is " + newVal);
                    }

                    string cStr = (string)newpdxins.GetField("m_string");
                    if (!cStr.Equals(changeStr))
                    {
                        throw new Exception("PdxInstance field m_string of PdxType not get modified and " +
                            "the previous value was " + changeStr + " and new value is " + cStr);
                    }

                    List<object> arr = (List<object>)newpdxins.GetField("m_arraylist");
                    exceptionThrow(newpdxins, pdxins);

                    
                    //int num = random.Next(0, 26); // Zero to 25
                    //char newChar = (char)('a' + num);
                    /*
                    char oldChar = (char)pdxins.GetField("m_char");
                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_char", 'D');
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    char newChar = (char)newpdxins.GetField("m_char");
                    if (!(newChar.Equals('D')))
                    {
                        throw new Exception("PdxInstance field m_char of PdxType not get modified and " +
                            "the previous value was " + oldChar + " and new value is " + newChar);
                    }
                    expcetionThrow(newpdxins, pdxins);
                    */
                    bool beforeValue = (bool)pdxins.GetField("m_bool");
                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_bool", false);
                    region0[key] = (TVal)(object)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    bool updateValue = (bool)newpdxins.GetField("m_bool");
                    if(updateValue)
                    {
                        throw new Exception("PdxInstance field m_bool is not equal");
                    }
                    exceptionThrow(newpdxins, pdxins);
                    /*
                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_byte", (sbyte)0x75);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((sbyte)newpdxins.GetField("m_byte"), (sbyte)0x75, "sbyte is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_sbyte", (sbyte)0x57);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((sbyte)newpdxins.GetField("m_sbyte"), (sbyte)0x57, "sbyte is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_int16", (short)0x5678);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((Int16)newpdxins.GetField("m_int16"), (short)0x5678, "int16 is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_long", (long)0x56787878);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((long)newpdxins.GetField("m_long"), (long)0x56787878, "long is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_float", 18389.34f);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((float)newpdxins.GetField("m_float"), 18389.34f, "float is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_float", 18389.34f);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((float)newpdxins.GetField("m_float"), 18389.34f, "float is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_double", 18389.34d);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((double)newpdxins.GetField("m_double"), 18389.34d, "double is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_boolArray", new bool[] { true, false, true, false, true, true, false, true });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((bool[])newpdxins.GetField("m_boolArray"), new bool[] { true, false, true, false, true, true, false, true }, "bool array is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_byteArray", new byte[] { 0x34, 0x64, 0x34, 0x64 });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((byte[])newpdxins.GetField("m_byteArray"), new byte[] { 0x34, 0x64, 0x34, 0x64 }, "byte array is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_charArray", new char[] { 'c', 'v', 'c', 'v' });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((char[])newpdxins.GetField("m_charArray"), new char[] { 'c', 'v', 'c', 'v' }, "char array is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    long ticks = 634460644691580000L;
                    DateTime tdt = new DateTime(ticks);
                    iwpi.SetField("m_dateTime", tdt);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((DateTime)newpdxins.GetField("m_dateTime"), tdt, "datetime is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_int16Array", new short[] { 0x2332, 0x4545, 0x88, 0x898 });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((Int16[])newpdxins.GetField("m_int16Array"), new short[] { 0x2332, 0x4545, 0x88, 0x898 }, "short array is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_int32Array", new int[] { 23, 676868, 34343 });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((Int32[])newpdxins.GetField("m_int32Array"), new int[] { 23, 676868, 34343 }, "int32 array is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_longArray", new Int64[] { 3245435, 3425435 });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((long[])newpdxins.GetField("m_longArray"), new Int64[] { 3245435, 3425435 }, "long array is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_floatArray", new float[] { 232.565f, 234323354.67f });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((float[])newpdxins.GetField("m_floatArray"), new float[] { 232.565f, 234323354.67f }, "float array is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_doubleArray", new double[] { 23423432d, 43242354315d });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((double[])newpdxins.GetField("m_doubleArray"), new double[] { 23423432d, 43242354315d }, "double array is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    byte[][] tmpbb = new byte[][]{new byte[] {0x23},
                    new byte[]{0x34, 0x55},
                    new byte[] {0x23},
                    new byte[]{0x34, 0x55}
                    };
                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_byteByteArray", tmpbb);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    byte[][] retbb = (byte[][])newpdxins.GetField("m_byteByteArray");

                    PdxType.compareByteByteArray(tmpbb, retbb);

                    expcetionThrow(newpdxins, pdxins);


                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_stringArray", new string[] { "one", "two", "eeeee" });
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((string[])newpdxins.GetField("m_stringArray"), new string[] { "one", "two", "eeeee" }, "string array is not equal");
                    expcetionThrow(newpdxins, pdxins);


                    List<object> tl = new List<object>();
                    tl.Add(new PdxType());
                    tl.Add(new byte[] { 0x34, 0x55 });

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_arraylist", tl);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual(((List<object>)newpdxins.GetField("m_arraylist")).Count, tl.Count, "list<object> is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    Dictionary<object, object> map = new Dictionary<object, object>();
                    map.Add(1, new bool[] { true, false, true, false, true, true, false, true });
                    map.Add(2, new string[] { "one", "two", "eeeee" });

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_map", map);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual(((Dictionary<object, object>)newpdxins.GetField("m_map")).Count, map.Count, "map is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    Hashtable hashtable = new Hashtable();
                    hashtable.Add(1, new string[] { "one", "two", "eeeee" });
                    hashtable.Add(2, new int[] { 23, 676868, 34343 });

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_hashtable", hashtable);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual(((Hashtable)newpdxins.GetField("m_hashtable")).Count, hashtable.Count, "hashtable is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_pdxEnum", pdxEnumTest.pdx1);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual(((pdxEnumTest)newpdxins.GetField("m_pdxEnum")), pdxEnumTest.pdx1, "pdx enum is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    ArrayList vector = new ArrayList();
                    vector.Add(1);
                    vector.Add(2);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_vector", vector);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual(((ArrayList)newpdxins.GetField("m_vector")).Count, vector.Count, "vector is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    CacheableHashSet chm = CacheableHashSet.Create();
                    chm.Add(1);
                    chm.Add("jkfdkjdsfl");

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_chs", chm);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((CacheableHashSet)newpdxins.GetField("m_chs"), chm, "CacheableHashSet is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    CacheableLinkedHashSet clhs = CacheableLinkedHashSet.Create();
                    clhs.Add(111);
                    clhs.Add(111343);

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_clhs", clhs);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual((CacheableLinkedHashSet)newpdxins.GetField("m_clhs"), clhs, "CacheableLinkedHashSet is not equal");
                    expcetionThrow(newpdxins, pdxins);

                    PdxTests.Address[] aa = new PdxTests.Address[2];
                    for (int i = 0; i < aa.Length; i++)
                    {
                        aa[i] = new PdxTests.Address(i + 1, "street" + i.ToString(), "city" + i.ToString());
                    }

                    iwpi = pdxins.CreateWriter();

                    iwpi.SetField("m_address", aa);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    IPdxInstance[] iaa = (IPdxInstance[])newpdxins.GetField("m_address");
                    Assert.AreEqual(iaa.Length, aa.Length, "address array length should equal");
                    expcetionThrow(newpdxins, pdxins);

                    List<object> oa = new List<object>();
                    oa.Add(new PdxTests.Address(1, "1", "12"));
                    oa.Add(new PdxTests.Address(1, "1", "12"));

                    iwpi = pdxins.CreateWriter();
                    iwpi.SetField("m_objectArray", oa);
                    region0[key] = (TVal)iwpi;
                    newpdxins = (IPdxInstance)region0[key];
                    Assert.AreEqual(((List<object>)newpdxins.GetField("m_objectArray")).Count, oa.Count, "Object arary is not equal");
                    expcetionThrow(newpdxins, pdxins);
                    */
         
                }
            }
            catch (Exception ex)
            {
                FwkException("doModifyPdxInstance() Caught Exception: {0}", ex);
            }
                
        }

        public void doVerifyAndModifyPdxInstance()
        {
            try
            {
                doAccessPdxInstanceAndVerify();
                doModifyPdxInstance();
            }
            catch (Exception ex)
            {
                FwkException("doModifyPdxInstance() Caught Exception: {0}", ex);
            }
        }


        // ------------------------------ end pdx ralated task ----------------
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

            DateTime now = DateTime.Now;
            DateTime end = now.AddSeconds(secondsToRun);

            TKey key;
            TVal value;
            TVal tmpValue;
            //TVal valBuf = (TVal)(object)Encoding.ASCII.GetBytes(new string('A', valSize));
            byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));
            string opcode = null;

            int creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, query = 0, putAll = 0, getAll = 0;
            IRegion<TKey, TVal> region = GetRegion();
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
                            FwkException("CacheServerTest::doEntryOperations(): No region to perform operation {0}", opcode);
                        }
                    }

                    key = (TKey)(object)GetKey(entryCount);
                    if (opcode == "add")
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
                                 "ex-ception in add: {0}", ex.Message);
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
                            try
                            {
                                region.Invalidate(key);
                                invals++;
                            }
                            catch (EntryNotFoundException ex)
                            {
                                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                                  "ex-ception in invalidate: {0}", ex.Message);
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
                                  "ex-ception in destroy: {0}", ex.Message);
                            }
                        }
                        else if (opcode == "read")
                        {
                            try
                            {
                                value = (TVal)region[key];
                                gets++;
                            }
                            catch (EntryNotFoundException ex)
                            {
                                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                                  "ex-ception in read: {0}", ex.Message);
                            }
                            catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex)
                            {
                                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                                  "ex-ception in read: {0}", ex.Message);
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
                                  "ex-ception in localDestroy: {0}", ex.Message);
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

            IRegion<TKey, TVal> region = GetRegion();
            if (region == null)
            {
                FwkException("CacheServer.DoEntryOperationsForSecurity(): " +
                  "No region to perform operations on.");
            }

            FwkInfo("CacheServer.DoEntryOperationsForSecurity() will work for {0}secs " +
              "using {1} byte values.", secondsToRun, valSize);

            int cnt = 0;
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
                        }
                        else if (opCode == "invalidate")
                        {
                            region.Invalidate(key);
                        }
                        else if (opCode == "destroy")
                        {
                            region.Remove(key);
                        }
                        else if (opCode == "get")
                        {
                            value = region[key];
                        }
                        else if (opCode == "read+localdestroy")
                        {
                            value = region[key];
                            region.GetLocalView().Remove(key);
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
                        }
                        else if (opCode == "cq")
                        {
                            string cqName = String.Format("cq-{0}-{1}", Util.ClientId, cnt++);
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

        public static void TestComplete()
        {
            OperationsMap.Clear();
            ExceptionsMap.Clear();
        }

        #endregion
    }
}

