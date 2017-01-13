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

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

    //----------------------------------- DoOpsTask start ------------------------
    public class DoFETask<TKey, TVal> : ClientTask
    {
        private IRegion<TKey, TVal> m_region;
        private const int INVALIDATE = 1;
        private const int LOCAL_INVALIDATE = 2;
        private const int DESTROY = 3;
        private const int LOCAL_DESTROY = 4;
        private const int UPDATE_EXISTING_KEY = 5;
        private const int GET = 6;
        private const int ADD_NEW_KEY = 7;
        private const int PUTALL_NEW_KEY = 8;
        //private const int QUERY = 8;
        private const int NUM_EXTRA_KEYS = 100;
        private static bool m_istransaction = false;
        private static string m_funcName;
        CacheTransactionManager txManager = null;
        private object CLASS_LOCK = new object();
        private object SKIPS_LOCK = new object();
        protected int[] operations = { INVALIDATE, LOCAL_INVALIDATE, DESTROY, LOCAL_DESTROY, UPDATE_EXISTING_KEY, GET, ADD_NEW_KEY, PUTALL_NEW_KEY };
        public DoFETask(IRegion<TKey, TVal> region, bool istransaction)
            : base()
        {
            m_region = region;
            m_istransaction = istransaction;
            m_funcName = FwkTest<TKey, TVal>.CurrentTest.GetStringValue("funcName");

        }
        public override void DoTask(int iters, object data)
        {

            Random random = new Random();
            List<int> availableOps = new List<int>(operations);
            lock (SKIPS_LOCK)
            {
                FwkTest<TKey, TVal>.CurrentTest.ResetKey("isSkipOps");
                bool isSkipOps = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isSkipOps");
                if (isSkipOps)
                {
                    availableOps.Remove(LOCAL_INVALIDATE);
                    availableOps.Remove(LOCAL_DESTROY);
                   
                }
            }
            while (Running && (availableOps.Count != 0))
            {
                int opcode = -1;
                lock (CLASS_LOCK)
                {
                    bool doneWithOps = false;
                    int i = random.Next(0, availableOps.Count);
                    try
                    {
                        opcode = availableOps[i];
                        if (m_istransaction)
                        {
                            txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
                            txManager.Begin();
                        }
                        switch (opcode)
                        {
                            case ADD_NEW_KEY:
                                doneWithOps = addNewKeyFunction();
                                break;
                            /*case QUERY:
                                doneWithOps = queryFunction();
                                break;*/
                            case PUTALL_NEW_KEY:
                                doneWithOps = putAllNewKeyFunction();
                                break;
                            case INVALIDATE:
                                doneWithOps = invalidateFunction();
                                break;
                            case DESTROY:
                                doneWithOps = destroyFunction();
                                break;
                            case UPDATE_EXISTING_KEY:
                                doneWithOps = updateExistingKeyFunction();
                                break;
                            case GET:
                                doneWithOps = getFunction();
                                break;
                            case LOCAL_INVALIDATE:
                                doneWithOps = localInvalidateFunction();
                                break;
                            case LOCAL_DESTROY:
                                doneWithOps = localDestroyFunction();
                                break;
                            default:
                                {
                                    throw new Exception("Invalid operation specified:" + opcode);
                                }
                        }
                        if (m_istransaction && (txManager != null) && (!doneWithOps))
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
                                FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught TransactionDataNodeHasDepartedException in doEntry : {0}", e);
                            }
                            catch (Exception ex)
                            {
                                FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught unexpected in doEntry : {0}", ex);
                            }
                        }
                    }
                    catch (TimeoutException e)
                    {
                        FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught unexpected timeout exception during entry operation: " + opcode + " " + e);
                    }
                    catch (IllegalStateException e)
                    {
                        FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught IllegalStateException during entry operation:" + opcode + " " + e);
                    }
                    catch (Exception e)
                    {
                        FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught exception during entry operation: " + opcode + "  exiting task.\n" + e);
                    }
                    if (doneWithOps)
                    {
                        if (m_istransaction && txManager != null)
                        {
                            try
                            {
                                txManager.Rollback();
                            }
                            catch (IllegalStateException e)
                            {
                                FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught IllegalStateException during rollback: " + e);
                            }
                            catch (Exception e)
                            {
                                FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught exception during entry operation: " + e + " exiting task.\n");
                            }
                        }
                        availableOps.Remove(opcode);
                    }
                }
            }
        } //end doTask function
       
        private void checkContainsValueForKey(TKey key, bool expected, string logStr)
        {
            //RegionPtr regionPtr = getRegion();
            bool containsValue = m_region.ContainsValueForKey(key);
            if (containsValue != expected)
                FwkTest<TKey, TVal>.CurrentTest.FwkException("DoOpsTask::checkContainsValueForKey: Expected containsValueForKey(" + key + ") to be " + expected +
                         ", but it was " + containsValue + ": " + logStr);
        }
        
        bool addNewKeyFunction()
        {
            int numNewKeysCreated = (int)Util.BBGet("ImageBB", "NUM_NEW_KEYS_CREATED");
            Util.BBIncrement("ImageBB", "NUM_NEW_KEYS_CREATED");
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("NumNewKeys");
            int numNewKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("NumNewKeys");
            if (numNewKeysCreated > numNewKeys)
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All new keys created; returning from addNewKey");
                return true;
            }
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("entryCount");
            int entryCount = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("entryCount");
            entryCount = (entryCount < 1) ? 10000 : entryCount;
            TKey key = (TKey)(object)(entryCount + numNewKeysCreated);
            checkContainsValueForKey(key, false, "before addNewKey");

            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();

            FwkTest<TKey, TVal>.CurrentTest.ResetKey("isPdxObject");
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("addKey");
            if (pdxobject)
                args.Add(pdxobject);    
            args.Add(filterObj[0]); 
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do addKey execute");
            ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "addNewKeyFunction");
            return (numNewKeysCreated >= numNewKeys);
        }
        bool putAllNewKeyFunction()
        {
            int numNewKeysCreated = (int)Util.BBGet("ImageBB", "NUM_NEW_KEYS_CREATED");
            Util.BBIncrement("ImageBB", "NUM_NEW_KEYS_CREATED");
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("NumNewKeys");
            int numNewKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("NumNewKeys");
            if (numNewKeysCreated > numNewKeys)
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All new keys created; returning from addNewKey");
                return true;
            }
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("entryCount");
            int entryCount = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("entryCount");
            entryCount = (entryCount < 1) ? 10000 : entryCount;
            TKey key = (TKey)(object)(entryCount + numNewKeysCreated);
            if(m_region.Attributes.CloningEnabled != false)
              checkContainsValueForKey(key, false, "before addNewKey");
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("isPdxObject");
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("putAll");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]);
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do addKey execute");
            ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "putAllNewKeyFunction");
            return (numNewKeysCreated >= numNewKeys);
        }
        bool invalidateFunction()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_INVALIDATE");
            Util.BBIncrement("ImageBB", "LASTKEY_INVALIDATE");
            int firstKey = (int)Util.BBGet("ImageBB", "First_Invalidate");
            int lastKey = (int)Util.BBGet("ImageBB", "Last_Invalidate");
            if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All existing keys invalidated; returning from invalidate");
                return true;
            }
            TKey key = (TKey)(object)(nextKey);
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("invalidate");
            if(pdxobject)
                args.Add(pdxobject);      
            args.Add(filterObj[0]); 
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do invalidate execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "invalidateFunction");
            return (nextKey >= lastKey);
        }
        bool localInvalidateFunction()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_LOCAL_INVALIDATE");
            Util.BBIncrement("ImageBB", "LASTKEY_LOCAL_INVALIDATE");
            int firstKey = (int)Util.BBGet("ImageBB", "First_LocalInvalidate");
            int lastKey = (int)Util.BBGet("ImageBB", "Last_LocalInvalidate");
            if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All local invalidates completed; returning from localInvalidate");
                return true;
            }
            TKey key = (TKey)(object)(nextKey);
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("localinvalidate");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]); 
            
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do locally invalidate execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "localInvalidateFunction");
            return (nextKey >= lastKey);
        }
        bool destroyFunction()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_DESTROY");
            Util.BBIncrement("ImageBB", "LASTKEY_DESTROY");
            int firstKey = (int)Util.BBGet("ImageBB", "First_Destroy");
            int lastKey = (int)Util.BBGet("ImageBB", "Last_Destroy");
            if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All destroys completed; returning from destroy");
                return true;
            }
            TKey key = (TKey)(object)(nextKey);
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("destroy");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]); 
           
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do destroy execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "destroyFunction");
            return (nextKey >= lastKey);
        }
        bool localDestroyFunction()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_LOCAL_DESTROY");
            Util.BBIncrement("ImageBB", "LASTKEY_LOCAL_DESTROY");
            int firstKey = (int)Util.BBGet("ImageBB", "First_LocalDestroy");
            int lastKey = (int)Util.BBGet("ImageBB", "Last_LocalDestroy");
            if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All local destroys completed; returning from localDestroy");
                return true;
            }
            TKey key = (TKey)(object)(nextKey);
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("localdestroy");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]); 
           
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do local destroy execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "localDestroyFunction");
            return (nextKey >= lastKey);
        }
        bool updateExistingKeyFunction()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_UPDATE_EXISTING_KEY");
            Util.BBIncrement("ImageBB", "LASTKEY_UPDATE_EXISTING_KEY");
            int firstKey = (int)Util.BBGet("ImageBB", "First_UpdateExistingKey");
            int lastKey = (int)Util.BBGet("ImageBB", "Last_UpdateExistingKey");
            if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All existing keys updated; returning from updateExistingKey");
                return true;
            }
            TKey key = (TKey)(object)(nextKey);

            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("isPdxObject");
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("update");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]); 
            
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do update execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "updateExistingKeyFunction");
            return (nextKey >= lastKey);
        }
        bool getFunction()
        {
            int nextKey = (int)Util.BBGet("ImageBB", "LASTKEY_GET");
            Util.BBIncrement("ImageBB", "LASTKEY_GET");
            int firstKey = (int)Util.BBGet("ImageBB", "First_Get");
            int lastKey = (int)Util.BBGet("ImageBB", "Last_Get");
            if (!((nextKey >= firstKey) && (nextKey <= lastKey)))
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All gets completed; returning from get");
                return true;
            }
            TKey key = (TKey)(object)(nextKey);
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("isPdxObject");
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("get");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]); 
            
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do get execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "getFunction");
            return (nextKey >= lastKey);
        }
        bool queryFunction()
        {
            int numNewKeysCreated = (int)Util.BBGet("ImageBB", "NUM_NEW_KEYS_CREATED");
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("NumNewKeys");
            int numThread = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("numThreads");
            numNewKeysCreated = numNewKeysCreated - (numThread - 1);
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("NumNewKeys");
            int numNewKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("NumNewKeys");
            if (numNewKeysCreated > numNewKeys)
            {
                FwkTest<TKey, TVal>.CurrentTest.FwkInfo("All query executed; returning from addNewKey");
                return true;
            }
            int entryCount = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("entryCount");
            entryCount = (entryCount < 1) ? 10000 : entryCount;
            TKey key = (TKey)(object)(entryCount + numNewKeysCreated);
            checkContainsValueForKey(key, false, "before addNewKey");
            //TVal value = GetValue((TVal)(object)(entryCount + numNewKeysCreated));
            //GetValue(value);
            //m_region.Add(key, value);
            Object[] filterObj = new Object[1];
            filterObj[0] = (TKey)(object)key;
            ArrayList args = new ArrayList();
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("isPdxObject");
            bool pdxobject = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("isPdxObject");
            args.Add("query");
            if (pdxobject)
                args.Add(pdxobject);
            args.Add(filterObj[0]); 
            
            GemStone.GemFire.Cache.Generic.Execution<object> exc =
                Generic.FunctionService<object>.OnRegion<TKey, TVal>(m_region);
            //FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Going to do query execute");
             ICollection<object> executeFunctionResult = null;
            FwkTest<TKey, TVal>.CurrentTest.ResetKey("replicated");
            bool isReplicate = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("replicated");
            if (!isReplicate){
                executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<object>(filterObj).Execute(m_funcName).GetResult(); 
            }else
            {
                executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(m_funcName).GetResult();      
            }
            verifyFEResult(executeFunctionResult, "queryFunction");
            return (numNewKeysCreated >= numNewKeys);
        }
        private void verifyFEResult(ICollection<object> exefuncResult, string funcName)
        {
            if (exefuncResult != null)
            {
                foreach (object item in exefuncResult)
                {
                    if ((bool)item != true)
                    {
                        FwkTest<TKey, TVal>.CurrentTest.FwkException("DoFETask::" + funcName + "failed, last result is not true");
                    }
                }
            }
        }
           
    }

    //------------------------------------DoOpsTask end --------------------------


  public class FunctionExecution<TKey,TVal> : FwkTest<TKey, TVal>

  {
    private const int NUM_EXTRA_KEYS = 100;
    private static bool m_istransaction = false;
    private static List<TKey> destroyedKeys = new List<TKey>();
    protected IRegion<TKey,TVal> GetRegion()
    {
      return GetRegion(null);
    }

    protected IRegion<TKey,TVal> GetRegion(string regionName)
    {
      IRegion<TKey, TVal> region;
      if (regionName == null)
      {
        region = GetRootRegion();
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
    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        IRegion<TKey, TVal> region = CreateRootRegion();
        ResetKey("useTransactions");
        m_istransaction = GetBoolValue("useTransactions");
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
    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      CacheHelper<TKey, TVal>.Close();
    }
      public virtual void DoClearRegion()
      {
          FwkInfo("In DoClearRegion()");
          try
          {
              IRegion<TKey, TVal> region = GetRegion();
              region.Clear();
          }
          catch (Exception ex)
          {
              FwkException("DoClearRegion() Caught Exception: {0}", ex);
          }
          FwkInfo("DoClearRegion() complete.");
      }
    public void DoLoadRegion()
    {
      FwkInfo("In DoLoadRegion()");
      try
      {
       IRegion<TKey, TVal> region = GetRegion();
        ResetKey("distinctKeys");
        int numKeys = GetUIntValue("distinctKeys");
        bool isUpdate = GetBoolValue("update");
        //string key = null;
        //string value = null;
        TKey key;
        TVal value;
        for (int j = 1; j < numKeys; j++)
        {
          string k = "key-" + j;
          string v = null;
          key = (TKey)(object)(k.ToString());
          if (isUpdate)
          {
            v = "valueUpdate-" + j;
            value = (TVal)(object)(v.ToString());
          }
          else
          {
            v = "valueCreate-" + j;
            value = (TVal)(object)(v.ToString());
          }
         // region.Put(key, value);
          region[key] = value;
        }
      }
      catch (Exception ex)
      {
        FwkException("DoLoadRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoLoadRegion() complete.");
    }
    public void DoAddDestroyNewKeysFunction()
    {
      FwkInfo("In DoAddDestroyNewKeysFunction()");
      IRegion<TKey, TVal> region = GetRegion();
      ResetKey("distinctKeys");
      Int32 numKeys = GetUIntValue("distinctKeys");
      int clientNum = Util.ClientNum;
      //IGFSerializable[] filterObj = new IGFSerializable[numKeys];
      Object[] filterObj = new Object[numKeys];
      try
      {
        for(int j = 0; j < numKeys; j++)
        {
          //filterObj[j] = new CacheableString("KEY--" + clientNum + "--" + j);
          filterObj[j] = "KEY--" + clientNum + "--" + j;
        }
        string opcode = GetStringValue( "entryOps" );
        if(opcode == "destroy")
          ExecuteFunction(filterObj, "destroy");
        else
          ExecuteFunction(filterObj,"addKey");
      }
      catch (Exception ex)
      {
        FwkException("DoAddDestroyNewKeysFunction() Caught Exception: {0}", ex);
      }
      FwkInfo("DoAddDestroyNewKeysFunction() complete.");
    }
    private void ExecuteFunction(Object[] filterObj, string ops)
    {
      FwkInfo("In ExecuteFunction() ops is {0}", ops);
      try
      {
        ResetKey( "getResult" );
        Boolean getresult = GetBoolValue("getResult");
        ResetKey( "replicated" );
        bool isReplicate = GetBoolValue("replicated");
        ResetKey( "distinctKeys" );
        Int32 numKeys = GetUIntValue( "distinctKeys" );
       // CacheableVector args = new CacheableVector();
        ArrayList args = new ArrayList();
        if (filterObj == null)
        {
          int clntId = Util.ClientNum;
          filterObj = new Object[1];
          //filterObj = new IGFSerializable[1];
          Random rnd = new Random();
          //filterObj[0] = new CacheableString("KEY--" + clntId + "--" + rnd.Next(numKeys));
          filterObj[0] = "KEY--" + clntId + "--" + rnd.Next(numKeys);
          args.Add(filterObj[0]);
        }
        else
        {
          for (int i = 0; i < filterObj.Length; i++)
          {
            args.Add(filterObj[i]);
          }
        }
        if (ops.Equals("destroy"))
        {
          for (int i = 0; i < filterObj.Length; i++)
          {
            destroyedKeys.Add((TKey)filterObj[i]);
          }
        }
        //Execution<object> exc = null;
        GemStone.GemFire.Cache.Generic.Execution<object> exc = null;
        //Execution exc = null;
        Pool/*<TKey, TVal>*/ pptr = null;
        IRegion<TKey, TVal> region = GetRegion();
        ResetKey("executionMode");
        string executionMode = GetStringValue( "executionMode" );
        ResetKey("poolName");
        string poolname = GetStringValue( "poolName" );
        string funcName = null;
        if(executionMode == "onServers" || executionMode  == "onServer"){
          pptr = PoolManager/*<TKey, TVal>*/.Find(poolname);
          if(getresult)
            funcName = "ServerOperationsFunction";
          else
            funcName = "ServerOperationsWithOutResultFunction";
        }
        if ( executionMode == "onServers") {
          /*exc = Generic.FunctionService.OnServers<TKey,TVal,object>(pptr);*/
          exc = Generic.FunctionService<object>.OnServers(pptr);
        } if ( executionMode == "onServer"){
          exc = Generic.FunctionService<object>.OnServer(pptr);
        }else if( executionMode == "onRegion"){
          exc = Generic.FunctionService<object>.OnRegion<TKey, TVal>(region);
          if(getresult)
            funcName = "RegionOperationsFunction";
          else
            funcName = "RegionOperationsWithOutResultFunction";
        }
        //FwkInfo("ExecuteFunction - function name is{0} ", funcName);
        //IGFSerializable[] executeFunctionResult = null;
        ICollection<object> executeFunctionResult = null;
        if(!isReplicate){
          if(getresult == true){
            if(executionMode == "onRegion"){
              executeFunctionResult = exc.WithArgs<string>(ops).WithFilter<object>(filterObj).Execute(funcName, 15).GetResult();
            }else{
              args.Add(ops);
              executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(funcName, 15).GetResult();
            }
          }else {
            if(executionMode == "onRegion"){
              exc.WithArgs<string>(ops).WithFilter<object>(filterObj).Execute(funcName, 15);
            } else {
              args.Add(ops);
              exc.WithArgs<ArrayList>(args).Execute(funcName, 15);
            }
          }
        } else {
          args.Add(ops);
          if (getresult)
          {
            executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(funcName, 15).GetResult();
          }
          else
          {
            executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(funcName, 15).GetResult();
          }       
        }
        Thread.Sleep(30000);
        if (ops == "addKey")
        {
          VerifyAddNewResult(executeFunctionResult, filterObj);
        }
        else
        {
          VerifyResult(executeFunctionResult, filterObj, ops);
        }
      }
      catch (Exception ex)
      {
        FwkException("ExecuteFunction() Caught Exception: {0}", ex);
      }
      FwkInfo("ExecuteFunction() complete.");
    }
    public void VerifyAddNewResult(ICollection<object> exefuncResult, Object[] filterObj)
    {
      FwkInfo("In VerifyAddNewResult()");
      try
      {
        Thread.Sleep(30000);
        IRegion<TKey, TVal> region = GetRegion();
        //CacheableString value = null;
        string value = null;
        if (filterObj != null)
        {
          for (int i = 0; i < filterObj.Length; i++)
          {
           // CacheableKey key = filterObj[i] as CacheableKey;
            TKey key = (TKey)filterObj[i];
            for (int cnt = 0; cnt < 100; cnt++)
            {
             // value = region.Get(key) as CacheableString;
              value = region[key].ToString();
            }
            //CacheableString expectedVal = filterObj[i] as CacheableString;
            string expectedVal = (String)filterObj[i];
            if (!(expectedVal.Equals(value)))
            {
              FwkException("VerifyAddNewResult Failed: expected value is {0} and found {1} for key {2}", expectedVal, value, key.ToString());
            }
          }
        }
      }
      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex)
      {
        FwkInfo("VerifyAddNewResult() caught exception: {0}", ex);
      }
      catch (Exception ex)
      {
        FwkException("VerifyAddNewResult() Caught Exception: {0}", ex);
      }
      FwkInfo("VerifyAddNewResult() complete.");
    }
    public void VerifyResult(ICollection<object> exefuncResult, Object[] filterObj, string ops)
    {
      FwkInfo("In VerifyResult()");
      Boolean getresult = GetBoolValue("getResult");
      FwkInfo("In VerifyResult() getresult = {0} ", getresult);
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        string buf = null;
        if(exefuncResult != null)
        {
          IEnumerator<object> enm = exefuncResult.GetEnumerator();
          enm.MoveNext();
          Boolean lastResult = (Boolean)enm.Current; //exefuncResult[0];
          if (lastResult!= true)
          FwkException("FunctionExecution::VerifyResult failed, last result is not true");
        }
        string value = null;
        if(filterObj != null )
        {
            //CacheableKey key = filterObj[0] as CacheableKey;
            TKey key = (TKey)filterObj[0];
            for (int i = 0; i<50;i++){
              try
              {
                value = region[key].ToString();
              }
              catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
              {
                if(ops.Equals("destroy") && destroyedKeys.Remove(key))
                {
                  FwkInfo("FunctionExecution.VerifyResults() key {0} has been destroyed " +
                " in read for index {1} ", key,i);
                  break;
                }
                else
                {
                  if (getresult)
                  {
                    FwkException("FunctionExecution.VerifyResults() Caught KeyNotFoundException " +
                      " in read for key: {0} for index {1}", key, i);
                  }
                  else
                  {
                    FwkInfo("FunctionExecution.VerifyResults() key {0} Caught KeyNotFoundException for getResult false" +
                      " in read for index {1} ", key, i);
                    break;
                  }
                }
              }
            }
            if(ops == "update" || ops == "get"){
              if (value != null)
              {
                if (value.IndexOf("update_") == 0)
                  buf = "update_" + key.ToString();
                else
                  buf = key.ToString();
                  if (!(buf.Equals(value)))
                    FwkException("VerifyResult Failed: expected value is {0} and found {1} for key {2} for operation {3}", buf, value, key.ToString(), ops);
              }
            } else if(ops == "destroy"){
                if(value == null){
                  ExecuteFunction(filterObj,"addKey");
                }else{
                  FwkException("FunctionExecution::VerifyResult failed to destroy key {0}",key.ToString());
                }
            } else if(ops == "invalidate"){
              if(value == null)
              {
                ExecuteFunction(filterObj,"update");
              }else {
                FwkException("FunctionExecution::VerifyResult Failed for invalidate key {0}" ,key.ToString());
              }
            }
        }
      }
      catch (Exception ex)
      {
        FwkException("VerifyResult() Caught Exception: {0}", ex);
      }
      FwkInfo("VerifyResult() complete.");
    }
    public void DoExecuteFunctions()
    {
      FwkInfo("In DoExecuteFunctions()");
      int secondsToRun = GetTimeValue("workTime");
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;
      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);
      string opcode = null;
      bool rolledback = false;
      CacheTransactionManager txManager = null;
      while (now < end)
      {
        try
        {
          if (m_istransaction)
          {
            txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
            txManager.Begin();
          }
          opcode = GetStringValue("entryOps");
          ExecuteFunction(null, opcode);
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
            catch (Exception ex)
            {
              throw new Exception("Caught unexpected in doEntry : {0}", ex);
            }
          }
        }
        catch (Exception ex)
        {
          FwkException("DoExecuteFunctions() Caught Exception: {0}", ex);
        }
        now = DateTime.Now;
      }
      FwkInfo("DoExecuteFunctions() complete.");
    }
    public void DoExecuteExceptionHandling()
    {
      FwkInfo("In DoExecuteExceptionHandling()");
      int secondsToRun = GetTimeValue("workTime");
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;
      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);
      string opcode = null;
      bool rolledback = false;
      CacheTransactionManager txManager = null;
      while (now < end)
      {
        try
        {
          if (m_istransaction)
          {
            txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
            txManager.Begin();
          }
          opcode = GetStringValue("entryOps");
          if(opcode == "ParitionedRegionFunctionExecution")
          {
            DoParitionedRegionFunctionExecution();
          }
          else if (opcode == "ReplicatedRegionFunctionExecution")
          {
            DoReplicatedRegionFunctionExecution();
          }
          else if (opcode == "FireAndForgetFunctionExecution")
          {
            DoFireAndForgetFunctionExecution();
          }
          else if (opcode == "OnServersFunctionExcecution")
          {
            DoOnServersFunctionExcecution();
          }
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
            catch (Exception ex)
            {
              throw new Exception("Caught unexpected in doEntry : {0}", ex);
            }
          }
        }
        catch (Exception ex)
        {
          FwkException( "Caught unexpected {0} during exception handling for {1} operation: {2} : exiting task." ,ex.ToString(), opcode,ex.Message);

        }
        now = DateTime.Now;
      }
      FwkInfo("DoExecuteExceptionHandling() complete.");
    }
    public void DoParitionedRegionFunctionExecution()
    {
      FwkInfo("In DoParitionedRegionFunctionExecution()");
      GemStone.GemFire.Cache.Generic.Execution<object> exc = null;
      IRegion<TKey, TVal> region = GetRegion("partitionedRegion");
      try
      {
        exc = Generic.FunctionService<object>.OnRegion<TKey, TVal>(region);
         MyResultCollector<object> myRC = new MyResultCollector<object>();
         Random rnd = new Random();
         if(rnd.Next(100) % 2 == 0)
         {
           //Execution on partitionedRegion with no filter
           exc = exc.WithCollector(myRC);
         }
         else
         {
           //Execution on partitionedRegion with filter
          // ICacheableKey<TKey>[]keys = region.GetKeys();
           ICollection<TKey> keys = region.GetLocalView().Keys;
                      
           //IGFSerializable[] filterObj = new IGFSerializable[keys.Count];
           Object[] filterObj = new Object[keys.Count];
           for (int i = 0; i < keys.Count; i++)
           {
             //filterObj[i] = (IGFSerializable)keys;
             filterObj[i] = keys;
           }
           exc = exc.WithFilter(filterObj).WithCollector(myRC);
         }
         // execute function
         Generic.IResultCollector<object> rc = exc.Execute("ExceptionHandlingFunction", 30);

      }
      catch (GemFire.Cache.Generic.FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.Generic.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoParitionedRegionFunctionExecution() complete.");
    }
    public void DoReplicatedRegionFunctionExecution()
    {
      FwkInfo("In DoReplicatedRegionFunctionExecution()");
      GemStone.GemFire.Cache.Generic.Execution<object> exc = null;
      IRegion<TKey, TVal> region = GetRegion("replicatedRegion");
      try
      {
        exc = Generic.FunctionService<object>.OnRegion<TKey, TVal>(region);
        MyResultCollector<object> myRC = new MyResultCollector<object>();
        exc = exc.WithCollector(myRC);
        // execute function
        Generic.IResultCollector<object> rc = exc.Execute("ExceptionHandlingFunction", 30);
      }
      catch (GemFire.Cache.Generic.FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.Generic.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoReplicatedRegionFunctionExecution() complete.");
    }
    public void DoFireAndForgetFunctionExecution()
    {
      FwkInfo("In DoFireAndForgetFunctionExecution()");
      string name = null;
      Random rnd = new Random();
      if(rnd.Next(100) % 2 == 0){
        //Execution Fire and forget on partitioned region
        name = "partitionedRegion";
      }
      else
      {
        //Execution Fire and forget on replicated region
        name = "replicatedRegion";
      }
      IRegion<TKey, TVal> region = GetRegion(name);
      try
      {
        MyResultCollector<object> myRC = new MyResultCollector<object>();
        GemStone.GemFire.Cache.Generic.Execution<object> exc = Generic.FunctionService<object>.OnRegion<TKey, TVal>(region).WithCollector(myRC);
        // execute function
        Generic.IResultCollector<object> rc = exc.Execute("FireNForget", 30);
      }
      catch (GemFire.Cache.Generic.FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.Generic.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoFireAndForgetFunctionExecution() complete.");
    }
    public void DoOnServersFunctionExcecution()
    {
      FwkInfo("In DoOnServersFunctionExcecution()");
      ResetKey("poolName");
      string poolname = GetStringValue("poolName");
      GemStone.GemFire.Cache.Generic.Execution<object> exc = null;
      try
      {
        Pool/*<TKey, TVal>*/ pptr = PoolManager/*<TKey, TVal>*/.Find(poolname);
        MyResultCollector<object> myRC = new MyResultCollector<object>();
        exc = Generic.FunctionService<object>.OnServers(pptr).WithCollector(myRC);
        // execute function
        Generic.IResultCollector<object> rc = exc.Execute("ExceptionHandlingFunction", 30);
      }
      catch (GemFire.Cache.Generic.FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.Generic.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoOnServersFunctionExcecution() complete.");
    }

    public void DoExecuteFunctionsHA()
    {
      ResetKey("getResult");
      bool getresult = GetBoolValue("getResult");
      ResetKey("distinctKeys");
      int numKeys = GetUIntValue("distinctKeys");
      IRegion<TKey, TVal> region = GetRegion();
      ICollection<object> executeFunctionResult = null;
      string funcName = "GetFunctionExeHA";
      GemStone.GemFire.Cache.Generic.Execution<object> exc = Generic.FunctionService<object>.OnRegion<TKey, TVal>(region);
      MyResultCollectorHA<object> myRC = new MyResultCollectorHA<object>();
      exc = exc.WithCollector(myRC);
      Generic.IResultCollector<object>  rc = exc.Execute(funcName, 120);
      executeFunctionResult = myRC.GetResult();
      if (executeFunctionResult != null)
      {
        IEnumerator<object> enmtor = executeFunctionResult.GetEnumerator();
        enmtor.MoveNext();
        //Boolean lastResult = (Boolean)enmtor.Current; //exefuncResult[0];
        //Boolean lastResult = (Boolean)executeFunctionResult[0];
       // if (lastResult!= true) 
       //   FwkException("FunctionExecution::DoExecuteFunctionHA failed, last result is not true");
        ICollection<object> resultListColl = myRC.GetResult(60);
        string[] resultList = new string[resultListColl.Count];
        resultList.CopyTo(resultList, 0);
        //FwkInfo("FunctionExecution::DoExecuteFunctionHA GetClearResultCount {0} GetGetResultCount {1} GetAddResultCount {2}", myRC.GetClearResultCount(), myRC.GetGetResultCount(), myRC.GetAddResultCount());
        if (resultList != null)
        {
          if (numKeys == resultList.Length)
          {
            for (int i = 1; i < numKeys; i++)
            {
              int count = 0;
              string key = "key-" + i;
              
              for (int j = 0; j < resultList.Length; j++)
              {
                if ((key.Equals(resultList[j])))
                {
                  count++;
                  if (count > 1)
                  {
                    FwkException("FunctionExecution::DoExecuteFunctionHA: duplicate entry found for key {0} ", key);
                  }
                } 
              }
              
              if(count == 0) {
                  FwkException("FunctionExecution::DoExecuteFunctionHA failed: key is missing in result list {0}", key);
                }
              }
            }
          }
          else
          {
            FwkException("FunctionExecution::DoExecuteFunctionHA failed: result size {0} doesn't match with number of keys {1}", resultList.Length, numKeys);
          }
        }
        else {
          FwkException("FunctionExecution::DoExecuteFunctionHA executeFunctionResult is null");
        }
        
      }
      public void DoGetServerKeys()
      {
          try
          {
              FwkInfo("FunctionExecution:DoGetServerKeys");
              IRegion<TKey, TVal> region = GetRegion();
              TKey[] keys = (TKey[])(object)region.Keys;
              bool pdxobject = GetBoolValue("isPdxObject");
              ArrayList args = new ArrayList();
              args.Add("get");
              if (pdxobject)
                  args.Add(pdxobject);
              //args.Add("pdxobject");
              for (int i = 0; i < keys.Length; i++)
                  args.Add(keys[i]);
              string funcName = GetStringValue("funcName");
              GemStone.GemFire.Cache.Generic.Execution<object> exc =
                  Generic.FunctionService<object>.OnRegion<TKey, TVal>(region);
              //FwkInfo("Going to do get execute");
              ICollection<object> executeFunctionResult = null;
              ResetKey("replicated");
              bool isReplicate = GetBoolValue("replicated");
              if (!isReplicate)
              {
                  executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<TKey>(keys).Execute(funcName).GetResult();
              }
              else
              {
                  executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(funcName).GetResult();
              }
              if (executeFunctionResult != null)
              {
                  foreach (object item in executeFunctionResult)
                  {
                      if ((bool)item != true)
                          FwkException("FunctionExecution:DoGetServerKeys:: failed, last result is not true");
                  }

              }

          }
          catch (CacheServerException ex)
          {
              FwkException("DoGetServerKeys() Caught CacheServerException: {0}", ex);
          }
          catch (Exception ex)
          {
              FwkException("DoGetServerKeys() Caught Exception: {0}", ex);
          }
       
      }
      public void DoUpdateServerKeys()
      {
          try
          {
              FwkInfo("FunctionExecution:DoGetServerKeys");
              IRegion<TKey, TVal> region = GetRegion();
              TKey[] keys = (TKey[])(object)region.Keys;
              bool pdxobject = GetBoolValue("isPdxObject");
              ArrayList args = new ArrayList();
              args.Add("update");
              if (pdxobject)
                  args.Add(pdxobject);
              //args.Add("pdxobject");
              for (int i = 0; i < keys.Length; i++)
                  args.Add(keys[i]);
              string funcName = GetStringValue("funcName");
              GemStone.GemFire.Cache.Generic.Execution<object> exc =
                  Generic.FunctionService<object>.OnRegion<TKey, TVal>(region);
              //FwkInfo("Going to do get execute");
              ICollection<object> executeFunctionResult = null;
              ResetKey("replicated");
              bool isReplicate = GetBoolValue("replicated");
              if (!isReplicate)
              {
                  executeFunctionResult = exc.WithArgs<ArrayList>(args).WithFilter<TKey>(keys).Execute(funcName).GetResult();
              }
              else
              {
                  executeFunctionResult = exc.WithArgs<ArrayList>(args).Execute(funcName).GetResult();
              }
              if (executeFunctionResult != null)
              {
                  foreach (object item in executeFunctionResult)
                  {
                      if ((bool)item != true)
                          FwkException("FunctionExecution:DoGetServerKeys:: failed, last result is not true");
                  }

              }

          }
          catch (CacheServerException ex)
          {
              FwkException("DoGetServerKeys() Caught CacheServerException: {0}", ex);
          }
          catch (Exception ex)
          {
              FwkException("DoGetServerKeys() Caught Exception: {0}", ex);
          }

      }
      public void doOps()
      {
          FwkInfo("FunctionExcution:doOps called.");

          int opsSec = GetUIntValue("opsSecond");
          opsSec = (opsSec < 1) ? 0 : opsSec;

          int secondsToRun = GetTimeValue("workTime");
          secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

          int valSize = GetUIntValue("valueSizes");
          valSize = ((valSize < 0) ? 32 : valSize);
          ResetKey("entryCount");
          int entryCount = GetUIntValue("entryCount");
          ResetKey("NumNewKeys");
          int numNewKeys = GetUIntValue("NumNewKeys");

          //TestClient * clnt = TestClient::getTestClient();
          IRegion<TKey, TVal> regionPtr = GetRegion();
          if (regionPtr == null)
          {
              FwkSevere("FunctionExcution:doOps(): No region to perform operations on.");
              //now = end; // Do not do the loop
          }
          ResetKey("useTransactions");
          bool m_istransaction = GetBoolValue("useTransactions");
          try
          {
              DoFETask<TKey, TVal> dooperation = new DoFETask<TKey, TVal>(regionPtr, m_istransaction);
              ResetKey("numThreads");
              int numThreads = GetUIntValue("numThreads");
              RunTask(dooperation, numThreads, entryCount + numNewKeys + NUM_EXTRA_KEYS, -1, -1, null);
          }
          catch (ClientTimeoutException)
          {
              FwkException("In FunctionExcution:doOps()  Timed run timed out.");
          }
          catch (FwkException e)
          {
              FwkException("Caught Exception exception during FunctionExcution:doOps: " + e);
          }
          catch (Exception e)
          {
              FwkException("Caught unexpected exception during FunctionExcution:doOps: " + e);
          }
         
          


          FwkInfo("Done in FunctionExcution:doOps");
          Thread.Sleep(10000);
      }
    }
  }
