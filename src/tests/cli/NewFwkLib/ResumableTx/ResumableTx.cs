using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.Cache.Generic;

  public class EntryTxTask<TKey, TVal> : ClientTask
  {
    #region Private members

    static protected int BEGIN_TX = 1001;
    static protected int EXECUTE_TX_OPS = 1002;
    static protected int EXECUTE_NONTX_OPS = 1003;
    static protected int COMMIT_TX = 1004;
    static protected int ROLLBACK_TX = 1005;


    private IRegion<TKey, TVal> m_region;
    private static int keyCount = 0;
    private static int kCount = 0;
    private int m_MaxKeys;
    private static Dictionary<TransactionId, object> activeTxns;
    private static Dictionary<IRegion<TKey, TVal>, Dictionary<TKey, TVal>> OpMap;
    private object txlock = new object();
    private List<TransactionId> keyList ;
    private Int32 m_create;
    private Int32 m_update;
    private Int32 m_destroy;
    //private Int32 m_invalidate;
    private Int32 m_cnt;
    //bool m_isDestroy;
    bool m_finish;
    bool m_beginTx;
    bool m_commitTx;
    private object CLASS_LOCK = new object();
    private const string SerialExc = "isSerialExecution";
    private const string ConcurrentExc = "isConcurrentExecution";
    private const string WorkTime = "workTime";
   
    #endregion

    private static String getClientIdString()
    {
      return "Cl_Id" + Util.ClientId + "_thr_" + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString();
    }

   public TxInfo begin(IRegion<TKey,TVal> region)
    {
      CacheTransactionManager txManager = null;
      IRegion<TKey, TVal> reg = region;
      TransactionId id = null;
      TxInfo txInfo = new TxInfo();
      txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
      try
      {
        txManager.Begin();
        try
        {
          kCount++;
          TKey key = (TKey)(object)(kCount);
          TVal value = GetValue();
          reg[key] = value;
          Util.Log("The Key is={0} and value={1}", key.ToString(),value.ToString());
        }
        catch (Exception e)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught {0}", e);
        }
      }
      catch (IllegalStateException e)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught {0}", e);
      }
      try
      {
        id = txManager.Suspend();
        Util.Log("Suspend() complete ");
      }
      catch (IllegalStateException e)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException("Got {0}", e);
      }
      Util.Log("BEGIN_TX returning txId = {0}",id);
      try
      {
       txInfo.setTxId(id);
      }
      catch (Exception e) 
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException("Got this {0}", e);
      }
     
      return txInfo;
    }

    public bool commit(TxInfo txInfo)
    {
      TransactionId id = (TransactionId)(object)txInfo.getTxId();
      bool commited = false;
      CacheTransactionManager txManager = null;
      txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
      if (txManager.TryResume(id, 30000))
      {
        try
        {
          txManager.Commit();
          commited = true;
        }
        catch (CommitConflictException ex)
        {
          //Expected exception with concurrent transactions.
          Util.Log("Got expected exception {0}", ex);
        }
        catch (TransactionDataRebalancedException ex)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("Got {0}", ex);
        }
        catch (TransactionDataNodeHasDepartedException ex)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("Got {0}", ex);
        }
      }
      else
        Util.Log("TxId {0} is not suspended in this member with tryResume time limit, cannot commit.  Expected, continuing test.");
      Util.Log("Commited returning {0}",commited);
      return commited;
     
    }

    public void rollback(TxInfo txInfo)
    {
      TransactionId id = (TransactionId)(object)txInfo.getTxId();
      CacheTransactionManager txManager = null;
      bool isRollBack = false;
      txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
      if (txManager.TryResume(id, 30000))
      {
         txManager.Rollback();
         isRollBack = true;
      }
      else
        Util.Log("TxId {0} is not suspended in this member with tryResume time limit, cannot rollback.  Expected with concurrent execution, continuing test.");
      Util.Log("RollbackTx returning {0}",isRollBack);
   
    }

    public void executeTxOps(TxInfo txInfo)
    {
      //This will do a resume+doOps+suspend
      TransactionId id = (TransactionId)(object)txInfo.getTxId();
      CacheTransactionManager txManager = null;
      txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
      bool executedOps = false;
      int numOfOpsToDo = 5;
      if (txManager.TryResume(id, 30000))
      {
        try
        {
          doEntryOps(numOfOpsToDo);
          executedOps = true;
        }
        catch (TransactionDataNodeHasDepartedException ex)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught {0}", ex);
        }
        catch (TransactionDataRebalancedException ex)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught {0}", ex);
        }
        catch (Exception ex)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught unexpected exception during doEntryOps task {0}", ex.Message);
        }
        finally
        {
          id = txManager.Suspend();
          Util.Log("Suspend() complete txId is {0}", id);
        }
      }
      Util.Log("EXECUTE_TX returned  {0}", executedOps);
    }

    public void doEntryOps(int numOfOpsToDo)
    {
      int numOpsCompleted = 0;
      string opcode = null;
      List<string> opList = new List<string>();
      IRegion<TKey, TVal> region = m_region;
      int size = 0;
      int create = 0, update = 0, destroy = 0, invalidate = 0, localdestroy = 0, /*localinvalidate = 0,*/
        get = 0/*, putall = 0*/;
      try
        {
          size = region.Count;
          opcode = FwkTest<TKey, TVal>.CurrentTest.GetStringValue("entryOps");
          Util.Log("Op code is {0} and numOf opsCompleted is {1} and Regionsize is {2}", opcode, numOpsCompleted, size);
          if (opcode == null) opcode = "no-opcode";
          if (((size < 1) && (opcode != "create")) || (opcode == "create"))
          {
            opcode = "create";
            addEntry(region);
            Interlocked.Increment(ref m_create);
            create++;
          }
          else if (opcode == "update")
          {
            updateEntry(region);
            Interlocked.Increment(ref m_update);
            update++;
          }
          else if (opcode == "destroy")
          {
            destroyEntry(region, false);
            Interlocked.Increment(ref m_destroy);
            destroy++;
          }
          else if (opcode == "localDestroy")
          {
            destroyEntry(region, true);
            //Interlocked.Increment();
            localdestroy++;
          }
          else if (opcode == "get")
          {
            getKey(region);
            get++;
          }
          else if (opcode == "invalidate")
          {
            invalidateEntry(region,false);
            invalidate++;
          }
          else
          {
            FwkTest<TKey, TVal>.CurrentTest.FwkException("CacheServer.doEntryOps() Invalid operation " +
             "specified: {0}", opcode);
          }
          numOpsCompleted++;
          opList.Add(opcode);
          
        }
        catch (Exception ex)
        {
          FwkTest<TKey, TVal>.CurrentTest.FwkException("CacheServer.doEntryOps() Caught unexpected " +
            "exception during entry '{0}' operation: {1}.", opcode, ex);
        }
        Util.Log("DoEntryOP: create = {0}, update = {1}, destroy = {2},get = {3} invalidate={4}, num of Ops ={5}",
        m_create, m_update, m_destroy, get, invalidate, numOpsCompleted);
    }
    
    protected TKey addEntry(IRegion<TKey, TVal> m_region)
    {
      TKey key = GetNewKey();
      TVal value = GetValue();
      //int beforeSize = 0;
      bool isSerialExecution = false;
      Dictionary<TKey, TVal> addMap = new Dictionary<TKey, TVal>();
      try
      {
        m_region.Add(key, value);
        addMap[key] = value;
        OpMap[m_region] = addMap;
        doEdgeClientValidation();
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
      return key;
    }

    protected void updateEntry(IRegion<TKey, TVal> r)
    {
      TKey key = GetExistingKey(true,r);
      TVal value = GetValue();
      Dictionary<TKey, TVal> updateMap = new Dictionary<TKey, TVal>();
      r[key] = value;
      updateMap[key] = value;
      OpMap[m_region] = updateMap;
      doEdgeClientValidation();
    }

    protected void invalidateEntry(IRegion<TKey, TVal> r, bool isLocalInvalidate)
    {
      TKey key = GetExistingKey(true,r);
      bool containsKey = m_region.GetLocalView().ContainsKey(key);
      bool containsValueForKey = m_region.GetLocalView().ContainsValueForKey(key);
      Util.Log("containsKey for " + key + ": " + containsKey);
      Util.Log("containsValueForKey for " + key + ": " + containsValueForKey);
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
      }
      catch (EntryNotFoundException e)
      {
         Util.Log("Caught {0} (expected); continuing with test", e);
          return;
        
      }
    }

    protected void destroyEntry(IRegion<TKey, TVal> m_region, bool isLocalDestroy)
    {
      TKey key = GetExistingKey(true, m_region);
      bool isSerialExecution = false;
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

    protected void getKey(IRegion<TKey, TVal> aRegion)
    {
      TKey key = GetExistingKey(true,aRegion);
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
    }

    protected TKey GetExistingKey(bool useServerKeys,IRegion<TKey,TVal> region)
    {
      TKey key = default(TKey);
        if (useServerKeys)
        {
          int size = region.Count;
          TKey[] keys = (TKey[])region.Keys;
          key = keys[Util.Rand(0, size)];
        }
        else
        {
          int size = region.GetLocalView().Count;
          TKey[] keys = (TKey[])region.GetLocalView().Keys;
          key = keys[Util.Rand(0, size)];
        }
        return key;
    }

    protected TKey GetNewKey()
    {
      keyCount++;
      FwkTest<TKey, TVal>.CurrentTest.ResetKey("distinctKeys");
      int numKeys = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("distinctKeys");
     // String keybuf = String.Format("Key-{0}-{1}-{2}", Util.PID, Util.ThreadID, keyCount);
      TKey key = (TKey)(object)(keyCount);
    //  keyCount++;
      return key;
    }

    protected TVal GetValue()
    {
      TVal tmpValue = default(TVal);
      FwkTest<TKey,TVal>.CurrentTest.ResetKey("valueSizes");
      int size = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("valueSizes");
      StringBuilder builder = new StringBuilder();
      Random random = new Random();
      char ch;
      for (int j = 0; j < size; j++)
      {
        ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
        builder.Append(ch);
      }
      if (typeof(TVal) == typeof(string))
      {
        tmpValue = (TVal)(object)builder.ToString();
      }
      else if (typeof(TVal) == typeof(byte[]))
      {
        tmpValue = (TVal)(object)(Encoding.ASCII.GetBytes(builder.ToString()));
      }
      return tmpValue;
    }

    public void doTransactions(IRegion<TKey,TVal> region)//,Dictionary<TransactionId, object> m_maps)
    {
      Util.Log("Inside ResumableTx:doTransactions()");
      TransactionId txId = null;
      //String txIdStr = "";
      TxInfo txInfo = null;
      IRegion<TKey, TVal> reg = region;
      int action = 0;
      FwkTest<TKey, TVal>.CurrentTest.ResetKey("minExecutions");
      int minExecutions = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("minExecutions");
      int numExecutions = 0;
      Util.Log("activeTx map count is {0}",activeTxns.Count);
      if (activeTxns.Count > 0)
      {
        try
        {
          keyList = new List<TransactionId>(activeTxns.Keys);
        }
        catch (Exception e) { FwkTest<TKey, TVal>.CurrentTest.FwkException("Inside keylist exception  {0}", e); }
      }
      FwkTest<TKey, TVal>.CurrentTest.ResetKey("numThreads");
      string NUMTHREADS = "numThreads";
      FwkTest<TKey, TVal>.CurrentTest.ResetKey(NUMTHREADS);
      int numActiveThd = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue(NUMTHREADS);
      int numActiveTx = activeTxns.Count;
      try
      {
        Util.Log("numAvtiveTx is {0} and no of AvtiveThread = {1} and activemap count= {2}", numActiveTx, numActiveThd,activeTxns.Count);
      }
      catch (Exception e) { FwkTest<TKey, TVal>.CurrentTest.FwkException("Caught exception {0}",e); }
      lock (CLASS_LOCK)
      {
      if (numActiveTx < numActiveThd)
        {
          action = BEGIN_TX;
        }
        else
        {
          for (int i = 0; i < keyList.Count; i++)
          {
            txId = (TransactionId)(object)keyList[i];
            txInfo = (TxInfo)(object)activeTxns[txId];
           
            numExecutions = txInfo.getNumExecutions();
            if (numExecutions > minExecutions)
            {
              bool isCommit = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue("TxBool");
              if (isCommit)
              {
                action = COMMIT_TX;
              }
              else
              {
                action = ROLLBACK_TX;
              }
              break;
            }
          }
          if (action == 0)
          {
            action = EXECUTE_TX_OPS;
            Random random = new Random();
            int rant = random.Next(0, keyList.Count - 1);
            txId = (TransactionId)(object)keyList[rant];
            txInfo = (TxInfo)(object)activeTxns[txId];
          }
        }
      }
       Boolean success = false;
       try
       {
         lock (txlock)
         {
           switch (action)
           {
             case (1001):
               txInfo = begin(reg);
               txId = txInfo.getTxId();
               activeTxns[txId] = (Object)txInfo;
               break;
             case (1002):
               executeTxOps(txInfo);
               txId = txInfo.getTxId();
               txInfo = (TxInfo)(object)activeTxns[txId];
               if(txInfo != null)
               {  // could happen if tx committed, entry removed
                 txInfo.incrementNumExecutions();
                 activeTxns[txId] = (Object)txInfo;
               }
               break;
             case (1004):
               success = commit(txInfo);
               break;
             case (1005):
               rollback(txInfo);
               txId = txInfo.getTxId();
               activeTxns.Remove(txId);
               break;
           }
         }
       }
       catch (Exception e)
       {
         FwkTest<TKey, TVal>.CurrentTest.FwkException("The Test Threw this exception {0}", e);
       }
    }

    public void doSerialExecution(IRegion<TKey, TVal> region)
    {
       Util.Log("Inside ResumableTX doSerialExecution()");
       keyList = new List<TransactionId>(activeTxns.Keys);
       TxInfo txInfo;
       TransactionId txId = null;
       for (int i = 0; i < keyList.Count; i++)
       {
         txInfo = (TxInfo)activeTxns[keyList[i]];
         txId = (TransactionId)(object)txInfo.getTxId();
         //bool executedOps;
         try
         {
           executeTxOps(txInfo);
         }
         catch (Exception ex)
         {
           Util.Log("Caught {0} while executing tx ops", ex);
           //executedOps = false;
         }
         txInfo.incrementNumExecutions();
         activeTxns[txId] = (Object)txInfo;
       }
     }

    public void doEdgeClientValidation()
    {
      //Validate the number of expected key/values and no. of destroyed entries.
      Util.Log("Verifying expected keys/values");
      Util.Log("Region count is {0} and local region count is {1}",m_region.Count,m_region.GetLocalView().Count);
      TKey key;
      TVal val;
      bool success = true;
      lock (OpMap)
      {
        foreach (KeyValuePair<IRegion<TKey, TVal>, Dictionary<TKey, TVal>> kvp in OpMap)
        {
          IRegion<TKey, TVal> myregion = (IRegion<TKey, TVal>)kvp.Key;
          Dictionary<TKey, TVal> mapp = (Dictionary<TKey, TVal>)kvp.Value;
          foreach (KeyValuePair<TKey, TVal> sp in mapp)
          {
            key = sp.Key;
            val = sp.Value;
            if (!m_region.ContainsKey(key))
            {
              FwkTest<TKey, TVal>.CurrentTest.FwkException("Expected containsKey() to be true for key {0} ,but it was false", key);
              success = false;
            }
            if (val != null && !m_region.ContainsValueForKey(key))
            {
              FwkTest<TKey, TVal>.CurrentTest.FwkException("Expected containsValueForKey() to be true for key {0} ,but it was false", key);
              success = false;
            }
          }
        }
      }
      if (!success)
        FwkTest<TKey, TVal>.CurrentTest.FwkException("edge client validation failed");
      else
        Util.Log("Done executing doEdgeClientValidation - validation successful");
        
    }

    public void finishAllActiveTx(Dictionary<TransactionId,object> activeTX,Boolean isCommit)
    {
      lock (CLASS_LOCK)
      {
        Util.Log("In Finish active map count is {0}",activeTX.Count);
        keyList = new List<TransactionId>(activeTX.Keys);
        bool txCompleted = false;
        for (int i = 0; i < keyList.Count; i++)
        {
          TransactionId txId = (TransactionId)(object)keyList[i];
          TxInfo txInfo = (TxInfo)(object)activeTX[txId];
          try
          {
            txCompleted = commit(txInfo);
            Util.Log("Commit got {0}",txCompleted);
          }
          catch (Exception e)
          {
            FwkTest<TKey, TVal>.CurrentTest.FwkException("Got this exception {0}", e);
          }
          finally 
          { 
            activeTX.Remove(txId);
          }
          Util.Log("After commit/update the Active Map is {0}", activeTX.Count);
        }
      }
      
    }

    public EntryTxTask(IRegion<TKey, TVal> region, int keyCnt, Dictionary<TransactionId, object> maps,Dictionary<IRegion<TKey,TVal>,Dictionary<TKey,TVal>>omaps, Boolean finish,Boolean isBeginTx,Boolean isCommitTx)
      : base()
    {
      m_region = region;
      m_MaxKeys = keyCnt;
      activeTxns = maps;
      OpMap = omaps;
      m_create = 0;
      m_update = 0;
      m_destroy = 0;
      //m_invalidate = 0;
      m_cnt = 0;
      //m_isDestroy = true;
      m_finish = finish;
      m_beginTx = isBeginTx;
      m_commitTx = isCommitTx;
    }
    public override void DoTask(int iters, object data)
    {
      Int32 localcnt = m_cnt;
      Interlocked.Increment(ref m_cnt);
      int offset = Util.Rand(m_MaxKeys);
      int count = offset;
      //TKey key = default(TKey);
      //TVal value = default(TVal);
      FwkTest<TKey, TVal>.CurrentTest.ResetKey(SerialExc);
      bool isSerialExecution = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue(SerialExc);
   
      FwkTest<TKey, TVal>.CurrentTest.ResetKey(ConcurrentExc);
      bool isConcExecution = FwkTest<TKey, TVal>.CurrentTest.GetBoolValue(ConcurrentExc);
      int Thrd = FwkTest<TKey, TVal>.CurrentTest.GetUIntValue("numThreads");
      Util.Log("EntryTask::DoTask: starting {0} iterations. and isSerialExecution is {1}", iters,isSerialExecution);
      while (Running && (iters-- != 0))
      {
        if (isConcExecution && !m_finish)
        {
          doTransactions(m_region);
        }
        if (isSerialExecution && !m_finish)
        {
          lock (CLASS_LOCK)
          {            
           doSerialExecution(m_region);
          }
        }
        if (m_finish && !m_commitTx)
        {          
          finishAllActiveTx(activeTxns,false);
        }
        if (m_beginTx)
        {
          lock (txlock)
          {
            TxInfo txInfo;
            TransactionId txId = null;
            txInfo = begin(m_region);
            txId = (TransactionId)(object)txInfo.getTxId();
            activeTxns[txId] = (Object)txInfo;
          }
        }

        if (m_commitTx && m_finish)
        {
          finishAllActiveTx(activeTxns, m_commitTx);
        }
      }
      Interlocked.Add(ref m_iters, count - offset);
    }
  }

  public class ResumableTx<TKey,TVal> : FwkTest<TKey,TVal>
  {
    // Tx actions (for concurrent resumable transactions with function execution)
    static protected int BEGIN_TX          = 1001;
    static protected int EXECUTE_TX_OPS    = 1002;
    static protected int EXECUTE_NONTX_OPS = 1003;
    static protected int COMMIT            = 1004;
    static protected int ROLLBACK          = 1005;
    #region Private constants and statics

    static protected int SUSPEND = 2;
    static protected int RESUME = 3;
 
    private static Dictionary<string, Dictionary<TKey, TVal>> BeforeTxMap = new Dictionary<string, Dictionary<TKey, TVal>>();
    private static Dictionary<string, Dictionary<TKey, TVal>> AfterTxMap = new Dictionary<string, Dictionary<TKey, TVal>>();
    protected static Dictionary<TransactionId, object> activeTxns = null;
    protected static Dictionary<IRegion<TKey,TVal>,Dictionary<TKey, TVal>> OpMap = null;
    //private List<TransactionId> keyList;
    //private static int globalTxId = 0;
    //private static int ExecutionNo = 0;
    //private static string RRName = null;
       
    private object CLASS_LOCK = new object();
    
    #endregion

    private const string RegionName = "regionName";
    private const string ValueSizes = "valueSizes";
    private const string OpsSecond = "opsSecond";
    private const string EntryCount = "entryCount";
    private const string WorkTime = "workTime";
    private const string EntryOps = "entryOps";
    private const string LargeSetQuery = "largeSetQuery";
    private const string UnsupportedPRQuery = "unsupportedPRQuery";
    private const string ObjectType = "objectType";
    protected const string NumThreads = "numThreads";
    protected const string TimedInterval = "timedInterval";
    protected const string DistinctKeys = "distinctKeys";
    protected const string BEGINTX = "isBeginTX";
    protected const string COMMITTX = "isCommitTX";

    public class SilenceListener<TKey1, TVal1> : CacheListenerAdapter<TKey1,TVal1>
    {
      public override void AfterCreate(EntryEvent<TKey1, TVal1> ev)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("SilenceListener: AfterCreate key");
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("SilenceListener: AfterCreate key = {0} value = {1}", ev.Key, ev.NewValue);
      }
      public override void AfterUpdate(EntryEvent<TKey1, TVal1> ev)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("SilenceListener: AfterUpdate");
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("SilenceListener: AfterUpdate key = {0} value = {1}", ev.Key, ev.NewValue);
      }
      public override void AfterDestroy(EntryEvent<TKey1, TVal1> ev)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("SilenceListener: AfterDestroy");
      }
      public override void AfterInvalidate(EntryEvent<TKey1, TVal1> ev)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("SilenceListener: AfterInvalidate");
      }
    }

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
    #endregion

    #region Public Methods

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
      FwkInfo("ResumableTx:DoCreateRegion()");
      try
      {
        IRegion<TKey, TVal> region = CreateRootRegion();
        ResetKey("useTransactions");
        if (region == null)
        {
          FwkException("ResumableTx:DoCreateRegion()  could not create region.");
        }
        FwkInfo("ResumableTx:DoCreateRegion()  Created region '{0}'", region.Name);
      }
      catch (Exception ex)
      {
        FwkException("ResumableTx:DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("ResumableTx:DoCreateRegion() complete.");
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

    public void DoSerialTxWithOps()
    {
      FwkInfo("Inside DoSerialTxWithOps ActiveMap count is {0}", activeTxns.Count);
      IRegion<TKey, TVal> region = GetRegion();
      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;
      OpMap = new Dictionary<IRegion<TKey, TVal>, Dictionary<TKey, TVal>>();

      int timedInterval = GetTimeValue("timedInterval") * 1000;
      int maxTime = 10 * timedInterval;
      // Loop over key set sizes
      ResetKey("distinctKeys");
      int numKeys = GetUIntValue("distinctKeys");
      
      ResetKey(NumThreads);
      int numThreads;
      while ((numThreads = GetUIntValue(NumThreads)) > 0)
      {
        EntryTxTask<TKey, TVal> entrytask = new EntryTxTask<TKey, TVal>(region, numKeys / numThreads, activeTxns, OpMap,false,false,false);
        FwkInfo("Running timed task ");
        try
        {
          RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
        }
        catch (ClientTimeoutException)
        {
          FwkException("In DoSerialTxWithOps()  Timed run timed out.");
        }
        FwkInfo("Completed timed task ");
        Thread.Sleep(3000);
        //entrytask.dumpToBB();
      }
    }

    public void DoConcTxWithOps()
    {
      FwkInfo("Inside ResumableTx:DoConcTxWithOps()");
      IRegion<TKey, TVal> region = GetRegion();
      activeTxns = new Dictionary<TransactionId, object>();
      OpMap = new Dictionary<IRegion<TKey, TVal>, Dictionary<TKey, TVal>>();
      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 10 : secondsToRun;

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      int timedInterval = GetTimeValue(TimedInterval) * 1000;
      int maxTime = 10 * timedInterval; 
      // Loop over key set sizes
      ResetKey(DistinctKeys);
      int numKeys = GetUIntValue(DistinctKeys);

      ResetKey(NumThreads);
      int numThreads;
      while ((numThreads = GetUIntValue(NumThreads)) > 0)
      {
        EntryTxTask<TKey, TVal> entrytask = new EntryTxTask<TKey, TVal>(region, numKeys / numThreads, activeTxns,OpMap,false,false,false);
        FwkInfo("Running timed task ");
        try
        {
          RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
        }
        catch (ClientTimeoutException)
        {
          FwkException("In DoConcTxWithOps()  Timed run timed out.");
        }
        Thread.Sleep(3000);
        //entrytask.dumpToBB();
      }
      FwkInfo("Completed timed task ");
    }

    public void DoBeginTx()
    {
      IRegion<TKey, TVal> region = GetRegion();
      activeTxns = new Dictionary<TransactionId, object>();
      int timedInterval = GetTimeValue(TimedInterval) * 1000;
      int maxTime = 10 * timedInterval;
      ResetKey(DistinctKeys);

      ResetKey(BEGINTX);
      bool isBeginTx = GetBoolValue(BEGINTX);
     
      int numKeys = GetUIntValue(DistinctKeys);
       ResetKey(NumThreads);
      int numThreads;
      while ((numThreads = GetUIntValue(NumThreads)) > 0)
      {
        EntryTxTask<TKey, TVal> entrytask = new EntryTxTask<TKey, TVal>(region, numKeys / numThreads, activeTxns,OpMap, false, isBeginTx, false);
        FwkInfo("Running timed task ");
        try
        {
          RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
        }
        catch (ClientTimeoutException)
        {
          FwkException("In DoBeginTx()  Timed run timed out.");
        }
        Thread.Sleep(3000);
        //entrytask.dumpToBB();
      }

    }

    public void DoFinishAllActiveTx()
    {
      FwkInfo("Inside DoFinishActiveTx ActiveMap count is {0}",activeTxns.Count);
      Dictionary<TKey, TVal> tempmap = new Dictionary<TKey, TVal>();
      IRegion<TKey, TVal> region = GetRegion();
      int timedInterval = GetTimeValue(TimedInterval) * 1000;
      int maxTime = 10 * timedInterval;
      // Loop over key set sizes
      ResetKey(DistinctKeys);
      int numKeys = GetUIntValue(DistinctKeys);

      ResetKey(COMMITTX);
      bool isCommitTx = GetBoolValue(COMMITTX);
      //bool Finish=true;
      ResetKey(NumThreads);
      int numThreads;
      while ((numThreads = GetUIntValue(NumThreads)) > 0)
      {
        EntryTxTask<TKey, TVal> entrytask = new EntryTxTask<TKey, TVal>(region, numKeys / numThreads, activeTxns,OpMap, true,false,isCommitTx);
        FwkInfo("Running timed task ");
        try
        {
          RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
        }
        catch (ClientTimeoutException)
        {
          FwkException("In DoFinishAllActiveTx()  Timed run timed out.");
        }
        Thread.Sleep(3000);
        //entrytask.dumpToBB();
      }

    }

    public void DoPopulateRegion() 
    {
      CacheTransactionManager txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager; 
      //TransactionId txId = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager; 
      IRegion<TKey, TVal> reg = GetRegion();
      for (int i = 0; i < 5; i++)
      {
        TKey key = (TKey)(object)(i);
        TVal value = (TVal)(object)"Value_";
        txManager.Begin();
        reg[key] = value;
        txManager.Commit();
        Util.Log("The Key is={0} and value={1}", key.ToString(), value.ToString());
      }
    }

    public void DoGet()
    {
      IRegion<TKey, TVal> reg = GetRegion();
      for (int i = 0; i < 5; i++)
      {
        TKey key = (TKey)(object)(i);
        try
        {
          TVal val = reg[key];
          Util.Log("The Key is={0} and value={1}", key.ToString(), val.ToString());
        }
        catch (Exception e) { FwkException("SP:Caught {0}",e); }
      }
    }

    public void DoBasicTX()
    {
      FwkInfo("Inside ResumableTx:DoBasicTX()");
      IRegion<TKey, TVal> region = GetRegion();
      string opcode = null;
      int action = 0;
      int minNoEx = GetUIntValue("minExecutions");

      if (region == null)
      {
        FwkSevere("ResumableTx:DoBasicTX(): No region to perform operations on.");
      }
      CacheTransactionManager txManager = null;
      TransactionId txId = null;

      int numOfEx = 0;
      try
      {
        txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
        if (numOfEx == 0)
        {
          FwkInfo("Begin.() tx numOfEx={0} and minNoEx={1}", numOfEx, minNoEx);
          txManager.Begin();
          action = BEGIN_TX;
          doValidateTxOps(txId, action);
        }
        while (!(numOfEx > minNoEx))
        {
          try
          {
            FwkInfo("numOfEx={0} and minNoEx={1}", numOfEx, minNoEx);
            txId = txManager.Suspend();
            action = SUSPEND;
            doValidateTxOps(txId, action);

            txManager.Resume(txId);
            int numKeys = 100;
            Dictionary<TKey, TVal> keyValMap = new Dictionary<TKey, TVal>();
            TKey key;
            TVal value;
            for (int i = 0; i < numKeys; i++)
            {
              string keyName = String.Format("key_{0}", i);
              key = (TKey)(object)keyName.ToString();
              value = (TVal)(object)"Value_";
              region[key] = value;
              keyValMap[key] = value;
              AfterTxMap["put"] = keyValMap;
            }
            action = RESUME;
            FwkInfo("keyValMap count={0} and region count is {1}",keyValMap.Count,region.Keys.Count);
            doValidateTxOps(txId, action);
          }
          catch (IllegalStateException ex)
          {
            FwkException("Got {0}", ex.Message);
          }
          catch (CommitConflictException ex)
          {
            FwkException("Got {0}", ex.Message);
          }
          numOfEx++;
        }
        if (numOfEx > minNoEx)
        {
          try
          {
            txManager.Commit();
          }
          catch (CommitConflictException)
          {
            FwkInfo("Got Expected exception as there was a write conflict");
          }
          action = COMMIT;
          doValidateTxOps(txId, action);
        }
      }
      catch (Exception ex)
      {
        FwkException("ResumableTx.DoEntryOpsWithTX() Caught unexpected " +
         "exception during entry '{0}' operation: {1}.", opcode, ex);
      }
    }

    public void doValidateTxOps(TransactionId txId, int txState)
    {
      FwkInfo("Inside Resumabletx:DoValidateTxOps");
      FwkInfo("Transactional Id is {0} and transaction state is={1}",txId,txState);
      //int cnt =0;
      //TKey key;
      int txnState = txState;
      TransactionId txnId = txId;
      ResetKey("entryCount");
      int EntryCount=GetUIntValue("entryCount");
      CacheTransactionManager txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
      try 
      {
        switch (txnState)
        {
          case 1001:
            FwkInfo("...txnState is Begin ...");
            checkContainsKey(txnState);
            break;
          case 2:
            FwkInfo("...txnState is Suspend...");
            if (!txManager.IsSuspended(txId) && !txManager.Exists(txId))
            {
              FwkException("After Suspend(),the Transaction should have been Suspended and should still exist");
            }
            checkContainsKey(txnState);
            break;
          case 3:
            FwkInfo("...txnState is Resume...");
            if (txManager.IsSuspended(txnId))
            {
              FwkException("After Resume(),the Transaction should NOT have been Suspended");
            }
            if (!txManager.Exists(txnId))
            {
              FwkException("After Resume(),the Transaction should still exist");
            }
            checkContainsKey(txnState);
            break;
          case 1004:
            FwkInfo("...txnState is Commit...");
            if (txManager.IsSuspended(txnId))
            {
              FwkException("After Commit(),the Transaction should NOT have been Suspended");
            }
            if (txManager.Exists(txnId))
            {
              FwkException("After Commit(),the Transaction should Not exist");
            }
            if (txManager.TryResume(txnId))
            {
              FwkException("After Commit(),the Transaction should Not be resumed");
            }
            checkContainsKey(txnState);
            FwkInfo("Commit BeforeTxMap.Count={0} ", BeforeTxMap.Count);
            break;
        }

      }
      catch(Exception ex) 
      {
        FwkException("doValidateTxOps caught exception {0}",ex.Message);
      }
    }

    public void checkContainsKey(int txnState)
    {
      IRegion<TKey, TVal> region = GetRegion();
      Dictionary<TKey, TVal> BfTxOpMap = new Dictionary<TKey, TVal>();
      Dictionary<TKey, TVal> AfTxOpMap = new Dictionary<TKey, TVal>();
      try
      {
        if (txnState == 3)
        {
          BfTxOpMap = BeforeTxMap["put"];
          AfTxOpMap = AfterTxMap["put"];
        }
        else
          BfTxOpMap = BeforeTxMap["put"];
        
      }
      catch (Exception e) 
      {
        FwkException("Throwing this exception {0} BMap cpunt={1} AfMap Count={2}", e.Message, BfTxOpMap.Count, AfTxOpMap.Count); 
      }
      FwkInfo("BfTxOpmap count is {0} and AfterTxMap is {1}", BfTxOpMap.Count, AfTxOpMap.Count);
      foreach (KeyValuePair<TKey, TVal> bkp in BfTxOpMap)
      {
        FwkInfo("Inside BfTxOpMap");
        TKey BfTxKey = bkp.Key;
        if (!region.ContainsKey(BfTxKey))
          FwkException("getKey: expected key {0} is not present in the region key set", bkp.Key.ToString());
      }
      foreach (KeyValuePair<TKey, TVal> akp in AfTxOpMap)
      {
        FwkInfo("Inside AfTxOpMap");
        TKey AfTxKey = akp.Key;
        if (!region.ContainsKey(AfTxKey))
          FwkException("getKey: expected key {0} is not present in the region key set", akp.Key.ToString());
      }
    }

   #endregion
  }
}

