//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Reflection;
using System.Threading;

namespace GemStone.GemFire.DUnitFramework
{
  /// <summary>
  /// Delegate to be invoked on the client side. This one is for
  /// a parameterless function that doesn't return any value.
  /// </summary>
  public delegate void UnitFnMethod();
  public delegate void UnitFnMethod<T1>(T1 param1);
  public delegate void UnitFnMethod<T1, T2>(T1 param1,
    T2 param2);
  public delegate void UnitFnMethod<T1, T2, T3>(T1 param1,
    T2 param2, T3 param3);
  public delegate void UnitFnMethod<T1, T2, T3, T4>(T1 param1,
    T2 param2, T3 param3, T4 param4);
  public delegate void UnitFnMethod<T1, T2, T3, T4, T5>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5);
  public delegate void UnitFnMethod<T1, T2, T3, T4, T5, T6>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6);
  public delegate void UnitFnMethod<T1, T2, T3, T4, T5, T6, T7>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7);

  /*
  public delegate void UnitFnMethodGeneric<TKey, TVal, T1, T2, T3, T4, T5, T6, T7>(T1<TKey, TVal> param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7);
   * */

  public delegate void UnitFnMethod<T1, T2, T3, T4, T5, T6, T7, T8>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8);
  public delegate void UnitFnMethod<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8, T9 param9);

  /// <summary>
  /// Delegate to be invoked on the client side. This one is for a function
  /// with arbitrary number of parameters that returns an object.
  /// </summary>
  /// <param name="data">Any data that needs to be passed to the function.</param>
  /// <returns>The result of the function as an 'object'.</returns>
  public delegate T0 UnitFnMethodR<T0>();
  public delegate T0 UnitFnMethodR<T0, T1>(T1 param1);
  public delegate T0 UnitFnMethodR<T0, T1, T2>(T1 param1,
    T2 param2);
  public delegate T0 UnitFnMethodR<T0, T1, T2, T3>(T1 param1,
    T2 param2, T3 param3);
  public delegate T0 UnitFnMethodR<T0, T1, T2, T3, T4>(T1 param1,
    T2 param2, T3 param3, T4 param4);
  public delegate T0 UnitFnMethodR<T0, T1, T2, T3, T4, T5>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5);
  public delegate T0 UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6);
  public delegate T0 UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6, T7>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7);

  /*
  public delegate T0 UnitFnMethodRUnitFnMethodGeneric<TKey, TVal, T0, T1, T2, T3, T4, T5, T6, T7>(T1<TKey, TVal> param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7);
   * */

  public delegate T0 UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6, T7, T8>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8);
  public delegate T0 UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>(T1 param1,
    T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8, T9 param9);

  /// <summary>
  /// An abstract class to encapsulate calling a function on a client
  /// thread/process/... Example implementations would be a 'Thread'
  /// or a process on a remote host.
  /// 
  /// Implementations are expected to catch the NUnit.Framework.AssertionException
  /// exception thrown by the client thread/process/... and throw them in the
  /// calling thread (i.e. in the function calls).
  /// </summary>
  public abstract class ClientBase : IDisposable
  {
    #region Private members

    private string m_id;
    private int m_numTasksRunning;

    #endregion

    #region Public accessors

    public virtual string ID
    {
      get
      {
        return m_id;
      }
      set
      {
        m_id = value;
      }
    }

    public bool TaskRunning
    {
      get
      {
        return (m_numTasksRunning > 0);
      }
    }

    public virtual string StartDir
    {
      get
      {
        return null;
      }
    }

    #endregion

    #region Call functions with no result

    /// <summary>
    /// Call a 'UnitFunction' on this thread/process/... blocking till
    /// the function completes.
    /// </summary>
    /// <param name="deleg">The function to run on this thread/process/...</param>
    /// <exception cref="ClientExitedException">
    /// If the client has exited due to some exception or if the object has been destroyed.
    /// </exception>
    public void Call(UnitFnMethod deleg)
    {
      CallFn(deleg, null);
    }

    public void Call<T1>(UnitFnMethod<T1> deleg, T1 param1)
    {
      CallFn(deleg, new object[] { param1 });
    }

    public void Call<T1, T2>(UnitFnMethod<T1, T2> deleg,
        T1 param1, T2 param2)
    {
      CallFn(deleg, new object[] { param1, param2 });
    }

    public void Call<T1, T2, T3>(UnitFnMethod<T1, T2, T3> deleg,
        T1 param1, T2 param2, T3 param3)
    {
      CallFn(deleg, new object[] { param1, param2, param3 });
    }

    public void Call<T1, T2, T3, T4>(UnitFnMethod<T1, T2, T3, T4> deleg,
        T1 param1, T2 param2, T3 param3, T4 param4)
    {
      CallFn(deleg, new object[] { param1, param2, param3, param4 });
    }

    public void Call<T1, T2, T3, T4, T5>(UnitFnMethod<T1, T2, T3, T4, T5> deleg,
        T1 param1, T2 param2, T3 param3, T4 param4, T5 param5)
    {
      CallFn(deleg, new object[] { param1, param2, param3, param4, param5 });
    }

    public void Call<T1, T2, T3, T4, T5, T6>(UnitFnMethod<T1, T2, T3, T4, T5, T6> deleg,
        T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6)
    {
      CallFn(deleg, new object[] { param1, param2, param3, param4, param5, param6 });
    }

    public void Call<T1, T2, T3, T4, T5, T6, T7>(UnitFnMethod<T1, T2, T3, T4, T5, T6, T7> deleg,
        T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7)
    {
      CallFn(deleg, new object[] { param1, param2, param3, param4, param5, param6, param7 });
    }

    public void Call<T1, T2, T3, T4, T5, T6, T7, T8>(UnitFnMethod<T1, T2, T3, T4, T5, T6, T7, T8> deleg,
        T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8)
    {
      CallFn(deleg, new object[] { param1, param2, param3, param4, param5, param6, param7, param8 });
    }

    public void Call<T1, T2, T3, T4, T5, T6, T7, T8, T9>(UnitFnMethod<T1, T2, T3, T4, T5, T6, T7, T8, T9> deleg,
        T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8, T9 param9)
    {
      CallFn(deleg, new object[] { param1, param2, param3, param4, param5, param6, param7, param8, param9 });
    }

    #endregion

    #region Call functions that return a result

    public void Call<T0>(UnitFnMethodR<T0> deleg,
      out T0 outparam)
    {
      outparam = (T0)CallFnR(deleg, null);
    }

    public void Call<T0, T1>(UnitFnMethodR<T0, T1> deleg,
      out T0 outparam, T1 param1)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1 });
    }

    public void Call<T0, T1, T2>(UnitFnMethodR<T0, T1, T2> deleg,
      out T0 outparam, T1 param1, T2 param2)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2 });
    }

    public void Call<T0, T1, T2, T3>(UnitFnMethodR<T0, T1, T2, T3> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3 });
    }

    public void Call<T0, T1, T2, T3, T4>(UnitFnMethodR<T0, T1, T2, T3, T4> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3, T4 param4)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3, param4 });
    }

    public void Call<T0, T1, T2, T3, T4, T5>(UnitFnMethodR<T0, T1, T2, T3, T4, T5> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3, T4 param4, T5 param5)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3, param4, param5 });
    }

    public void Call<T0, T1, T2, T3, T4, T5, T6>(UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3, param4, param5, param6 });
    }

    public void Call<T0, T1, T2, T3, T4, T5, T6, T7>(UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6, T7> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3, param4, param5, param6, param7 });
    }

    public void Call<T0, T1, T2, T3, T4, T5, T6, T7, T8>(UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6, T7, T8> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3, param4, param5, param6, param7, param8 });
    }

    public void Call<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>(UnitFnMethodR<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> deleg,
      out T0 outparam, T1 param1, T2 param2, T3 param3, T4 param4, T5 param5, T6 param6, T7 param7, T8 param8, T9 param9)
    {
      outparam = (T0)CallFnR(deleg, new object[] { param1, param2, param3, param4, param5, param6, param7, param8, param9 });
    }

    #endregion

    public void RemoveObject(object obj)
    {
      if (obj != null)
      {
        RemoveObjectID(obj.GetHashCode());
      }
    }

    public void RemoveType(Type tp)
    {
      if (tp != null)
      {
        RemoveObjectID(tp.GetHashCode());
      }
    }

    public virtual void IncrementTasksRunning()
    {
      Interlocked.Increment(ref m_numTasksRunning);
    }

    public virtual void DecrementTasksRunning()
    {
      Interlocked.Decrement(ref m_numTasksRunning);
    }

    public static void GetDelegateInfo(Delegate deleg, out string assemblyName,
        out string typeName, out string methodName, out int objectId)
    {
      MethodInfo mInfo = deleg.Method;
      object target = deleg.Target;
      Type delegType;

      if (target != null)
      {
        // Do not use ReflectedType, otherwise it returns base class type
        // for a derived class object when the derived class does not
        // override the method.
        delegType = target.GetType();
        objectId = target.GetHashCode();
      }
      else
      {
        // This case would be for static methods.
        delegType = mInfo.ReflectedType;
        objectId = delegType.GetHashCode();
      }
      assemblyName = delegType.Assembly.FullName;
      typeName = delegType.FullName;
      methodName = mInfo.Name;
    }

    public abstract void RemoveObjectID(int objectID);

    public abstract void CallFn(Delegate deleg, object[] paramList);

    public abstract object CallFnR(Delegate deleg, object[] paramList);

    public abstract string HostName
    {
      get;
    }

    /// <summary>
    /// Set the path of the log file.
    /// </summary>
    /// <param name="logPath">The path of the log file.</param>
    public abstract void SetLogFile(string logPath);

    /// <summary>
    /// Set logging level for the client.
    /// </summary>
    /// <param name="logLevel">The logging level to set.</param>
    public abstract void SetLogLevel(Util.LogLevel logLevel);

    /// <summary>
    /// Dump the stack trace of the client in the file set using the
    /// <see cref="SetLogFile"/> functions (or to console if not set).
    /// </summary>
    public abstract void DumpStackTrace();

    /// <summary>
    /// Kill the client after waiting for it to exit cleanly for the given
    /// milliseconds. This function should also dispose the object so that
    /// no other functions can be invoked on it.
    /// </summary>
    /// <param name="waitMillis">
    /// The number of milliseconds to wait before forcibly killing the client.
    /// A value of less than or equal to 0 means immediately killing.
    /// </param>
    public abstract void ForceKill(int waitMillis);

    /// <summary>
    /// Create a new client from the current instance.
    /// </summary>
    /// <param name="clientId">The ID of the new client.</param>
    /// <returns>An instance of a new client.</returns>
    public abstract ClientBase CreateNew(string clientId);

    /// <summary>
    /// Any actions required to be done to cleanup at the end of each test.
    /// </summary>
    public abstract void TestCleanup();

    /// <summary>
    /// Exit the client cleanly. This should normally wait for the current
    /// call to complete, perform any additional cleanup tasks registered and
    /// then close the client. To immediately kill the client use <see cref="ForceKill"/>.
    /// </summary>
    public abstract void Exit();

    ~ClientBase()
    {
      Exit();
    }

    #region IDisposable Members

    public void Dispose()
    {
      Exit();
      GC.SuppressFinalize(this);
    }

    #endregion
  }
}
