//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Management;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Security.Principal;

#pragma warning disable 618

namespace GemStone.GemFire.DUnitFramework
{
  using NUnit.Framework;
  
  public static class Util
  {
    /// <summary>
    /// Enumeration to specify logging level.
    /// For now this is same as the GFCPP product LogLevel enumeration.
    /// </summary>
    public enum LogLevel
    {
      None,
      Error,
      Warning,
      Info,
      Default,
      Config,
      Fine,
      Finer,
      Finest,
      Debug,
      All
    }

    #region Public accessors and members

    /// <summary>
    /// Channel to communicate with the server -- this must be setup by
    /// each client process.
    /// </summary>
    public static IDriverComm DriverComm
    {
      get
      {
        return m_driverComm;
      }
      set
      {
        m_driverComm = value;
      }
    }

    /// <summary>
    /// The port to use for running the driver.
    /// </summary>
    public static int DriverPort
    {
      get
      {
        return m_driverPort;
      }
      set
      {
        m_driverPort = value;
      }
    }

    /// <summary>
    /// Channel to communicate with the BBserver -- this must be setup by
    /// each client process.
    /// </summary>
    public static IBBComm BBComm
    {
      get
      {
        return m_bbComm;
      }
      set
      {
        m_bbComm = value;
      }
    }

    /// <summary>
    /// The URL of the BlackBoard server when using an external one.
    /// </summary>
    public static string ExternalBBServer
    {
      get
      {
        return m_externalBBServer;
      }
      set
      {
        m_externalBBServer = value;
      }
    }

    /// <summary>
    /// Get or set the ID for a client -- this must be set by each client
    /// process for client and server side logging purpose.
    /// </summary>
    public static string ClientId
    {
      get
      {
        return m_clientId;
      }
      set
      {
        m_clientId = value;
      }
    }

    public static string AppDomainPrefix
    {
      get
      {
        return "APPDOMAIN:";
      }
    }

    public static string IPAddressString
    {
      get
      {
        if (m_ipAddress == null)
        {
          NetworkInterface[] adapters =
            NetworkInterface.GetAllNetworkInterfaces();
          if (adapters != null)
          {
            foreach (NetworkInterface adapter in adapters)
            {
              if (adapter.OperationalStatus == OperationalStatus.Up &&
                adapter.NetworkInterfaceType != NetworkInterfaceType.Loopback)
              {
                UnicastIPAddressInformationCollection unicastAddresses =
                  adapter.GetIPProperties().UnicastAddresses;
                if (unicastAddresses != null)
                {
                  foreach (UnicastIPAddressInformation unicastAddress in
                    unicastAddresses)
                  {
                    IPHostEntry hostEntry;
                    try
                    {
                      hostEntry = Dns.GetHostByAddress(unicastAddress.Address);
                    }
                    catch
                    {
                      hostEntry = null;
                    }
                    if (hostEntry != null && hostEntry.HostName != null &&
                      hostEntry.HostName.Length > 0)
                    {
                      m_ipAddress = unicastAddress.Address;
                      break;
                    }
                  }
                  if (m_ipAddress != null) break;
                }
              }
            }
          }
        }
        return (m_ipAddress == null ? null : m_ipAddress.ToString());
      }
    }

    /// <summary>
    /// Get or set a number for the client.
    /// </summary>
    public static int ClientNum
    {
      get
      {
        return m_clientNum;
      }
      set
      {
        m_clientNum = value;
      }
    }

    public static bool LogAppend
    {
      get
      {
        return (m_log == null ? true : m_log.Append);
      }
      set
      {
        if (m_log == null)
        {
          throw new IllegalArgException("LogFile is not yet set.");
        }
        m_log.Append = value;
      }
    }

    public static string LogFile
    {
      get
      {
        return (m_log == null ? null : m_log.File);
      }
      set
      {
        if (value == null)
        {
          if (m_log != null)
          {
            m_log.Dispose();
          }
          m_log = null;
        }
        else if (m_log == null)
        {
          m_log = new UnitLog(value, true);
        }
        else
        {
          m_log.File = value;
          m_log.Append = true;
        }
      }
    }

    public static string LogPrefix
    {
      get
      {
        return m_logPrefix;
      }
      set
      {
        if (value == null)
        {
          m_logPrefix = DefaultLogPrefix;
        }
        else
        {
          m_logPrefix = value + ':';
        }
      }
    }

    public static string DUnitLogDir
    {
      get
      {
        if (m_logDir == null)
        {
          try
          {
            m_logDir = Util.GetEnvironmentVariable("DUNIT_LOGDIR");
            if (m_logDir.Length == 0)
            {
              m_logDir = null;
            }
          }
          catch (Exception)
          {
            m_logDir = null;
          }
        }
        return m_logDir;
      }
    }

    public static bool LogOnConsole
    {
      get
      {
        return m_logOnConsole;
      }
      set
      {
        m_logOnConsole = value;
      }
    }

    public static string AssemblyDir
    {
      get
      {
        if (m_assemblyDir == null)
        {
          m_assemblyDir = Util.NormalizePath(Path.GetDirectoryName(
            Assembly.GetExecutingAssembly().Location)).ToLower();
        }
        return m_assemblyDir;
      }
    }

    public static XmlNodeReaderWriter DefaultSettings
    {
      get
      {
        if (m_defaultSettings == null)
        {
          m_defaultSettings = XmlNodeReaderWriter.GetInstance("Settings.xml");
        }
        return m_defaultSettings;
      }
    }

    public static string HostName
    {
      get
      {
        if (m_hostName == null)
        {
          m_hostName = Dns.GetHostName();
        }
        return m_hostName;
      }
    }

    public static Process Process
    {
      get
      {
        if (m_proc == null)
        {
          m_proc = Process.GetCurrentProcess();
        }
        return m_proc;
      }
    }

    public static int PID
    {
      get
      {
        return Util.Process.Id;
      }
    }

    public static string ProcessName
    {
      get
      {
        return Util.Process.ProcessName;
      }
    }

    public static int ThreadID
    {
      get
      {
        return Thread.CurrentThread.ManagedThreadId;
      }
    }

    public static string ThreadName
    {
      get
      {
        return Thread.CurrentThread.Name;
      }
    }

    public static LogLevel CurrentLogLevel
    {
      get
      {
        return m_logLevel;
      }
      set
      {
        m_logLevel = value;
      }
    }

    public static LogLevel DefaultLogLevel
    {
      get
      {
        return LogLevel.Info;
      }
    }

    public static MethodBase CurrentTest
    {
      get
      {
        StackFrame sf = new StackFrame(1, false);
        return sf.GetMethod();
      }
    }

    public static string CurrentTestName
    {
      get
      {
        StackFrame sf = new StackFrame(1, false);
        return sf.GetMethod().Name;
      }
    }

    public static ICollection<UnitFnMethod> RegisteredTestCompleteDelegates
    {
      get
      {
        return m_testCompleteMap.Keys;
      }
    }

    public static string SystemType
    {
      get
      {
        if (m_systemType == null)
        {
          m_systemType = Environment.OSVersion.Platform.ToString();
          m_systemType = m_systemType.Substring(0, 3).ToUpper();
        }
        return m_systemType;
      }
    }

    public static string MarkerString = Environment.NewLine + "------------" +
      "-------------------------------------------------------" +
      Environment.NewLine;

    #endregion

    #region Private members

    private static Random m_rnd = new Random((int)DateTime.Now.Ticks);
    private static IDriverComm m_driverComm = null;
    private static int m_driverPort = RandPort(20000, 40000);
    private static IPAddress m_ipAddress = null;
    private static IBBComm m_bbComm = null;
    private static string m_externalBBServer = null;
    private static string m_clientId = "0";
    private static int m_clientNum = 0;
    private static string m_hostName = null;
    private static Process m_proc = null;
    private static volatile UnitLog m_log = null;
    private static LogLevel m_logLevel = DefaultLogLevel;
    private static string m_logDir = null;
    private static string m_logPrefix = DefaultLogPrefix;
    private static bool m_logOnConsole = false;
    private static string m_assemblyDir = null;
    private static XmlNodeReaderWriter m_defaultSettings = null;
    private static string m_systemType = null;
    private const string DefaultLogPrefix = "TEST:";
    private const string NoServerConnMsg = "Server connection not established.";
    private static Dictionary<UnitFnMethod, bool> m_testCompleteMap =
      new Dictionary<UnitFnMethod, bool>();

    #endregion

    /// <summary>
    /// Convenience function to convert a string to a byte.
    /// </summary>
    /// <param name="str">The string to convert.</param>
    /// <returns>The converted byte.</returns>
    public static byte String2Byte(string str)
    {
      if (str.StartsWith("0x"))
      {
        return byte.Parse(str.Substring(2), System.Globalization.NumberStyles.HexNumber);
      }
      return byte.Parse(str);
    }

    /// <summary>
    /// Convenience function to convert a string to a 16-bit short integer.
    /// </summary>
    /// <param name="str">The string to convert.</param>
    /// <returns>The converted 16-bit integer.</returns>
    public static Int16 String2Int16(string str)
    {
      if (str.StartsWith("0x"))
      {
        return Int16.Parse(str.Substring(2), System.Globalization.NumberStyles.HexNumber);
      }
      return Int16.Parse(str);
    }

    /// <summary>
    /// Convenience function to convert a string to a 32-bit integer.
    /// </summary>
    /// <param name="str">The string to convert.</param>
    /// <returns>The converted 32-bit integer.</returns>
    public static Int32 String2Int32(string str)
    {
      if (str.StartsWith("0x"))
      {
        return Int32.Parse(str.Substring(2), System.Globalization.NumberStyles.HexNumber);
      }
      return Int32.Parse(str);
    }

    /// <summary>
    /// Convenience function to convert a string to a 64-bit integer.
    /// </summary>
    /// <param name="str">The string to convert.</param>
    /// <returns>The converted 64-bit integer.</returns>
    public static Int64 String2Int64(string str)
    {
      if (str.StartsWith("0x"))
      {
        return Int64.Parse(str.Substring(2), System.Globalization.NumberStyles.HexNumber);
      }
      return Int64.Parse(str);
    }

    /// <summary>
    /// Convenience function to convert a string with ':' separated bytes to bytes.
    /// </summary>
    /// <param name="str">The string with different bytes separated by ':'</param>
    /// <returns>Array of converted bytes.</returns>
    public static byte[] String2Bytes(string str)
    {
      string[] strSplit = str.Split(':');
      byte[] buffer = new byte[strSplit.Length];
      for (int i = 0; i < strSplit.Length; i++)
      {
        buffer[i] = String2Byte(strSplit[i]);
      }
      return buffer;
    }

    /// <summary>
    /// Convenience function to convert a string with ':' separated bytes to string.
    /// Normally only useful for UTF encoded strings.
    /// </summary>
    /// <param name="str">The string with different bytes separated by ':'</param>
    /// <returns>Array of converted string.</returns>
    public static string String2String(string str)
    {
      string[] strSplit = str.Split(':');
      char[] chars = new char[strSplit.Length];
      for (int i = 0; i < strSplit.Length; i++)
      {
        chars[i] = (char)String2Int16(strSplit[i]);
      }
      return new string(chars);
    }

    /// <summary>
    /// Convenience generic function to compare two arbitrary arrays.
    /// </summary>
    /// <typeparam name="T">The type of the arrays.</typeparam>
    /// <param name="testArray">
    /// The array of type '<typeparamref name="T"/>' to be tested.
    /// </param>
    /// <param name="expectedArray">
    /// The expected array of type '<typeparamref name="T"/>'.
    /// </param>
    public static bool CompareArrays<T>(T[] testArray, T[] expectedArray)
    {
      if (testArray == null)
      {
        return testArray == expectedArray;
      }
      if (expectedArray == null)
      {
        return false;
      }
      if (testArray.Length != expectedArray.Length)
      {
        return false;
      }
      for (int i = 0; i < expectedArray.Length; i++)
      {
        if (!testArray[i].Equals(expectedArray[i]))
        {
          return false;
        }
      }
      return true;
    }

    /// <summary>
    /// Convenience generic function to compare two arbitrary arrays.
    /// Checks if 'expectedArray' is a prefix of 'testArray'
    /// </summary>
    /// <typeparam name="T">The type of the arrays.</typeparam>
    /// <param name="testArray">
    /// The array of type '<typeparamref name="T"/>' to be tested.
    /// </param>
    /// <param name="expectedArray">
    /// The expected array of type '<typeparamref name="T"/>'.
    /// </param>
    public static bool CompareArraysPrefix<T>(T[] testArray, T[] expectedArray)
    {
      if (testArray == null)
      {
        return testArray == expectedArray;
      }
      if (expectedArray == null)
      {
        return false;
      }
      if (testArray.Length < expectedArray.Length)
      {
        return false;
      }
      for (int i = 0; i < expectedArray.Length; i++)
      {
        if (!testArray[i].Equals(expectedArray[i]))
        {
          return false;
        }
      }
      return true;
    }

    /// <summary>
    /// Convenience generic function to compare two arbitrary arrays
    /// using NUnit.
    /// </summary>
    /// <typeparam name="T">The type of the arrays.</typeparam>
    /// <param name="expectedArray">
    /// The expected array of type '<typeparamref name="T"/>'.
    /// </param>
    /// <param name="actualArray">
    /// The actual result array of type '<typeparamref name="T"/>'.
    /// </param>
    public static void CompareTestArrays<T>(T[] expectedArray, T[] actualArray)
    {
      Assert.IsNotNull(expectedArray, "The expectedBytes cannot be null");
      Assert.IsNotNull(actualArray, "The bytes read cannot be null");

      Assert.IsTrue(expectedArray.Length <= actualArray.Length,
        "The length of the resulting bytes is too small.{0}Expected: {1}{2}" +
        "Got: {3}", Environment.NewLine, expectedArray.Length,
        Environment.NewLine, actualArray.Length);
      for (int i = 0; i < expectedArray.Length; i++)
      {
        Assert.AreEqual(expectedArray[i], actualArray[i],
          "Incorrect result in array index {0}.", i);
      }
    }

    /// <summary>
    /// Convenience generic function to compare two arbitrary arrays with
    /// custom comparer using NUnit.
    /// </summary>
    /// <typeparam name="T">The type of the arrays.</typeparam>
    /// <param name="expectedArray">
    /// The expected array of type '<typeparamref name="T"/>'.
    /// </param>
    /// <param name="actualArray">
    /// The actual result array of type '<typeparamref name="T"/>'.
    /// </param>
    public static void CompareTestArrays<T>(T[] expectedArray, T[] actualArray,
      UnitFnMethodR<bool, T, T> compareDelegate)
    {
      Assert.IsNotNull(expectedArray, "The expectedBytes cannot be null");
      Assert.IsNotNull(actualArray, "The bytes read cannot be null");

      Assert.IsTrue(expectedArray.Length <= actualArray.Length,
        "The length of the resulting bytes is too small.{0}Expected: {1}{2}" +
        "Got: {3}", Environment.NewLine, expectedArray.Length,
        Environment.NewLine, actualArray.Length);
      for (int i = 0; i < expectedArray.Length; i++)
      {
        Assert.IsTrue(compareDelegate(expectedArray[i], actualArray[i]),
          "Incorrect result in array index {0}; Expected: {1}, Got: {2}.",
          i, expectedArray[i], actualArray[i]);
      }
    }

    /// <summary>
    /// Get a random double between 0.0 and 1.0.
    /// </summary>
    /// <returns>the random double value</returns>
    public static double Rand()
    {
      return m_rnd.NextDouble();
    }

    /// <summary>
    /// Get a random integer.
    /// </summary>
    /// <param name="max">
    /// The exclusive upper bound of the random integer.
    /// </param>
    /// <returns>the random integer</returns>
    public static int Rand(int max)
    {
      return m_rnd.Next(max);
    }

    /// <summary>
    /// Get a random integer.
    /// </summary>
    /// <param name="min">
    /// The inclusive lower bound of the random integer.
    /// </param>
    /// <param name="max">
    /// The exclusive upper bound of the random integer.
    /// </param>
    /// <returns>the random integer</returns>
    public static int Rand(int min, int max)
    {
      return m_rnd.Next(min, max);
    }

    /// <summary>
    /// Get random bytes in a buffer.
    /// </summary>
    /// <param name="buffer">The buffer to fill with the random bytes.</param>
    public static void RandBytes(byte[] buffer)
    {
      m_rnd.NextBytes(buffer);
    }

    /// <summary>
    /// Get an arrray of random bytes.
    /// </summary>
    /// <param name="size">The size of the random byte array.</param>
    /// <returns>An array containing random bytes.</returns>
    public static byte[] RandBytes(int size)
    {
      if (size > 0)
      {
        byte[] buffer = new byte[size];
        m_rnd.NextBytes(buffer);
        return buffer;
      }
      return null;
    }

    /// <summary>
    /// Get a free random port in the given range.
    /// </summary>
    /// <param name="min">
    /// The inclusive lower bound of the random integer.
    /// </param>
    /// <param name="max">
    /// The exclusive upper bound of the random integer.
    /// </param>
    /// <returns>the free port number</returns>
    public static int RandPort(int min, int max)
    {
      int portNo;
      while (true)
      {
        portNo = Rand(min, max);
        Socket s = new Socket(AddressFamily.InterNetwork,
          SocketType.Stream, ProtocolType.Tcp);
        try
        {
          s.Bind(new IPEndPoint(IPAddress.Any, portNo));
          s.Close();
          return portNo;
        }
        catch (SocketException ex)
        {
          // EADDRINUSE?
          if (ex.ErrorCode == 10048)
          {
            continue;
          }
          else
          {
            throw;
          }
        }
      }
    }

    /// <summary>
    /// Swap two objects.
    /// </summary>
    /// <typeparam name="Tp">The type of the objects.</typeparam>
    /// <param name="first">The first object.</param>
    /// <param name="second">The second object.</param>
    public static void Swap<Tp>(ref Tp first, ref Tp second)
    {
      Tp tmp = first;
      first = second;
      second = tmp;
    }

    /// <summary>
    /// Start a process with the given path and arguments.
    /// </summary>
    /// <remarks>
    /// This function starts the process using shell (cmd.exe on windows)
    /// when the <see cref="Util.LogDir"/> is not set.
    /// </remarks>
    /// <param name="procPath">The path of the process to start.</param>
    /// <param name="procArgs">The arguments to be passed.</param>
    /// <param name="useShell">Whether to start in the DOS cmd shell.</param>
    /// <param name="startDir">
    /// The starting directory for the process;
    /// pass null to use the current working directory.
    /// </param>
    /// <param name="proc">The created Process as an out parameter.</param>
    /// <returns>Whether the process sucessfully started.</returns>
    public static bool StartProcess(string procPath, string procArgs,
      bool useShell, string startDir, bool redirectStdOut,
      bool redirectStdIn, bool redirectStdErr, out Process proc)
    {
      return StartProcess(procPath, procArgs, useShell, startDir, redirectStdOut,
      redirectStdIn, redirectStdErr, false, out proc);
    }
    
    public static bool StartProcess(string procPath, string procArgs,
      bool useShell, string startDir, bool redirectStdOut,
      bool redirectStdIn, bool redirectStdErr, bool createNoWindow, out Process proc)
    {
      ProcessStartInfo pInfo = new ProcessStartInfo(procPath, procArgs);

      // Force launch without a shell. This allows launching FwkClient.exe without a window so tests can run in CI.
      useShell = false;

      if (!useShell)
      {
        if (m_externalBBServer != null || createNoWindow)
        {
          pInfo.CreateNoWindow = true;
        }
        pInfo.UseShellExecute = false;
      }
      if (startDir != null && startDir.Length > 0)
      {
        pInfo.WorkingDirectory = startDir;
      }
      pInfo.RedirectStandardOutput = redirectStdOut;
      pInfo.RedirectStandardInput = redirectStdIn;
      pInfo.RedirectStandardError = redirectStdErr;
      proc = new Process();
      proc.StartInfo = pInfo;
      return proc.Start();
    }
    #region Methods for client side logging

    /// <summary>
    /// Format the message with time and other stuff appropriate to be
    /// written into the log.
    /// </summary>
    /// <param name="prefix">A string to be prefixed in the log line.</param>
    /// <param name="message">The message to be logged.</param>
    /// <returns></returns>
    public static string GetLogLine(string prefix, string message)
    {
      return GetLogLine(prefix, message, HostName + ':' + PID + ' ' + ThreadID);
    }

    /// <summary>
    /// Format the message with time and other stuff appropriate to be
    /// written into the log.
    /// </summary>
    /// <param name="logLevel">A string representing the logging level.</param>
    /// <param name="message">The message to be logged.</param>
    /// <param name="idString">The ID of process,thread to be logged.</param>
    /// <returns></returns>
    public static string GetLogLine(string prefix, string message, string idString)
    {
      DateTime now = DateTime.Now;

      return '[' + prefix + ' ' + now.ToShortDateString() + ' ' +
        now.Hour.ToString("D02") + ':' + now.Minute.ToString("D02") + ':' +
        now.Second.ToString("D02") + '.' + now.Millisecond.ToString("D03") +
        ' ' + idString + "] " + message + Environment.NewLine;
    }

    /// <summary>
    /// Log a message with the <see cref="CurrentLogLevel" />
    /// logging level on the client side.
    /// </summary>
    /// <param name="message">The message to log.</param>
    public static void Log(string message)
    {
      Log(m_logLevel, message);
    }

    /// <summary>
    /// Log a formatted message with the <see cref="CurrentLogLevel" />
    /// logging level on the client side.
    /// </summary>
    /// <param name="format">The formatted message to log.</param>
    /// <param name="args">
    /// The objects arguments to write into the formatted string.
    /// </param>
    public static void Log(string format, params object[] args)
    {
      Log(m_logLevel, string.Format(format, args));
    }

    /// <summary>
    /// Log a formatted message with the given
    /// logging level on the client side.
    /// </summary>
    /// <param name="logLevel">The required logging level.</param>
    /// <param name="format">The formatted message to log.</param>
    /// <param name="args">
    /// The objects arguments to write into the formatted string.
    /// </param>
    public static void Log(LogLevel logLevel, string format,
      params object[] args)
    {
      Log(logLevel, string.Format(format, args));
    }

    /// <summary>
    /// Log a message with the given
    /// logging level on the client side.
    /// </summary>
    /// <param name="logLevel">The required logging level.</param>
    /// <param name="message">The message to log.</param>
    public static void Log(LogLevel logLevel, string message)
    {
      string logLine = GetLogLine(m_logPrefix + logLevel.ToString(), message);
      RawLog(logLine);
      if (LogOnConsole && LogFile != null)
      {
        Console.WriteLine(logLine);
      }
    }

    /// <summary>
    /// Write the given message to the log file.
    /// </summary>
    /// <param name="message">The message to log.</param>
    public static void RawLog(string message)
    {
      if (m_log == null)
      {
        Console.WriteLine(message);
      }
      else
      {
        lock (m_log)
        {
          m_log.Write(message);
        }
      }
    }

    #endregion

    #region Convenience methods to perform server operations

    /// <summary>
    /// Log a message with the <see cref="CurrentLogLevel"/>
    /// logging level on the main server thread.
    /// </summary>
    /// <param name="message">The message to log.</param>
    public static void ServerLog(string message)
    {
      ServerLog(m_logLevel, message);
    }

    /// <summary>
    /// Log a message with the given
    /// logging level on the main server thread.
    /// </summary>
    /// <param name="logLevel">The required logging level.</param>
    /// <param name="message">The message to log.</param>
    public static void ServerLog(LogLevel logLevel, string message)
    {
      if (DriverComm != null)
      {
        DriverComm.Log(m_clientId, logLevel.ToString(), message);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Log a formatted message with the <see cref="CurrentLogLevel"/>
    /// logging level on the main server thread.
    /// </summary>
    /// <param name="format">The formatted message to log.</param>
    /// <param name="args">
    /// The objects arguments to write into the formatted string.
    /// </param>
    public static void ServerLog(string format, params object[] args)
    {
      ServerLog(m_logLevel, format, args);
    }

    /// <summary>
    /// Log a formatted message with the given
    /// logging level on the main server thread.
    /// </summary>
    /// <param name="logLevel">The required logging level.</param>
    /// <param name="format">The formatted message to log.</param>
    /// <param name="args">
    /// The objects arguments to write into the formatted string.
    /// </param>
    public static void ServerLog(LogLevel logLevel, string format,
      params object[] args)
    {
      if (DriverComm != null)
      {
        string message = string.Format(format, args);
        DriverComm.Log(m_clientId, logLevel.ToString(), message);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Write an object with a given key and black-board on the server.
    /// Useful for communication between different client processes. There is
    /// intentionally no data protection i.e. any client can (over-)write any
    /// object. The value should be Serializable i.e. the class (and all the
    /// types that it contains) should be marked with [Serializable] attribute.
    /// </summary>
    /// <param name="bbName">The name of the black-board to use.</param>
    /// <param name="key">The key of the object to write.</param>
    /// <param name="value">The value of the object.</param>
    public static void BBSet(string bbName, string key, object value)
    {
      if (BBComm != null)
      {
        BBComm.WriteObject(bbName + '.' + key, value);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Read the value of an object for the given key and black-board from the
    /// server. Useful for communication between different client processes.
    /// </summary>
    /// <param name="bbName">The name of the black-board to use.</param>
    /// <param name="key">The key of the object to read.</param>
    /// <returns>The value of the object with the given key.</returns>
    public static object BBGet(string bbName, string key)
    {
      if (BBComm != null)
      {
        return BBComm.ReadObject(bbName + '.' + key);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Remove the value of an object for the given key and black-board from the
    /// server. Useful for communication between different client processes.
    /// </summary>
    /// <param name="bbName">The name of the black-board to use.</param>
    /// <param name="key">The key of the object to remove.</param>
    public static void BBRemove(string bbName, string key)
    {
      if (BBComm != null)
      {
        BBComm.RemoveObject(bbName + '.' + key);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    public static int BBAdd(string bbName, string key, int incValue)
    {
      if (BBComm != null)
      {
        return BBComm.AddInt(bbName + '.' + key, incValue);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    public static int BBIncrement(string bbName, string key)
    {
      if (BBComm != null)
      {
        return BBComm.AddOrSetInt(bbName + '.' + key, 1);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    public static int BBDecrement(string bbName, string key)
    {
      if (BBComm != null)
      {
        return BBComm.AddInt(bbName + '.' + key, -1);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Signal that the client is up and running, so that the server
    /// can initiate connection to the client.
    /// </summary>
    public static void ClientListening()
    {
      if (DriverComm != null)
      {
        DriverComm.ClientListening(m_clientId);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Client callback to request server to start a task on another Windows machine.
    /// </summary>
    /// <param name="clientId">The ID of the client.</param>
    /// <param name="hostName">
    /// The host name where the task is to be started.
    /// </param>
    /// <param name="taskSpec">
    /// A specification of the task to be executed on the new client.
    /// </param>
    /// <returns>Whether the task successfully executed.</returns>
    public static bool RunClientWinTask(string clientId, string hostName, string taskSpec)
    {
      if (DriverComm != null)
      {
        return DriverComm.RunWinTask(clientId, hostName, taskSpec);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    /// <summary>
    /// Client callback to request server to run a shell command on another machine.
    /// </summary>
    /// <param name="clientId">The ID of the client.</param>
    /// <param name="hostName">
    /// The host name where the task is to be started.
    /// </param>
    /// <param name="shellCmd">
    /// Command to be executed using the shell.
    /// </param>
    /// <param name="envVars">
    /// An optional list of environment variables to be set in the shell.
    /// </param>
    /// <returns>The standard output of the task.</returns>
    public static string RunClientShellTask(string clientId, string hostName,
      string shellCmd, Dictionary<string, string> envVars)
    {
      if (DriverComm != null)
      {
        return DriverComm.RunShellTask(clientId, hostName, shellCmd, envVars);
      }
      else
      {
        throw new NoServerConnectionException(NoServerConnMsg);
      }
    }

    #endregion

    /// <summary>
    /// Normalize a given path by finding its full path and
    /// converting to POSIX convention.
    /// </summary>
    /// <param name="path">The path to normalize.</param>
    /// <returns>The normalized path.</returns>
    public static string NormalizePath(string path)
    {
      return Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar).Replace('\\', '/');
    }

    /// <summary>
    /// Delete a share (if any) of the given directory.
    /// </summary>
    /// <param name="directoryPath">
    /// The path of the directory that is shared.
    /// </param>
    public static void DeleteSharePath(string directoryPath)
    {
      directoryPath = directoryPath.Replace(@"\", @"\\");
      using (ManagementObjectSearcher searcher =
        new ManagementObjectSearcher("SELECT Name FROM Win32_Share WHERE Path='" +
        directoryPath + "'"))
      {
        ManagementObjectCollection objColl = searcher.Get();
        foreach (ManagementObject obj in objColl)
        {
          obj.Delete();
          obj.Dispose();
        }
      }
    }

    /// <summary>
    /// Delete a share (if any) of the given name.
    /// </summary>
    /// <param name="shareName">The name of the share.</param>
    public static void DeleteShareName(string shareName)
    {
      using (ManagementObjectSearcher searcher =
        new ManagementObjectSearcher("SELECT Name FROM Win32_Share WHERE Name='" +
        shareName + "'"))
      {
        ManagementObjectCollection objColl = searcher.Get();
        foreach (ManagementObject obj in objColl)
        {
          obj.Delete();
          obj.Dispose();
        }
      }
    }

    /// <summary>
    /// Check whether the given share name already exists.
    /// </summary>
    /// <param name="shareName">The name of the share to check.</param>
    /// <returns>
    /// True if the share name already exists; false otherwise.
    /// </returns>
    public static bool ShareExists(string shareName)
    {
      using (ManagementObjectSearcher searcher =
        new ManagementObjectSearcher("SELECT Name FROM Win32_Share WHERE Name='" +
        shareName + "'"))
      {
        ManagementObjectCollection objColl = searcher.Get();
        return (objColl != null && objColl.Count > 0);
      }
    }

    public static ManagementObject permissionForShareFolder()
    {
      // Create a WMI instance of the principal (Win32_Trustee).
      //Getting the Sid value is not required for Vista.
      NTAccount account = new NTAccount(System.Environment.UserDomainName, System.Environment.UserName);
      SecurityIdentifier sid = (SecurityIdentifier)account.Translate(typeof(SecurityIdentifier));
      byte[] sidArray = new byte[sid.BinaryLength];
      sid.GetBinaryForm(sidArray, 0);

      ManagementObject Trustee = new ManagementClass(new ManagementPath("Win32_Trustee"), null);
      Trustee["Domain"] = System.Environment.UserDomainName;
      Trustee["Name"] = System.Environment.UserName;
      Trustee["SID"] = sidArray;
      //Create a WMI instance of Win32_Ace, assign the Trustee to this Win32_Ace instance.
      ManagementObject AdminACE = new ManagementClass(new ManagementPath("Win32_Ace"), null);
      AdminACE["AccessMask"] = 2032127;
      AdminACE["AceFlags"] = 3;
      AdminACE["AceType"] = 0;
      AdminACE["Trustee"] = Trustee;

      //To know what values you need to put there, check msdn (link). I actually encourage you to write an enum flag to encapsulate those values.
      //In nut shell, 2032127 is for full access, Access Flags 3 is for non-container and container child objects to inherit the ACE, and Ace Type 0 is to allow the trustee to access it.
      //Create a WMI instance of the security descriptor (Win32_SecurityDescriptor)

      ManagementObject secDescriptor = new ManagementClass(new ManagementPath("Win32_SecurityDescriptor"), null);
      secDescriptor["ControlFlags"] = 4; //SE_DACL_PRESENT
      secDescriptor["DACL"] = new object[] { AdminACE };

      return secDescriptor;
      //Now, create a WMI instance of Win32_Share, and setup the security.

      //        ManagementObject share = new ManagementObject(@"\\ContosoServer\root\cimv2:Win32_Share.Name='JohnShare'");
      //share.InvokeMethod("SetShareInfo", new object[] {Int32.MaxValue, "This is John's share", secDescriptor});
      //Check the return value of the Invoke, the method returns an Object, convert it to Int32. 


    }

    /// <summary>
    /// Make a given directory shared, overwriting previous share (if any).
    /// </summary>
    /// <param name="directoryPath">The directory to be made shared.</param>
    /// <param name="sharePrefix">
    /// The prefix for the share name.
    /// The actual name will be of the form '[prefix][num]' for the first free
    /// share name available for all integer [num] starting from 1.
    /// </param>
    /// <param name="shareDesc">A description for the share.</param>
    /// <returns>The share name of the directory.</returns>
    public static string ShareDirectory(string directoryPath,
      string sharePrefix, string shareDesc)
    {
      string resShareName = null;
      DeleteSharePath(directoryPath);
      using (ManagementClass manage = new ManagementClass("Win32_Share"))
      {
        int num = 1;
        string shareName;
        while (ShareExists((shareName = sharePrefix + num)))
        {
          num++;
        }

        ManagementBaseObject inputArgs = manage.GetMethodParameters("Create");
        inputArgs["Name"] = shareName;
        inputArgs["Path"] = directoryPath;
        inputArgs["Description"] = shareDesc;
        inputArgs["Type"] = 0; // Disk share type
        inputArgs["Access"] = permissionForShareFolder();
        ManagementBaseObject outParams = manage.InvokeMethod("Create", inputArgs, null);
        uint ret = (uint)outParams.Properties["ReturnValue"].Value;
        if (ret == 0)
        {
          resShareName = "//" + IPAddressString + '/' + shareName;
        }
        // Set the correct permissions on the share:
        // full control for the current user while readable for "Everyone"
        //ManagementObject v_theTrusteeObject =
        //  new ManagementClass("Win32_Trustee").CreateInstance();
        //Object[] v_theSID = { 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0 };
        //v_theTrusteeObject["Domain"] = null;
        //v_theTrusteeObject["Name"] = "Everyone";
        //v_theTrusteeObject["SID"] = v_theSID;

        //ManagementObject v_theAceObject =
        //  new ManagementClass("Win32_ACE").CreateInstance();
        //v_theAceObject["AccessMask"] = (uint)0xffffffff;
        //v_theAceObject["AceFlags"] = 0x3;
        //v_theAceObject["AceType"] = 0;
        //v_theAceObject["Trustee"] = v_theTrusteeObject;

        //ManagementObject v_theSecDescObject =
        //  new ManagementClass("Win32_SecurityDescriptor").CreateInstance();
        //object[] v_theAceArray = { v_theAceObject };
        //v_theSecDescObject["DACL"] = v_theAceArray;

        //ManagementBaseObject secArgs = manage.GetMethodParameters("SetShareInfo");
        //secArgs["Access"] = v_theSecDescObject;
        //ManagementBaseObject secOut = manage.InvokeMethod("SetShareInfo", secArgs, null);
      }
      return resShareName;
    }

    /// <summary>
    /// Check whether the given hostname belongs to this host
    /// </summary>
    /// <param name="hostName">The hostname to check</param>
    /// <returns>True if the given hostname belongs to this host</returns>
    public static bool IsHostMyself(string hostName)
    {
      if (hostName.ToLower() == "localhost")
      {
        return true;
      }
      // Get the IP addresses associated with the hostName
      IPAddress[] ipAddresses = Dns.GetHostAddresses(hostName);
      if (ipAddresses != null)
      {
        // Check if any of those IP address is of a network interface
        foreach (IPAddress ipAddress in ipAddresses)
        {
          NetworkInterface[] adapters =
            NetworkInterface.GetAllNetworkInterfaces();
          if (adapters != null)
          {
            foreach (NetworkInterface adapter in adapters)
            {
              foreach (UnicastIPAddressInformation ipInfo in
                adapter.GetIPProperties().UnicastAddresses)
              {
                if (ipInfo.Address.Equals(ipAddress))
                {
                  return true;
                }
              }
            }
          }
        }
      }
      return false;
    }

    /// <summary>
    /// Return a string for host type converting to standard string
    /// representation of 3 characters.
    /// </summary>
    /// <param name="hostTypeStr">
    /// A host type string typically obtained by invoking uname
    /// </param>
    public static string GetHostType(string hostTypeStr)
    {
      if (!string.IsNullOrEmpty(hostTypeStr))
      {
        try
        {
          hostTypeStr = hostTypeStr.Trim().Substring(0, 3).ToUpper();
          if (hostTypeStr == "CYG")
          {
            hostTypeStr = "WIN";
          }
        }
        catch (ArgumentOutOfRangeException)
        {
        }
      }
      else 
        hostTypeStr = "WIN";
      return hostTypeStr;
    }

    /// <summary>
    /// Get value of given environment variable accomodating for the
    /// convention of variables like [varname].[system type]
    /// </summary>
    /// <param name="envVar">
    /// The name of the environment variable. If [envVar].[system type]
    /// is set then value of that variable is returned as result else
    /// value of [envVar] is returned.
    /// </param>
    /// <param name="systemType">
    /// A string representing the type of the system.
    /// This is typically obtained using uname command.
    /// </param>
    public static string GetEnvironmentVariable(string envVar,
      string systemType)
    {
      string envVal;
      if (systemType == null)
      {
        systemType = SystemType;
      }
      if (envVar.EndsWith('.' + systemType))
      {
        envVal = Environment.GetEnvironmentVariable(envVar);
        if (envVal == null || envVal.Length == 0)
        {
          envVal = Environment.GetEnvironmentVariable(envVar.Substring(0,
            envVar.Length - systemType.Length - 1));
        }
      }
      else
      {
        envVal = Environment.GetEnvironmentVariable(envVar + '.' + systemType);
        if (envVal == null || envVal.Length == 0)
        {
          envVal = Environment.GetEnvironmentVariable(envVar);
        }
      }
      return envVal;
    }

    /// <summary>
    /// Get value of given environment variable accomodating for the
    /// convention of variables like [varname].[system type]
    /// </summary>
    /// <param name="envVar">
    /// The name of the environment variable. If [envVar].[system type]
    /// is set then value of that variable is returned as result else
    /// value of [envVar] is returned.
    /// </param>
    public static string GetEnvironmentVariable(string envVar)
    {
      return GetEnvironmentVariable(envVar, SystemType);
    }

    /// <summary>
    /// Set given environment variable to given value accomodating for the
    /// convention of variables like [varname].[system type]
    /// </summary>
    /// <param name="envVar">
    /// The name of the environment variable. This can be of the form
    /// [varname].[system type] and if the [system type] matches the type of
    /// this system then just [varname] is also set to the given value else
    /// value of [varname].[my system type] is also set to the given value.
    /// </param>
    /// <param name="envVal">The value of the environment variable</param>
    public static void SetEnvironmentVariable(string envVar, string envVal)
    {
      Environment.SetEnvironmentVariable(envVar, envVal);
      if (envVar.EndsWith('.' + SystemType))
      {
        Environment.SetEnvironmentVariable(envVar.Substring(0,
          envVar.Length - SystemType.Length - 1), envVal);
      }
      else
      {
        string currentVal = Environment.GetEnvironmentVariable(
          envVar + '.' + SystemType);
        if (currentVal == null || currentVal.Length == 0)
        {
          Environment.SetEnvironmentVariable(envVar + '.' + SystemType,
            envVal);
        }
      }
    }

    /// <summary>
    /// Hide the Plink password arg (if any) from the given args.
    /// </summary>
    /// <remarks>
    /// Will not work properly if the password contains space/tab characters.
    /// </remarks>
    /// <param name="sshArgs">The arguments to the plink command.</param>
    /// <returns>The arguments with the password hidden.</returns>
    public static string HideSshPassword(string sshArgs)
    {
      return Regex.Replace(sshArgs, @"-pw[ \t]*[^ \t]*", "-pw [hidden]");
    }


    /// <summary>
    /// Hide the Psexec password arg (if any) from the given args.
    /// </summary>
    /// <remarks>
    /// Will not work properly if the password contains space/tab characters.
    /// </remarks>
    /// <param name="psexecArgs">The arguments to the psexec command.</param>
    /// <returns>The arguments with the password hidden.</returns>
    public static string HidePsexecPassword(string psexecArgs)
    {
      return Regex.Replace(psexecArgs, @" -p [ \t]*[^ \t]*", "-p [hidden]");
    }
    
    /// <summary>
    /// Register a delegate to be invoked when the test on client completes.
    /// </summary>
    /// <param name="deleg">The delegate to be registered.</param>
    public static void RegisterTestCompleteDelegate(UnitFnMethod deleg)
    {
      if (deleg != null)
      {
        m_testCompleteMap[deleg] = true;
      }
    }

    /// <summary>
    /// Get the base log directory for all tests for the given host type.
    /// </summary>
    /// <param name="hostType">The host type of the node.</param>
    /// <returns>
    /// Path of the log directory for the host or "." if environment is not
    /// properly set up.
    /// </returns>
    public static string GetFwkLogDir(string hostType)
    {
      string baseDir = Util.GetEnvironmentVariable("FWK_LOGDIR", hostType);
      if (baseDir != null && baseDir.Length > 0)
      {
        string logDir = Util.NormalizePath(Util.LogFile);
        int logSuffixPos = logDir.IndexOf("/FwkCS_");
        string logSuffix = logDir.Substring(logSuffixPos + 1);
        int logSuffixEndPos = logSuffix.IndexOf('/');
        baseDir = baseDir + '/' + logSuffix.Substring(0, logSuffixEndPos);
      }
      return baseDir;
    }

    /// <summary>
    /// Get the base directory where logs go for the current test for
    /// the given host type.
    /// </summary>
    /// <param name="hostType">The host type of the node.</param>
    /// <returns>
    /// Path of the log directory for the host or "." if environment is not
    /// properly set up.
    /// </returns>
    public static string GetLogBaseDir(string hostType)
    {
      string baseDir = null;
      if (Util.LogFile != null && Util.LogFile.Length > 0)
      {
        string logDir = Util.GetEnvironmentVariable("FWK_LOGDIR", hostType);
        baseDir = Util.NormalizePath(Path.GetDirectoryName(Util.LogFile));
        if (logDir != null && logDir.Length > 0)
        {
          int baseSuffixPos = baseDir.IndexOf("/FwkCS_");
          baseDir = logDir + baseDir.Substring(baseSuffixPos);
        }
      }
      if (baseDir == null || baseDir.Length == 0)
      {
        baseDir = ".";
      }
      return baseDir;
    }

    /// <summary>
    /// Try to copy the given set of files (matching the given pattern) for
    /// source directory to destination directory.
    /// </summary>
    /// <param name="sourceDir">source directory containing the files</param>
    /// <param name="pattern">pattern to match the files</param>
    /// <param name="destDir">destination directory for the files</param>
    /// <param name="recursive">true to do recursive copy</param>
    public static void CopyFiles(string sourceDir, string pattern,
      string destDir, bool recursive)
    {
      try
      {
        SearchOption opt = (recursive ? SearchOption.AllDirectories :
          SearchOption.TopDirectoryOnly);
        sourceDir = NormalizePath(sourceDir);
        string[] files = Directory.GetFiles(sourceDir, pattern, opt);
        for (int index = 0; index < files.Length; ++index)
        {
          try
          {
            string file = NormalizePath(files[index]);
            string fileName = Path.GetFileName(file);
            string dirPath = Util.NormalizePath(destDir + '/' +
              Path.GetDirectoryName(file.Replace(sourceDir, "")));
            if (!Directory.Exists(dirPath))
            {
              Directory.CreateDirectory(dirPath);
            }
            File.Copy(file, dirPath + '/' + fileName);
          }
          catch (Exception)
          {
          }
        }
      }
      catch (Exception)
      {
      }
    }
  }

  /// <summary>
  /// A simple generic reference class that wraps a value type.
  /// </summary>
  /// <typeparam name="T">The type to be wrapped.</typeparam>
  /// <remarks>
  /// This class is useful to make a value type act as a reference type
  /// so that different parts of code can modify same value.
  /// </remarks>
  public class RefValue<T>
    where T : struct
  {
    public T Value
    {
      get
      {
        return m_value;
      }
      set
      {
        m_value = value;
      }
    }

    public RefValue(T value)
    {
      m_value = value;
    }

    public override bool Equals(object obj)
    {
      return m_value.Equals(obj);
    }

    public override int GetHashCode()
    {
      return m_value.GetHashCode();
    }

    public override string ToString()
    {
      return m_value.ToString();
    }

    public static implicit operator T(RefValue<T> value)
    {
      return value.m_value;
    }

    public static explicit operator RefValue<T>(T value)
    {
      return new RefValue<T>(value);
    }

    private T m_value;
  }

  public class PaceMeter
  {
    private DateTime m_endTime;
    private int m_current;
    private TimeSpan m_waitTime;
    private int m_opsLimit;

    public PaceMeter(int ops)
      : this(ops, 1)
    {
    }

    public PaceMeter(int ops, int seconds)
    {
      m_current = 0;
      m_opsLimit = 0;
      if (ops > 0 && seconds > 0)
      {
        if (ops < seconds) // This is the "seconds per op" case
        {
          m_opsLimit = ops;
          m_waitTime = TimeSpan.FromSeconds(seconds);
        }
        else
        {
          int opsSec = ops / seconds;
          if (opsSec > 4) // Use sample interval of 0.25 secs
          {
            m_opsLimit = opsSec / 4;
            m_waitTime = TimeSpan.FromSeconds(0.25);
          }
          else // Use sample interval of 1.0 secs
          {
            m_opsLimit = opsSec;
            m_waitTime = TimeSpan.FromSeconds(1.0);
          }
        }
        ResetCurrent(DateTime.Now);
      }
    }

    public void CheckPace()
    {
      if (m_opsLimit > 0)
      {
        if (++m_current >= m_opsLimit)
        {
          DateTime now = DateTime.Now;
          if (m_endTime > now)
          {
            TimeSpan sleepSpan = m_endTime - now;
            //Util.Log("PaceMeter:CheckPace:: Expected to end at: {0}:{1}:{2}.{3}, sleep span: {4}",
            //  m_endTime.Hour.ToString("D02"), m_endTime.Minute.ToString("D02"),
            //  m_endTime.Second.ToString("D02"), m_endTime.Millisecond.ToString("D03"), sleepSpan);
            Thread.Sleep(sleepSpan);
          }
          ResetCurrent(now);
        }
      }
    }

    private void ResetCurrent(DateTime now)
    {
      m_current = 0;
      m_endTime = now + m_waitTime;
    }
  }
}
