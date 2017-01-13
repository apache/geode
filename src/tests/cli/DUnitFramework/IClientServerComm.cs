//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Xml;

namespace GemStone.GemFire.DUnitFramework
{
  /// <summary>
  /// Interface for sending commands from server to client.
  /// </summary>
  public interface IClientComm
  {
    /// <summary>
    /// Synchronously call a function on the client without any return value.
    /// </summary>
    /// <param name="objectId">
    /// The ID of the object on which the method has to be invoked.
    /// </param>
    /// <param name="assemblyName">
    /// The name of the assembly where the type is defined.
    /// </param>
    /// <param name="typeName">The type name (class) of the object.</param>
    /// <param name="methodName">
    /// The name of the method to be invoked for the object.
    /// </param>
    /// <param name="paramList">The list of parameters for the method.</param>
    void Call(int objectId, string assemblyName,
      string typeName, string methodName, params object[] paramList);

    /// <summary>
    /// Synchronously call a function on the client returning some value.
    /// </summary>
    /// <param name="objectId">
    /// The ID of the object on which the method has to be invoked.
    /// </param>
    /// <param name="assemblyName">
    /// The name of the assembly where the type is defined.
    /// </param>
    /// <param name="typeName">The type name (class) of the object.</param>
    /// <param name="methodName">
    /// The name of the method to be invoked for the object.
    /// </param>
    /// <param name="paramList">The list of parameters for the method.</param>
    /// <returns>The result of the invocation of the method.</returns>
    object CallR(int objectId, string assemblyName,
      string typeName, string methodName, params object[] paramList);

    /// <summary>
    /// End the persistence of the object with the given ID.
    /// </summary>
    /// <param name="objectId">The ID of the object.</param>
    void RemoveObjectID(int objectId);

    /// <summary>
    /// Set path of the log file for the client.
    /// </summary>
    /// <param name="logPath">The path of the log file.</param>
    void SetLogFile(string logPath);

    /// <summary>
    /// Set logging level for the client.
    /// </summary>
    /// <param name="logLevel">The logging level to set.</param>
    void SetLogLevel(Util.LogLevel logLevel);

    /// <summary>
    /// Create a new client on the same host as of the running process
    /// with the given clientId and port.
    /// </summary>
    /// <param name="clientId">The ID of the new client.</param>
    /// <param name="port">The port of the new client.</param>
    /// <returns>Whether the process successfully started.</returns>
    bool CreateNew(string clientId, int port);

    /// <summary>
    /// Dump the stack trace of the process.
    /// Usually invoked before exiting when a timeout has occurred.
    /// </summary>
    void DumpStackTrace();

    /// <summary>
    /// Any cleanup to be performed at the end of a test.
    /// </summary>
    void TestCleanup();

    /// <summary>
    /// Signal that the client should exit.
    /// </summary>
    void Exit(bool force);

    /// <summary>
    /// Gets the process object of the client process
    /// </summary>
    /// <returns>Process object</returns>
    System.Diagnostics.Process GetClientProcess();   
  }
  public interface IClientCommV2 : IClientComm
  {
    /// <summary>
    /// Launches a client on the same host as of the running process
    /// with the given arguments.
    /// </summary>
    /// <param name="clientPath">Location and name of the client to be launched.</param>
    /// <param name="args">Arguments that are to be passed to the client.</param>
    /// <param name="proc">Process object of the started process.</param>
    /// <returns>Whether the process successfully started.</returns>
    bool LaunchNewClient(string clientPath, string args, out System.Diagnostics.Process proc);    
  }
  /// <summary>
  /// Interface for sending commands from client to server.
  /// </summary>
  public interface IDriverComm
  {
    /// <summary>
    /// Log a message on the main server thread.
    /// </summary>
    /// <param name="clientId">The ID of the client.</param>
    /// <param name="prefix">A string to be prefixed in the log line.</param>
    /// <param name="message">The log message.</param>
    void Log(string clientId, string prefix, string message);

    /// <summary>
    /// Signal that the client is up and running, so that the server
    /// can initiate connection to the client.
    /// </summary>
    /// <param name="clientId">The ID of the client.</param>
    void ClientListening(string clientId);

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
    bool RunWinTask(string clientId, string hostName, string taskSpec);

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
    string RunShellTask(string clientId, string hostName,
      string shellCmd, Dictionary<string, string> envVars);

    /// <summary>
    /// Send the TERM signal to the driver, that will cleanup clients and exit.
    /// </summary>
    void Term();
  }

  /// <summary>
  /// Interface for the blackboard for reading/writing values.
  /// </summary>
  public interface IBBComm
  {
    /// Write an object with a given key on the server. Useful for
    /// communication between different client processes. There is intentionally
    /// no data protection i.e. any client can (over-)write any object.
    /// The value should be Serializable i.e. the class (and all the types
    /// that it contains) should be marked with [Serializable] attribute.
    /// </summary>
    /// <param name="key">The key of the object to write.</param>
    /// <param name="value">The value of the object.</param>
    void WriteObject(string key, object value);

    /// <summary>
    /// Add a given value to an integer given the key on the server and return
    /// the new value; if the key does not exist or the type of object is not
    /// integer then it throws a <c>KeyNotFoundException</c>.
    /// </summary>
    /// <param name="key">The key of the integer to change.</param>
    /// <param name="incValue">
    /// The value to be added to the existing value.
    /// </param>
    /// <returns>
    /// The modified value if the key exists and value is an integer.
    /// </returns>
    /// <exception cref="KeyNotFoundException">
    /// When the current object associated with the key is not an integer,
    /// or the key does not exist.
    /// </exception>
    int AddInt(string key, int incValue);

    /// <summary>
    /// Add a given value to an integer given the key on the server and return
    /// the new value; if the key does not exist or the type of object is not
    /// integer then it sets the value to the given value.
    /// </summary>
    /// <param name="key">The key of the integer to change.</param>
    /// <param name="incValue">
    /// The value to be added to the existing value, or the value to be
    /// set if the key is not found or the type of object is not integer.
    /// </param>
    /// <returns>
    /// The modified value or created value when key does not exist or type
    /// of object is not integer.
    /// </returns>
    int AddOrSetInt(string key, int val);

    /// <summary>
    /// Read the value of an object for the given key from the server.
    /// Useful for communication between different client processes.
    /// </summary>
    /// <param name="key">The key of the object to write.</param>
    /// <returns>The value of the object with the given key.</returns>
    /// <exception cref="KeyNotFoundException">
    /// When the key does not exist.
    /// </exception>
    object ReadObject(string key);

    /// <summary>
    /// Remove the object with the given key from the server.
    /// </summary>
    /// <param name="key">The key of the object to remove.</param>
    void RemoveObject(string key);

    /// <summary>
    /// Clear all the keys/values from the blackboard server.
    /// </summary>
    void Clear();

    /// <summary>
    /// Exit the blackboard server.
    /// </summary>
    void Exit();
  }

  public static class CommConstants
  {
    public const string ClientService = "Client";
    public const string ClientIPC = "localClientIPCPort";

    public const string DriverService = "Driver";
    public const string ServerIPC = "localServerIPCPort";
    public const string BBService = "BlackBoard";
    public const string BBAddrEnvVar = "CSFWK_BBADDR";
    public const string DriverAddrEnvVar = "CSFWK_DRIVERADDR";
  }
}
