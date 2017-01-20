/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.Serialization;

namespace Apache.Geode.DUnitFramework
{
  /// <summary>
  /// Exception thrown when 'Call' is invoked on a client thread/process/...
  /// that has already exited (either due to some error/exception on the
  /// client side or due to its 'Dispose' function being called).
  /// </summary>
  [Serializable]
  public class ClientExitedException : Exception
  {
    /// <summary>
    /// Constructor to create an exception object with empty message.
    /// </summary>
    public ClientExitedException()
      : base()
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public ClientExitedException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public ClientExitedException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow deserialization of this exception by .Net remoting
    /// </summary>
    public ClientExitedException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }

  /// <summary>
  /// Exception thrown when an argument to a constructor/method is invalid.
  /// </summary>
  [Serializable]
  public class IllegalArgException : Exception
  {
    /// <summary>
    /// Constructor to create an exception object with empty message.
    /// </summary>
    public IllegalArgException()
      : base()
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public IllegalArgException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public IllegalArgException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow this exception to be Deserialized
    /// </summary>
    public IllegalArgException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }

  /// <summary>
  /// Exception thrown when the value for a given key is not found.
  /// </summary>
  [Serializable]
  public class KeyNotFoundException : Exception
  {
    /// <summary>
    /// Constructor to create an exception object with empty message.
    /// </summary>
    public KeyNotFoundException()
      : base()
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public KeyNotFoundException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public KeyNotFoundException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow this exception to be Deserialized
    /// </summary>
    public KeyNotFoundException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }

  /// <summary>
  /// Exception thrown when a client tries to connect to a server
  /// but the connection has not been initialized.
  /// </summary>
  [Serializable]
  public class NoServerConnectionException : Exception
  {
    /// <summary>
    /// Constructor to create an exception object with empty message.
    /// </summary>
    public NoServerConnectionException()
      : base()
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public NoServerConnectionException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public NoServerConnectionException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow this exception to be Deserialized
    /// </summary>
    public NoServerConnectionException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }

  /// <summary>
  /// Exception thrown when a client tries to connect to a server
  /// but the server could not be found.
  /// </summary>
  [Serializable]
  public class ServerNotFoundException : Exception
  {
    /// <summary>
    /// Constructor to create an exception object with empty message.
    /// </summary>
    public ServerNotFoundException()
      : base()
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public ServerNotFoundException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public ServerNotFoundException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow this exception to be Deserialized
    /// </summary>
    public ServerNotFoundException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }

  /// <summary>
  /// Exception thrown when some call on a client times out.
  /// </summary>
  [Serializable]
  public class ClientTimeoutException : NUnit.Framework.AssertionException
  {
    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public ClientTimeoutException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public ClientTimeoutException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow this exception to be Deserialized
    /// </summary>
    public ClientTimeoutException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }
}
