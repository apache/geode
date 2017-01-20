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
using System.IO;
using System.Text;

namespace Apache.Geode.DUnitFramework
{
  /// <summary>
  /// Class to do logging.
  /// </summary>
  public class UnitLog : IDisposable
  {
    #region Private members

    private string m_file;
    private FileStream m_stream;
    private bool m_append;

    #endregion

    public string File
    {
      get
      {
        return m_file;
      }
      set
      {
        Close();
        m_file = value;
        Init();
      }
    }

    public bool Append
    {
      get
      {
        return m_append;
      }
      set
      {
        m_append = value;
      }
    }

    public UnitLog(string file)
    {
      m_file = file;
      m_stream = null;
      m_append = true;
    }

    public UnitLog(string file, bool append)
    {
      m_file = file;
      m_stream = null;
      m_append = append;
    }

    /// <summary>
    /// Initialize the log file.
    /// </summary>
    private void Init()
    {
      if (m_stream == null)
      {
        try
        {
          string dirName = Path.GetDirectoryName(m_file);
          if (dirName != null && dirName.Length > 0 && !Directory.Exists(dirName))
          {
            Directory.CreateDirectory(dirName);
          }
          m_stream = new FileStream(m_file, (m_append ? FileMode.Append :
            FileMode.Create), FileAccess.Write, FileShare.ReadWrite);
        }
        catch (Exception)
        {
          m_stream = null;
        }
      }
    }

    /// <summary>
    /// Write the given message to the log file with default encoding.
    /// </summary>
    /// <param name="message">The message to write.</param>
    public void Write(string message)
    {
      Write(message, Encoding.Default);
    }

    /// <summary>
    /// Write the given message to the log file with the given encoding.
    /// </summary>
    /// <param name="message">The message to write.</param>
    /// <param name="encoding">The encoding of the message.</param>
    public void Write(string message, Encoding encoding)
    {
      Init();
      if (m_stream != null)
      {
        byte[] messageBytes = encoding.GetBytes(message);
        int numTries = 3;
        while (numTries-- > 0)
        {
          try
          {
            m_stream.Seek(0, SeekOrigin.End);
            m_stream.Write(messageBytes, 0, messageBytes.Length);
            m_stream.Flush();
            break;
          }
          catch
          {
            System.Threading.Thread.Sleep(50);
          }
        }
      }
    }

    /// <summary>
    /// Close the client side log.
    /// </summary>
    private void Close()
    {
      if (m_stream != null)
      {
        try
        {
          m_stream.Close();
        }
        catch
        {
        }
        m_stream = null;
      }
    }

    #region IDisposable Members

    /// <summary>
    /// Disposer closes the log properly.
    /// </summary>
    public void Dispose()
    {
      Close();
      GC.SuppressFinalize(this);
    }

    #endregion

    /// <summary>
    /// Finalizer for the case when the Disposer is not called.
    /// </summary>
    ~UnitLog()
    {
      Close();
    }
  }
}
