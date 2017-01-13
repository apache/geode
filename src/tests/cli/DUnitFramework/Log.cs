//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.IO;
using System.Text;

namespace GemStone.GemFire.DUnitFramework
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
