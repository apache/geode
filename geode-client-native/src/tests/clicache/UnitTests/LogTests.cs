//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.IO;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;


  [TestFixture]
  [Category("unicast_only")]
  public class LogTests : UnitTests
  {
    #region constants
    const int LENGTH_OF_BANNER = 53;
    #endregion

    #region Private functions

    private int NumOfLinesInFile(string fName)
    {
      try
      {
        int numLines = 0;
        FileStream fs = new FileStream(fName, FileMode.Open,
          FileAccess.Read, FileShare.ReadWrite);
        if (fs == null)
        {
          return -1;
        }
        StreamReader sr = new StreamReader(fs);
        string s;
        while ((s = sr.ReadLine()) != null && s.Length > 0)
        {
          numLines++;
        }
        sr.Close();
        fs.Close();
        return numLines;
      }
      catch
      {
        return -1;
      }
    }

    private int LinesAtLevel(LogLevel level)
    {
      int expected = (int)level;
      if ( level != LogLevel.Null ) {
        expected += LENGTH_OF_BANNER;
      }
      if (level >= LogLevel.Default)
      {
        expected--;
      }
      return expected;
    }

    private void LogAll(string logFileName,
      LogLevel level, int expectedLines)
    {
      string logFile = logFileName + ".log";

      Log.Close();
      File.Delete(logFile);
      Log.Init(level, logFileName);

      Log.Write(LogLevel.Error, "Error Message");
      Log.Write(LogLevel.Warning, "Warning Message");
      Log.Write(LogLevel.Info, "Info Message");
      Log.Write(LogLevel.Config, "Config Message");
      Log.Write(LogLevel.Fine, "Fine Message");
      Log.Write(LogLevel.Finer, "Finer Message");
      Log.Write(LogLevel.Finest, "Finest Message");
      Log.Write(LogLevel.Debug, "Debug Message");

      Log.Close();
      int lines = NumOfLinesInFile(logFile);
      Assert.AreEqual(expectedLines, lines, "Expected " + expectedLines.ToString() + " lines");

      File.Delete(logFile);
    }

    private void LogSome(string logFileName,
      LogLevel level, int expectedLines)
    {
      string logFile = logFileName + ".log";

      Log.Close();
      File.Delete(logFile);
      Log.Init(level, logFileName);

      Log.Write(LogLevel.Debug, "Debug Message");
      Log.Write(LogLevel.Config, "Config Message");
      Log.Write(LogLevel.Info, "Info Message");
      Log.Write(LogLevel.Warning, "Warning Message");
      Log.Write(LogLevel.Error, "Error Message");

      Log.Close();
      int lines = NumOfLinesInFile(logFile);
      Assert.AreEqual(expectedLines, lines, "Expected " + expectedLines.ToString() + " lines");

      File.Delete(logFile);
    }

    #endregion

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [Test]
    public void AllLevels()
    {
      for (LogLevel level = LogLevel.Error;
      level <= LogLevel.Debug; level = (LogLevel)((int)level + 1))
      {
        LogAll("all_logfile", level, LinesAtLevel(level));
      }
    }

    [Test]
    public void AllLevelsMacro()
    {
      for (LogLevel level = LogLevel.Error;
      level <= LogLevel.Debug; level = (LogLevel)((int)level + 1))
      {
        LogAll("logleveltest" + (int)level,
          level, LinesAtLevel(level));
      }
    }

    [Test]
    public void ConfigOnwards()
    {
      LogSome("logfile", LogLevel.Config, 4 + LENGTH_OF_BANNER );
    }

    [Test]
    public void InfoOnwards()
    {
      LogSome("logfile", LogLevel.Info, 3 + LENGTH_OF_BANNER );
    }

    [Test]
    public void WarningOnwards()
    {
      LogSome("logfile", LogLevel.Warning, 2 + LENGTH_OF_BANNER );
    }

    [Test]
    public void ErrorOnwards()
    {
      LogSome("logfile", LogLevel.Error, 1 + LENGTH_OF_BANNER );
    }

    [Test]
    public void NoLog()
    {
      LogSome("logfile", LogLevel.Null, 0);
    }
  }
}
