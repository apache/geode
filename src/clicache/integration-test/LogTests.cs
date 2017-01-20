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

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.DUnitFramework;


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
