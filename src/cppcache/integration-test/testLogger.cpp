/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testLogger"

#include "fw_helper.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#ifndef WIN32
#include <unistd.h>
#endif

#define LENGTH_OF_BANNER 9

using namespace gemfire;

int numOfLinesInFile(const char* fname) {
  char line[2048];
  char* read;
  int ln_cnt = 0;
  FILE* fp = fopen(fname, "r");
  if (fp == NULL) {
    return -1;
  }
  while (!!(read = fgets(line, sizeof line, fp))) {
    printf("%d:%s", ++ln_cnt, line);
  }

  if (!feof(fp)) {
    fclose(fp);
    return -2;
  }
  fclose(fp);
  return ln_cnt;
}

void testLogFnError() {
  LogFn logFn("TestLogger::testLogFnError", Log::Error);
  Log::error("...");
}

void testLogFnWarning() {
  LogFn logFn("TestLogger::testLogFnWarning", Log::Warning);
  Log::warning("...");
}

void testLogFnInfo() {
  LogFn logFn("TestLogger::testLogFnInfo", Log::Info);
  Log::info("...");
}

void testLogFnConfig() {
  LogFn logFn("TestLogger::testLogFnConfig", Log::Config);
  Log::config("...");
}

void testLogFnFine() {
  LogFn logFn("TestLogger::testLogFnFine", Log::Fine);
  Log::fine("...");
}

void testLogFnFiner() {
  LogFn logFn("TestLogger::testLogFnFiner", Log::Finer);
  Log::finer("...");
}

void testLogFnFinest() {
  LogFn logFn("TestLogger::testLogFnFinest");
  Log::finest("...");
}

void testLogFnDebug() {
  LogFn logFn("TestLogger::testLogFnDebug", Log::Debug);
  Log::debug("...");
}

int expected(int level) {
  int expected = level;
  if (level != Log::None) {
    expected += LENGTH_OF_BANNER;
  }
  if (level >= Log::Default) {
    expected--;
  }
  return expected;
}

BEGIN_TEST(REINIT)
  {
    LOGINFO("Started logging");
    int exceptiongot = 0;
    Log::init(Log::Debug, "logfile");
    try {
      Log::init(Log::Debug, "logfile1");
    } catch (IllegalStateException& ex) {
      printf("Got Illegal state exception while calling init again\n");
      ex.showMessage();
      printf("Exception mesage = %s\n", ex.getMessage());
      exceptiongot = 1;
    }
    ASSERT(exceptiongot == 1, "expected exceptiongot to be 1");
    Log::close();
    unlink("logfile.log");
  }
END_TEST(REINIT)

BEGIN_TEST(ALL_LEVEL)
  {
    for (Log::LogLevel level = Log::Error; level <= Log::Debug;
         level = Log::LogLevel(level + 1)) {
      Log::init(level, "all_logfile");

      Log::error("Error Message");
      Log::warning("Warning Message");
      Log::info("Info Message");
      Log::config("Config Message");
      Log::fine("Fine Message");
      Log::finer("Finer Message");
      Log::finest("Finest Message");
      Log::debug("Debug Message");

      int lines = numOfLinesInFile("all_logfile.log");

      printf("lines = %d expected = %d \n", lines, expected(level));

      ASSERT(lines == expected(level), "Wrong number of lines");

      Log::close();
      unlink("all_logfile.log");
    }
  }
END_TEST(ALL_LEVEL)

BEGIN_TEST(ALL_LEVEL_MACRO)
  {
    for (Log::LogLevel level = Log::Error; level <= Log::Debug;
         level = Log::LogLevel(level + 1)) {
      Log::init(level, "all_logfile");

      LOGERROR("Error Message");
      LOGWARN("Warning Message");
      LOGINFO("Info Message");
      LOGCONFIG("Config Message");
      LOGFINE("Fine Message");
      LOGFINER("Finer Message");
      LOGFINEST("Finest Message");
      LOGDEBUG("Debug Message");

      int lines = numOfLinesInFile("all_logfile.log");

      printf("lines = %d Level = %d, %d\n", lines, static_cast<int>(level),
             Log::logLevel());

      ASSERT(lines == expected(level), "Wrong number of lines");
      Log::close();

      unlink("all_logfile.log");
    }
  }
END_TEST(ALL_LEVEL_MACRO)

BEGIN_TEST(FILE_LIMIT)
  {
#ifdef _WIN32
// Fail to roll file over to timestamp file on windows.
#else
    for (Log::LogLevel level = Log::Error; level <= Log::Debug;
         level = Log::LogLevel(level + 1)) {
      if (level == Log::Default) continue;
      Log::init(level, "logfile", 1);

      Log::error("Error Message");
      Log::warning("Warning Message");
      Log::info("Info Message");
      Log::config("Config Message");
      Log::fine("Fine Message");
      Log::finer("Finer Message");
      Log::finest("Finest Message");
      Log::debug("Debug Message");

      int lines = numOfLinesInFile("logfile.log");
      int expectedLines =
          level + LENGTH_OF_BANNER - (level >= Log::Default ? 1 : 0);
      printf("lines = %d expectedLines = %d level = %d\n", lines, expectedLines,
             level);

      ASSERT(lines == expectedLines, "Wrong number of lines");

      Log::close();
      unlink("logfile.log");
    }
#endif
  }
END_TEST(FILE_LIMIT)

BEGIN_TEST(CONFIG_ONWARDS)
  {
    Log::init(Log::Config, "logfile");

    Log::debug("Debug Message");
    Log::config("Config Message");
    Log::info("Info Message");
    Log::warning("Warning Message");
    Log::error("Error Message");

    int lines = numOfLinesInFile("logfile.log");
    printf("lines = %d\n", lines);
    // debug should not be printed
    ASSERT(lines == 4 + LENGTH_OF_BANNER,
           "Expected 4 + LENGTH_OF_BANNER lines.");
    Log::close();
    unlink("logfile.log");
  }
END_TEST(CONFIG_ONWARDS)

BEGIN_TEST(INFO_ONWARDS)
  {
    Log::init(Log::Info, "logfile");

    Log::debug("Debug Message");
    Log::config("Config Message");
    Log::info("Info Message");
    Log::warning("Warning Message");
    Log::error("Error Message");

    int lines = numOfLinesInFile("logfile.log");
    printf("lines = %d\n", lines);
    // debug, config should not be printed
    ASSERT(lines == 3 + LENGTH_OF_BANNER,
           "Expected 3 + LENGTH_OF_BANNER lines.");
    Log::close();
    unlink("logfile.log");
  }
END_TEST(INFO_ONWARDS)

BEGIN_TEST(WARNING_ONWARDS)
  {
    Log::init(Log::Warning, "logfile");

    Log::debug("Debug Message");
    Log::config("Config Message");
    Log::info("Info Message");
    Log::warning("Warning Message");
    Log::error("Error Message");

    int lines = numOfLinesInFile("logfile.log");
    printf("lines = %d\n", lines);
    // debug, config, info should not be printed
    ASSERT(lines == 2 + LENGTH_OF_BANNER,
           "Expected 2 + LENGTH_OF_BANNER lines.");
    Log::close();
    unlink("logfile.log");
  }
END_TEST(WARNING_ONWARDS)

BEGIN_TEST(ERROR_LEVEL)
  {
    Log::init(Log::Error, "logfile");

    Log::debug("Debug Message");
    Log::config("Config Message");
    Log::info("Info Message");
    Log::warning("Warning Message");
    Log::error("Error Message");

    int lines = numOfLinesInFile("logfile.log");
    printf("lines = %d\n", lines);
    // debug, config, info and warning should not be printed
    ASSERT(lines == 1 + LENGTH_OF_BANNER, "Expected 1+LENGTH_OF_BANNER lines.");
    Log::close();
    unlink("logfile.log");
  }
END_TEST(ERROR_LEVEL)

BEGIN_TEST(NO_LOG)
  {
    Log::init(Log::None, "logfile");

    Log::debug("Debug Message");
    Log::config("Config Message");
    Log::info("Info Message");
    Log::warning("Warning Message");
    Log::error("Error Message");

    int lines = numOfLinesInFile("logfile.log");
    printf("lines = %d\n", lines);
    // debug, config, info and warning and even error should not be printed
    // As the logfile is not there so -1 will be returned.
    ASSERT(lines == -1 || lines == 0, "Expected 0 or -1 lines.");
    Log::close();
    unlink("logfile.log");
  }
END_TEST(NO_LOG)

BEGIN_TEST(LOGFN)
  {
    for (Log::LogLevel level = Log::Error; level <= Log::Debug;
         level = Log::LogLevel(level + 1)) {
      Log::init(level, "logfile");

      testLogFnError();
      testLogFnWarning();
      testLogFnInfo();
      testLogFnConfig();
      testLogFnFine();
      testLogFnFiner();
      testLogFnFinest();
      testLogFnDebug();

      int lines = numOfLinesInFile("logfile.log");

      printf("lines = %d, level = %s\n", lines, Log::levelToChars(level));

      ASSERT(lines == 3 * expected(level) - 2 * LENGTH_OF_BANNER,
             "Wrong number of lines");
      Log::close();

      unlink("logfile.log");
    }
  }
END_TEST(LOGFN)
