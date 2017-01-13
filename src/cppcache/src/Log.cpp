/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <cctype>
#include <string>
#include <utility>
#include <vector>

#include <ace/ACE.h>
#include <ace/Guard_T.h>
#include <ace/Thread_Mutex.h>
#include <ace/OS.h>
#include <ace/OS_NS_time.h>
#include <ace/OS_NS_sys_time.h>
#include <ace/OS_NS_unistd.h>
#include <ace/OS_NS_Thread.h>
#include <ace/Dirent.h>
#include <ace/Dirent_Selector.h>
#include <ace/OS_NS_sys_stat.h>

#include <gfcpp/Log.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcppBanner.hpp>

#if defined(_WIN32)
#include <io.h>
#define GF_FILEEXISTS(x) _access_s(x, 00)
#else
#include <unistd.h>
#define GF_FILEEXISTS(x) access(x, F_OK)
#endif

/*****************************************************************************/

using namespace gemfire;

/**
 * The implementation of the Log class
 *
 *
 */

Log::LogLevel Log::s_logLevel = Default;

/*****************************************************************************/

namespace gemfire_log_globals {

std::string* g_logFile = NULL;
std::string* g_logFileWithExt = NULL;

size_t g_bytesWritten = 0;
bool g_isLogFileOpened = false;

size_t g_fileSizeLimit = GEMFIRE_MAX_LOG_FILE_LIMIT;
size_t g_diskSpaceLimit = GEMFIRE_MAX_LOG_DISK_LIMIT;

char g_logFileNameBuffer[2048] = {0};

ACE_Thread_Mutex* g_logMutex = NULL;

int g_rollIndex = 0;
size_t g_spaceUsed = 0;
// Make a pair for the filename & its size
std::pair<std::string, int64_t> g_fileInfoPair;
// Vector to hold the fileInformation
typedef std::vector<std::pair<std::string, int64_t> > g_fileInfo;

FILE* g_log = NULL;
ACE_utsname g_uname;
pid_t g_pid = 0;
}  // namespace gemfire_log_globals

extern "C" {

static int selector(const dirent* d) {
  std::string inputname(d->d_name);
  std::string filebasename =
      ACE::basename(gemfire_log_globals::g_logFileWithExt->c_str());
  size_t actualHyphenPos = filebasename.find_last_of('.');
  if (strcmp(filebasename.c_str(), d->d_name) == 0) return 1;
  size_t fileExtPos = inputname.find_last_of('.');
  std::string extName = inputname.substr(fileExtPos + 1, inputname.length());
  if (strcmp(extName.c_str(), "log") != 0) return 0;
  if (fileExtPos != std::string::npos) {
    std::string tempname = inputname.substr(0, fileExtPos);
    size_t fileHyphenPos = tempname.find_last_of('-');
    if (fileHyphenPos != std::string::npos) {
      std::string buff1 = tempname.substr(0, fileHyphenPos);
      if (ACE_OS::strstr(filebasename.c_str(), buff1.c_str()) == 0) {
        return 0;
      }
      if (fileHyphenPos != actualHyphenPos) return 0;
      std::string buff = tempname.substr(fileHyphenPos + 1,
                                         tempname.length() - fileHyphenPos - 1);
      for (std::string::iterator iter = buff.begin(); iter != buff.end();
           ++iter) {
        if (*iter < '0' || *iter > '9') {
          return 0;
        }
      }
      return 1;
    } else {
      return 0;
    }
  } else {
    return 0;
  }
}

static int comparator(const dirent** d1, const dirent** d2) {
  if (strlen((*d1)->d_name) < strlen((*d2)->d_name)) {
    return -1;
  } else if (strlen((*d1)->d_name) > strlen((*d2)->d_name)) {
    return 1;
  }

  int diff = ACE_OS::strcmp((*d1)->d_name, (*d2)->d_name);
  if (diff < 0) {
    return -1;
  } else if (diff > 0) {
    return 1;
  } else {
    return 0;
  }
}
}

using namespace gemfire_log_globals;

namespace gemfire {
// this method is not thread-safe and expected to be invoked once
// during initialization
void gf_log_libinit() {
  if (g_logMutex == NULL) {
    g_logFile = NULL;
    g_bytesWritten = 0;
    g_fileSizeLimit = 0;
    g_diskSpaceLimit = 0;
    g_rollIndex = 0;
    g_spaceUsed = 0;
    g_logMutex = new ACE_Thread_Mutex("Log::logMutex");
  }
  if (g_logMutex == NULL) {
    throw IllegalStateException("Log not initialized successfully");
  }
}
}  // namespace gemfire

/*****************************************************************************/

const char* Log::logFileName() {
  ACE_Guard<ACE_Thread_Mutex> guard(*g_logMutex);

  if (!g_logFile) {
    g_logFileNameBuffer[0] = '\0';
  } else {
    if (g_logFile->size() >= sizeof g_logFileNameBuffer) {
      throw IllegalStateException(
          ("Log file name is too long: " + *g_logFile).c_str());
    }
    ACE_OS::strncpy(g_logFileNameBuffer, g_logFile->c_str(),
                    sizeof(g_logFileNameBuffer));
  }

  return g_logFileNameBuffer;
}

void Log::init(LogLevel level, const char* logFileName, int32 logFileLimit,
               int64 logDiskSpaceLimit) {
  if (g_log != NULL) {
    throw IllegalStateException(
        "The Log has already been initialized. "
        "Call Log::close() before calling Log::init again.");
  }
  s_logLevel = level;
  if (g_logMutex == NULL) g_logMutex = new ACE_Thread_Mutex("Log::logMutex");

  if (logDiskSpaceLimit <
      0 /*|| logDiskSpaceLimit > GEMFIRE_MAX_LOG_DISK_LIMIT*/) {
    logDiskSpaceLimit = GEMFIRE_MAX_LOG_DISK_LIMIT;
  }

  if (logFileLimit < 0 || logFileLimit > GEMFIRE_MAX_LOG_FILE_LIMIT) {
    logFileLimit = GEMFIRE_MAX_LOG_FILE_LIMIT;
  }

  ACE_Guard<ACE_Thread_Mutex> guard(*g_logMutex);

  if (logFileName && logFileName[0]) {
    std::string filename = logFileName;
    if (filename.size() >= sizeof g_logFileNameBuffer) {
      throw IllegalStateException(
          ("Log file name is too long: " + filename).c_str());
    }
    if (g_logFile) {
      *g_logFile = filename;
    } else {
      g_logFile = new std::string(filename);
    }

#ifdef _WIN32
    // replace all '\' with '/' to make everything easier..
    size_t length = g_logFile->length() + 1;
    char* slashtmp = new char[length];
    ACE_OS::strncpy(slashtmp, g_logFile->c_str(), length);
    for (size_t i = 0; i < g_logFile->length(); i++) {
      if (slashtmp[i] == '/') {
        slashtmp[i] = '\\';
      }
    }
    *g_logFile = slashtmp;
    delete[] slashtmp;
    slashtmp = NULL;
#endif

    // Appending a ".log" at the end if it does not exist or file has some other
    // extension.
    std::string filebasename = ACE::basename(g_logFile->c_str());
    int32 len = static_cast<int32>(filebasename.length());
    size_t fileExtPos = filebasename.find_last_of('.', len);
    // if no extension then add .log extension
    if (fileExtPos == std::string::npos) {
      g_logFileWithExt = new std::string(*g_logFile + ".log");
    } else {
      std::string extName = filebasename.substr(fileExtPos + 1);
      // if extension other than .log change it to ext + .log
      if (extName != "log") {
        g_logFileWithExt = new std::string(*g_logFile + ".log");
      }
      // .log Extension already provided, no need to append any extension.
      else {
        g_logFileWithExt = new std::string(*g_logFile);
      }
    }

    g_fileSizeLimit = logFileLimit * 1024 * 1024;
    g_diskSpaceLimit = logDiskSpaceLimit * 1024ll * 1024ll;

    // If FileSizelimit is greater than DiskSpaceLimit & diskspaceLimit is set,
    // then set DiskSpaceLimit to FileSizelimit
    if (g_fileSizeLimit > g_diskSpaceLimit && g_diskSpaceLimit != 0) {
      g_fileSizeLimit = g_diskSpaceLimit;
    }

    // If only DiskSpaceLimit is specified and no FileSizeLimit specified, then
    // set DiskSpaceLimit to FileSizelimit.
    // This helps in getting the file handle that is exceeded the limit.
    if (g_fileSizeLimit == 0 && g_diskSpaceLimit != 0) {
      g_fileSizeLimit = g_diskSpaceLimit;
    }

    g_bytesWritten = 0;
    g_spaceUsed = 0;
    g_rollIndex = 0;

    std::string dirname = ACE::dirname(g_logFile->c_str());
    /*struct dirent **resultArray;
    //int entries_count = ACE_OS::scandir(dirname.c_str(), &resultArray,
    selector, comparator);

    for(int i = 0; i < entries_count; i++) {
      std::string strname = ACE::basename(resultArray[i]->d_name);
      size_t fileExtPos = strname.find_last_of('.', strname.length());
      if ( fileExtPos != std::string::npos ) {
        std::string tempname = strname.substr(0,fileExtPos);
        size_t fileHyphenPos = tempname.find_last_of('-', tempname.length());
        if ( fileHyphenPos != std::string::npos ) {
          std::string buff = tempname.substr(fileHyphenPos+1,tempname.length());
          g_rollIndex = ACE_OS::atoi(buff.c_str())+ 1;
        }
      }
    }
    for(int i = 0; i < entries_count; i++) {
      ACE_OS::free ( resultArray[i] );
    }

    if (entries_count >= 0) {
      ACE_OS::free( resultArray );
      resultArray = NULL;
    }*/

    ACE_Dirent_Selector sds;
    int status = sds.open(dirname.c_str(), selector, comparator);
    if (status != -1) {
      for (int index = 0; index < sds.length(); ++index) {
        std::string strname = ACE::basename(sds[index]->d_name);
        size_t fileExtPos = strname.find_last_of('.', strname.length());
        if (fileExtPos != std::string::npos) {
          std::string tempname = strname.substr(0, fileExtPos);
          size_t fileHyphenPos = tempname.find_last_of('-', tempname.length());
          if (fileHyphenPos != std::string::npos) {
            std::string buff =
                tempname.substr(fileHyphenPos + 1, tempname.length());
            g_rollIndex = ACE_OS::atoi(buff.c_str()) + 1;
          }
        }  // if loop
      }    // for loop
    }
    sds.close();

    /*ACE_Dirent logDirectory;
    logDirectory.open (ACE_TEXT (g_logFile->c_str()));
    for (ACE_DIRENT *directory; (directory = logDirectory.read ()) != 0;) {
            // Skip the ".." and "." files.
            if (ACE::isdotdir(directory->d_name) != true) {
                    ACE_stat stat_buf;
                    ACE_OS::lstat (directory->d_name, &stat_buf);
                    switch (stat_buf.st_mode & S_IFMT) {
                            case S_IFREG:
                                    stat_buf.
                                    break;
                            default:
                                    break;
                    }
            }
    }*/

    FILE* existingFile = fopen(g_logFileWithExt->c_str(), "r");
    if (existingFile != NULL && logFileLimit > 0) {
      /* adongre
       * Coverity - II
       * CID 29205: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
       * "sprintf" can cause a
       * buffer overflow when done incorrectly. Because sprintf() assumes an
       * arbitrarily long string,
       * callers must be careful not to overflow the actual space of the
       * destination.
       * Use snprintf() instead, or correct precision specifiers.
       * Fix : using ACE_OS::snprintf
       */
      char rollFile[1024] = {0};
      std::string logsdirname;
      std::string logsbasename;
      std::string fnameBeforeExt;
      std::string extName;
      std::string newfilestr;

      int32 len = static_cast<int32>(g_logFileWithExt->length());
      int32 lastPosOfSep = static_cast<int32>(
          g_logFileWithExt->find_last_of(ACE_DIRECTORY_SEPARATOR_CHAR, len));
      if (lastPosOfSep == -1) {
        logsdirname = ".";
      } else {
        logsdirname = g_logFileWithExt->substr(0, lastPosOfSep);
      }
      logsbasename = g_logFileWithExt->substr(lastPosOfSep + 1, len);
      char logFileExtAfter = '.';
      int32 baselen = static_cast<int32>(logsbasename.length());
      int32 posOfExt = static_cast<int32>(
          logsbasename.find_last_of(logFileExtAfter, baselen));
      if (posOfExt == -1) {
        // throw IllegalArgument;
      } else {
        fnameBeforeExt = logsbasename.substr(0, posOfExt);
        extName = logsbasename.substr(posOfExt + 1, baselen);
      }
      ACE_OS::snprintf(rollFile, 1024, "%s%c%s-%d.%s", logsdirname.c_str(),
                       ACE_DIRECTORY_SEPARATOR_CHAR, fnameBeforeExt.c_str(),
                       g_rollIndex++, extName.c_str());
      bool rollFileNameGot = false;
      while (!rollFileNameGot) {
        FILE* checkFile = fopen(rollFile, "r");
        if (checkFile != NULL) {
          fclose(checkFile);
          checkFile = NULL;
          ACE_OS::snprintf(rollFile, 1024, "%s%c%s-%d.%s", logsdirname.c_str(),
                           ACE_DIRECTORY_SEPARATOR_CHAR, fnameBeforeExt.c_str(),
                           g_rollIndex++, extName.c_str());
        } else {
          rollFileNameGot = true;
        }
        /* adongre
         * CID 28999: Use after free (USE_AFTER_FREE)
         */
        if (checkFile != NULL) fclose(existingFile);
      }
      // retry some number of times before giving up when file is busy etc.
      int renameResult = -1;
      int maxTries = 10;
      while (maxTries-- > 0) {
        renameResult = ACE_OS::rename(g_logFileWithExt->c_str(), rollFile);
        if (renameResult >= 0) {
          break;
        }
        // continue after some sleep
        gemfire::millisleep(200);
      }
      /* (don't throw exception; try appending to existing file instead)
      if (renameResult < 0) {
        std::string msg = "Could not rename: " +
          *g_logFileWithExt + " to: " + rollFile;
        throw GemfireIOException(msg.c_str());
      }
      */
    }
    if (existingFile != NULL) {
      fclose(existingFile);
      existingFile = NULL;
    }
  } else if (g_logFile) {
    delete g_logFile;
    g_logFile = NULL;
    g_logFileWithExt = NULL;
  }
  writeBanner();
}

void Log::close() {
  ACE_Guard<ACE_Thread_Mutex> guard(*g_logMutex);

  std::string oldfile;

  if (g_logFile) {
    oldfile = *g_logFile;
    delete g_logFile;
    g_logFile = NULL;
  }
  if (g_logFileWithExt) {
    delete g_logFileWithExt;
    g_logFileWithExt = NULL;
  }

  if (g_log) {
    fclose(g_log);
    g_log = NULL;
  }
}

void Log::writeBanner() {
  if (g_logFileWithExt == NULL) {
    return;
  }
  const char* dirname = ACE::dirname(g_logFileWithExt->c_str());
  if (GF_FILEEXISTS(dirname) != 0 && ACE_OS::mkdir(dirname) != 0) {
    std::string msg =
        "Error in creating directories for: " + std::string(dirname);
    throw GemfireIOException(msg.c_str());
  }
  // retry some number of times before giving up when file is busy etc.
  int maxTries = 10;
  while (maxTries-- > 0) {
    g_log = fopen(g_logFileWithExt->c_str(), "a");
    if (g_log != NULL) {
      break;
    }
    int lastError = ACE_OS::last_error();
    if (lastError != EACCES && lastError != EINTR && lastError != EWOULDBLOCK) {
      break;
    }
    // continue after some sleep
    gemfire::millisleep(200);
  }
  if (!g_log) {
    // g_log = stdout;
    // fprintf(stdout,"File %s unable to open, logging into console.",
    // g_logFile->c_str());
    // fflush(stdout);
    // g_logFile = NULL;
    // g_logFileWithExt = NULL;
    g_isLogFileOpened = false;
    return;
    // std::string msg = "Error in opening log file: " + *g_logFile;
    // throw GemfireIOException(msg.c_str());
  } else {
    g_isLogFileOpened = true;
  }

  if (s_logLevel == Log::None) {
    return;
  }
  std::string bannertext = gfcppBanner::getBanner();

  if (g_logFile == NULL) {
    fprintf(stdout, "%s", bannertext.c_str());
    fflush(stdout);
    return;
  }  // else
  GF_D_ASSERT(g_logFile && g_logMutex && g_logFileWithExt);

  if (fprintf(g_log, "%s", bannertext.c_str()) == 0 || ferror(g_log)) {
    // we should be continue,
    // fclose( g_log );
    // std::string msg = "Error in writing banner to log file: " + *g_logFile;
    // throw GemfireIOException(msg.c_str());
    return;
  }

  int numchars = 0;
  const char* pch = NULL;
  pch = strchr(bannertext.c_str(), '\n');
  while (pch != NULL) {
    pch = strchr(pch + 1, '\n');
    numchars += 2;
  }

  g_bytesWritten += static_cast<int32_t>(bannertext.length() + numchars);
  fflush(g_log);
}

const char* Log::levelToChars(Log::LogLevel level) {
  switch (level) {
    case Log::None:
      return "none";

    case Log::Error:
      return "error";

    case Log::Warning:
      return "warning";

    case Log::Info:
      return "info";

    case Log::Default:
      return "default";

    case Log::Config:
      return "config";

    case Log::Fine:
      return "fine";

    case Log::Finer:
      return "finer";

    case Log::Finest:
      return "finest";

    case Log::Debug:
      return "debug";

    case Log::All:
      return "all";

    default: {
      char buf[64] = {0};
      ACE_OS::snprintf(buf, 64, "Unexpected log level: %d", level);
      throw IllegalArgumentException(buf);
    }
  }
}

Log::LogLevel Log::charsToLevel(const char* chars) {
  std::string level = chars;

  if (level.empty()) return Log::None;

  for (uint32_t i = 0; i < level.size(); i++) {
    if (isupper(level[i])) level[i] = tolower(level[i]);
  }

  if (level == "none") {
    return Log::None;
  } else if (level == "error") {
    return Log::Error;
  } else if (level == "warning") {
    return Log::Warning;
  } else if (level == "info") {
    return Log::Info;
  } else if (level == "default") {
    return Log::Default;
  } else if (level == "config") {
    return Log::Config;
  } else if (level == "fine") {
    return Log::Fine;
  } else if (level == "finer") {
    return Log::Finer;
  } else if (level == "finest") {
    return Log::Finest;
  } else if (level == "debug") {
    return Log::Debug;
  } else if (level == "all") {
    return Log::All;
  } else {
    throw IllegalArgumentException(("Unexpected log level: " + level).c_str());
  }
}

char* Log::formatLogLine(char* buf, Log::LogLevel level) {
  if (g_pid == 0) {
    g_pid = ACE_OS::getpid();
    ACE_OS::uname(&g_uname);
  }
  const size_t MINBUFSIZE = 128;
  ACE_Time_Value clock = ACE_OS::gettimeofday();
  time_t secs = clock.sec();
  struct tm* tm_val = ACE_OS::localtime(&secs);
  char* pbuf = buf;
  pbuf += ACE_OS::snprintf(pbuf, 15, "[%s ", Log::levelToChars(level));
  pbuf += ACE_OS::strftime(pbuf, MINBUFSIZE, "%Y/%m/%d %H:%M:%S", tm_val);
  pbuf +=
      ACE_OS::snprintf(pbuf, 15, ".%06ld ", static_cast<long>(clock.usec()));
  pbuf += ACE_OS::strftime(pbuf, MINBUFSIZE, "%Z ", tm_val);

  ACE_OS::snprintf(pbuf, 300, "%s:%d %lu] ", g_uname.nodename, g_pid,
                   (unsigned long)ACE_OS::thr_self());

  return buf;
}

// int g_count = 0;
void Log::put(LogLevel level, const char* msg) {
  ACE_Guard<ACE_Thread_Mutex> guard(*g_logMutex);

  g_fileInfo fileInfo;

  char buf[256] = {0};
  char fullpath[512] = {0};

  if (!g_logFile) {
    fprintf(stdout, "%s%s\n", formatLogLine(buf, level), msg);
    fflush(stdout);
    // TODO: ignoring for now; probably store the log-lines for possible
    // future logging if log-file gets initialized properly

  } else {
    if (!g_isLogFileOpened) {
      g_log = fopen(g_logFileWithExt->c_str(), "a");
      if (!g_log) {
        // fprintf(stdout,"%s%s\n", formatLogLine(buf, level), msg);
        // fflush(stdout);
        g_isLogFileOpened = false;
        return;
      }
      g_isLogFileOpened = true;
    } else if (!g_log) {
      g_log = fopen(g_logFileWithExt->c_str(), "a");
      if (!g_log) {
        return;
      }
    }

    formatLogLine(buf, level);
    size_t numChars =
        static_cast<int>(ACE_OS::strlen(buf) + ACE_OS::strlen(msg));
    g_bytesWritten +=
        numChars + 2;  // bcoz we have to count trailing new line (\n)

    if ((g_fileSizeLimit != 0) && (g_bytesWritten >= g_fileSizeLimit)) {
      char rollFile[1024] = {0};
      std::string logsdirname;
      std::string logsbasename;
      std::string fnameBeforeExt;
      std::string extName;
      std::string newfilestr;

      int32 len = static_cast<int32>(g_logFileWithExt->length());
      int32 lastPosOfSep = static_cast<int32>(
          g_logFileWithExt->find_last_of(ACE_DIRECTORY_SEPARATOR_CHAR, len));
      if (lastPosOfSep == -1) {
        logsdirname = ".";
      } else {
        logsdirname = g_logFileWithExt->substr(0, lastPosOfSep);
      }
      logsbasename = g_logFileWithExt->substr(lastPosOfSep + 1, len);
      char logFileExtAfter = '.';
      int32 baselen = static_cast<int32>(logsbasename.length());
      int32 posOfExt = static_cast<int32>(
          logsbasename.find_last_of(logFileExtAfter, baselen));
      if (posOfExt == -1) {
        // throw IllegalArgument;
      } else {
        fnameBeforeExt = logsbasename.substr(0, posOfExt);
        extName = logsbasename.substr(posOfExt + 1, baselen);
      }
      ACE_OS::snprintf(rollFile, 1024, "%s%c%s-%d.%s", logsdirname.c_str(),
                       ACE_DIRECTORY_SEPARATOR_CHAR, fnameBeforeExt.c_str(),
                       g_rollIndex++, extName.c_str());
      bool rollFileNameGot = false;
      while (!rollFileNameGot) {
        FILE* fp1 = fopen(rollFile, "r");
        if (fp1 != NULL) {
          fclose(fp1);
          ACE_OS::snprintf(rollFile, 1024, "%s%c%s-%d.%s", logsdirname.c_str(),
                           ACE_DIRECTORY_SEPARATOR_CHAR, fnameBeforeExt.c_str(),
                           g_rollIndex++, extName.c_str());
        } else {
          rollFileNameGot = true;
        }
      }

      fclose(g_log);
      g_log = NULL;

      if (ACE_OS::rename(g_logFileWithExt->c_str(), rollFile) < 0) {
        /* printf
               ("thid = %lu, g_bytesWritten = %d\n",
                ACE_OS::thr_self(), g_bytesWritten);*/
        // std::string msg =
        // "Could not rename: " + *g_logFileWithExt + " to: " + rollFile;
        // throw GemfireIOException(msg.c_str());
        return;  // no need to throw exception try next time
      }

      g_bytesWritten =
          numChars + 2;  // bcoz we have to count trailing new line (\n)
      writeBanner();
    }

    g_spaceUsed += g_bytesWritten;

    if ((g_diskSpaceLimit > 0) && (g_spaceUsed >= g_diskSpaceLimit)) {
      std::string dirname = ACE::dirname(g_logFile->c_str());
      // struct dirent **resultArray;
      // int entries_count = ACE_OS::scandir(dirname.c_str(), &resultArray,
      // selector, comparator);
      // int64 spaceUsed = 0;
      g_spaceUsed = 0;
      ACE_stat statBuf = {0};
      /*for(int i = 1; i < entries_count; i++) {
        ACE_OS::snprintf(fullpath , 512 ,
    "%s%c%s",dirname.c_str(),ACE_DIRECTORY_SEPARATOR_CHAR,resultArray[i]->d_name);
        ACE_OS::stat(fullpath,&statBuf);
        g_fileInfoPair = std::make_pair(fullpath,statBuf.st_size);
        fileInfo.push_back(g_fileInfoPair);
        g_spaceUsed += fileInfo[i-1].second;
      }
    g_spaceUsed += g_bytesWritten;
        for(int i = 0; i < entries_count; i++) {
        ACE_OS::free ( resultArray[i] );
        }
    if (entries_count >= 0) {
        ACE_OS::free( resultArray );
      resultArray = NULL;
    }*/

      ACE_Dirent_Selector sds;
      int status = sds.open(dirname.c_str(), selector, comparator);
      if (status != -1) {
        for (int index = 1; index < sds.length(); ++index) {
          ACE_OS::snprintf(fullpath, 512, "%s%c%s", dirname.c_str(),
                           ACE_DIRECTORY_SEPARATOR_CHAR, sds[index]->d_name);
          ACE_OS::stat(fullpath, &statBuf);
          g_fileInfoPair = std::make_pair(fullpath, statBuf.st_size);
          fileInfo.push_back(g_fileInfoPair);
          g_spaceUsed += fileInfo[index - 1].second;
        }  // for loop
        g_spaceUsed += g_bytesWritten;
        sds.close();
      }
      int fileIndex = 0;

      while ((g_spaceUsed > (g_diskSpaceLimit /*- g_fileSizeLimit*/))) {
        int64 fileSize = fileInfo[fileIndex].second;
        if (ACE_OS::unlink(fileInfo[fileIndex].first.c_str()) == 0) {
          g_spaceUsed -= fileSize;
        } else {
          char printmsg[256];
          ACE_OS::snprintf(printmsg, 256, "%s\t%s\n", "Could not delete",
                           fileInfo[fileIndex].first.c_str());
          int numChars =
              fprintf(g_log, "%s%s\n", formatLogLine(buf, level), printmsg);
          g_bytesWritten +=
              numChars + 2;  // bcoz we have to count trailing new line (\n)
        }
        fileIndex++;
      }
    }

    if ((numChars = fprintf(g_log, "%s%s\n", buf, msg)) == 0 || ferror(g_log)) {
      if ((g_diskSpaceLimit > 0)) {
        g_spaceUsed = g_spaceUsed - (numChars + 2);
      }
      if (g_fileSizeLimit > 0) {
        g_bytesWritten = g_bytesWritten - (numChars + 2);
      }

      // lets continue wothout throwing the exception; it should not cause
      // process to terminate
      fclose(g_log);
      g_log = NULL;
      // g_isLogFileOpened = false;
      // std::string msg = "Error in writing to log file: " + *g_logFile;
      // throw GemfireIOException(msg.c_str());
    } else {
      fflush(g_log);
    }
  }
}

void Log::putThrow(LogLevel level, const char* msg, const Exception& ex) {
  char buf[128] = {0};
  ACE_OS::snprintf(buf, 128, "GemFire exception %s thrown: ", ex.getName());
  put(level, (std::string(buf) + ex.getMessage() + "\n" + msg).c_str());
}

void Log::putCatch(LogLevel level, const char* msg, const Exception& ex) {
  char buf[128] = {0};
  ACE_OS::snprintf(buf, 128, "GemFire exception %s caught: ", ex.getName());
  put(level, (std::string(buf) + ex.getMessage() + "\n" + msg).c_str());
}

void Log::enterFn(LogLevel level, const char* functionName) {
  enum { MAX_NAME_LENGTH = 1024 };
  std::string fn = functionName;
  if (fn.size() > MAX_NAME_LENGTH) {
    fn = fn.substr(fn.size() - MAX_NAME_LENGTH, MAX_NAME_LENGTH);
  }
  char buf[MAX_NAME_LENGTH + 512] = {0};
  ACE_OS::snprintf(buf, 1536, "{{{===>>> Entering function %s", fn.c_str());
  put(level, buf);
}

void Log::exitFn(LogLevel level, const char* functionName) {
  enum { MAX_NAME_LENGTH = 1024 };
  std::string fn = functionName;
  if (fn.size() > MAX_NAME_LENGTH) {
    fn = fn.substr(fn.size() - MAX_NAME_LENGTH, MAX_NAME_LENGTH);
  }
  char buf[MAX_NAME_LENGTH + 512] = {0};
  ACE_OS::snprintf(buf, 1536, "<<<===}}} Exiting function %s", fn.c_str());
  put(level, buf);
}

// var arg logging routines.

#ifdef _WIN32
#define vsnprintf _vsnprintf
#endif

void LogVarargs::debug(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Debug, msg);
  va_end(argp);
}

void LogVarargs::error(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Error, msg);
  va_end(argp);
}

void LogVarargs::warn(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Warning, msg);
  va_end(argp);
}

void LogVarargs::info(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Info, msg);
  va_end(argp);
}

void LogVarargs::config(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Config, msg);
  va_end(argp);
}

void LogVarargs::fine(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Fine, msg);
  va_end(argp);
}

void LogVarargs::finer(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Finer, msg);
  va_end(argp);
}

void LogVarargs::finest(const char* fmt, ...) {
  char msg[_GF_MSG_LIMIT] = {0};
  va_list argp;
  va_start(argp, fmt);
  vsnprintf(msg, _GF_MSG_LIMIT, fmt, argp);
  /* win doesn't guarantee termination */ msg[_GF_MSG_LIMIT - 1] = '\0';
  Log::put(Log::Finest, msg);
  va_end(argp);
}
