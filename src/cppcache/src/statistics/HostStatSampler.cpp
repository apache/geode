/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/ACE.h>
#include <ace/Thread_Mutex.h>
#include <ace/Task.h>
#include <ace/OS_NS_sys_utsname.h>
#include <ace/INET_Addr.h>
#include <ace/Dirent.h>
#include <ace/Dirent_Selector.h>
#include <ace/OS_NS_sys_stat.h>
#include <utility>
#include <vector>

#include "HostStatSampler.hpp"
#include "HostStatHelper.hpp"
#include "StatArchiveWriter.hpp"
#include <NanoTimer.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/Log.hpp>
#include "GemfireStatisticsFactory.hpp"
#include <ClientHealthStats.hpp>
#include <ClientProxyMembershipID.hpp>
using namespace gemfire_statistics;
using namespace gemfire;

namespace gemfire_stats_globals {
std::string g_statFile;
std::string g_statFileWithExt;
int64 g_spaceUsed = 0;
int64 g_previoussamplesize = 0;
int64 g_previoussamplesizeLastFile = 0;
// Make a pair for the filename & its size
std::pair<std::string, size_t> g_fileInfoPair;
// Vector to hold the fileInformation
typedef std::vector<std::pair<std::string, int64> > g_fileInfo;
}  // namespace gemfire_stats_globals

using namespace gemfire_stats_globals;

extern "C" {

int selector(const dirent* d) {
  std::string inputname(d->d_name);
  std::string filebasename =
      ACE::basename(gemfire_stats_globals::g_statFileWithExt.c_str());
  size_t actualHyphenPos = filebasename.find_last_of('.');
  if (strcmp(filebasename.c_str(), d->d_name) == 0) return 1;
  size_t fileExtPos = inputname.find_last_of('.');
  std::string extName = inputname.substr(fileExtPos + 1, inputname.length());
  if (strcmp(extName.c_str(), "gfs") != 0) return 0;
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

int comparator(const dirent** d1, const dirent** d2) {
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

const char* HostStatSampler::NC_HSS_Thread = "NC HSS Thread";

HostStatSampler::HostStatSampler(const char* filePath, int64 sampleIntervalMs,
                                 StatisticsManager* statMngr,
                                 int64 statFileLimit,
                                 int64 statDiskSpaceLimit) {
  m_isStatDiskSpaceEnabled = false;
  m_adminError = false;
  m_running = false;
  m_stopRequested = false;
  m_archiver = NULL;
  m_samplerStats = new StatSamplerStats();

  ACE_Time_Value startTimeValue = ACE_OS::gettimeofday();
  m_startTime = startTimeValue.msec();

  m_pid = ACE_OS::getpid();
  m_statMngr = statMngr;
  m_archiveFileName = filePath;
  g_statFile = filePath;
  m_sampleRate = sampleIntervalMs;
  rollIndex = 0;
  m_archiveDiskSpaceLimit = statDiskSpaceLimit;
  g_spaceUsed = 0;

  if (statDiskSpaceLimit != 0) {
    m_isStatDiskSpaceEnabled = true;
  }

  m_archiveFileSizeLimit = statFileLimit * 1024 * 1024;  // 10000000;
  if (m_archiveFileSizeLimit < 0 ||
      m_archiveFileSizeLimit > GEMFIRE_MAX_STATS_FILE_LIMIT) {
    m_archiveFileSizeLimit = GEMFIRE_MAX_STATS_FILE_LIMIT;
  }

  if (m_isStatDiskSpaceEnabled) {
    m_archiveDiskSpaceLimit = statDiskSpaceLimit * 1024 * 1024;  // 10000000;

    if (m_archiveDiskSpaceLimit <
        0 /*|| m_archiveDiskSpaceLimit > GEMFIRE_MAX_STAT_DISK_LIMIT*/) {
      m_archiveDiskSpaceLimit = GEMFIRE_MAX_STAT_DISK_LIMIT;
    }

    // If FileSizelimit is greater than DiskSpaceLimit & diskspaceLimit is set,
    // then set DiskSpaceLimit to FileSizelimit
    if (m_archiveFileSizeLimit > m_archiveDiskSpaceLimit) {
      m_archiveFileSizeLimit = m_archiveDiskSpaceLimit;
    }

    // If only DiskSpaceLimit is specified and no FileSizeLimit specified, then
    // set DiskSpaceLimit to FileSizelimit.
    // This helps in getting the file handle that is exceeded the limit.
    if (m_archiveFileSizeLimit == 0) {
      m_archiveFileSizeLimit = m_archiveDiskSpaceLimit;
    }

    g_statFileWithExt = initStatFileWithExt();

#ifdef _WIN32
    // replace all '\' with '/' to make everything easier..
    size_t len = g_statFile.length() + 1;
    char* slashtmp = new char[len];
    ACE_OS::strncpy(slashtmp, g_statFile.c_str(), len);
    for (size_t i = 0; i < g_statFile.length(); i++) {
      if (slashtmp[i] == '/') {
        slashtmp[i] = '\\';
      }
    }
    g_statFile = slashtmp;
    delete[] slashtmp;
    slashtmp = NULL;
#endif

    std::string dirname = ACE::dirname(g_statFile.c_str());
    // struct dirent **resultArray;
    // int entries_count = ACE_OS::scandir(dirname.c_str(), &resultArray,
    // selector, comparator);
    ACE_Dirent_Selector sds;
    int status = sds.open(dirname.c_str(), selector, comparator);
    if (status != -1) {
      for (int i = 0; i < sds.length(); i++) {
        // std::string strname = ACE::basename(resultArray[i]->d_name);
        std::string strname = ACE::basename(sds[i]->d_name);
        size_t fileExtPos = strname.find_last_of('.', strname.length());
        if (fileExtPos != std::string::npos) {
          std::string tempname = strname.substr(0, fileExtPos);
          size_t fileHyphenPos = tempname.find_last_of('-', tempname.length());
          if (fileHyphenPos != std::string::npos) {
            std::string buff =
                tempname.substr(fileHyphenPos + 1, tempname.length());
            rollIndex = ACE_OS::atoi(buff.c_str()) + 1;
          }
        }
      }
      sds.close();
    }
    /*for(int i = 0; i < entries_count; i++) {
      ACE_OS::free ( resultArray[i] );
    }
    if (entries_count >= 0) {
      ACE_OS::free( resultArray );
      resultArray = NULL;
    }*/

    FILE* existingFile = fopen(g_statFileWithExt.c_str(), "r");
    if (existingFile != NULL && statFileLimit > 0) {
      fclose(existingFile);
      /* adongre
       * CID 28820: Resource leak (RESOURCE_LEAK)
       */
      existingFile = NULL;
      changeArchive(g_statFileWithExt);
    } else {
      writeGfs();
    }
    /* adongre
     * CID 28820: Resource leak (RESOURCE_LEAK)
     */
    if (existingFile != NULL) {
      fclose(existingFile);
      existingFile = NULL;
    }
  }
}

std::string HostStatSampler::initStatFileWithExt() {
  std::string archivefilename = createArchiveFileName();
  archivefilename = chkForGFSExt(archivefilename);
  return archivefilename;
}

HostStatSampler::~HostStatSampler() {
  if (m_samplerStats != NULL) {
    delete m_samplerStats;
    m_samplerStats = NULL;
  }
  if (m_archiver != NULL) {
    delete m_archiver;
    m_archiver = NULL;
  }
}

std::string HostStatSampler::createArchiveFileName() {
  if (!m_isStatDiskSpaceEnabled) {
    char buff[1024] = {0};
    int32 pid = ACE_OS::getpid();
    int32 len = static_cast<int32>(m_archiveFileName.length());
    size_t fileExtPos = m_archiveFileName.find_last_of('.', len);
    if (fileExtPos == std::string::npos) {
      ACE_OS::snprintf(buff, 1024, "%s-%d.gfs", m_archiveFileName.c_str(), pid);
    } else {
      std::string tmp;
      tmp = m_archiveFileName.substr(0, fileExtPos);
      ACE_OS::snprintf(buff, 1024, "%s-%d.gfs", tmp.c_str(), pid);
    }
    m_archiveFileName = buff;
    return m_archiveFileName;
  } else {
    return m_archiveFileName;
  }
}

std::string HostStatSampler::getArchiveFileName() { return m_archiveFileName; }

int64 HostStatSampler::getArchiveFileSizeLimit() {
  return m_archiveFileSizeLimit;
}

int64 HostStatSampler::getArchiveDiskSpaceLimit() {
  return m_archiveDiskSpaceLimit;
}

int64 HostStatSampler::getSampleRate() { return m_sampleRate; }

bool HostStatSampler::isSamplingEnabled() { return true; }

void HostStatSampler::accountForTimeSpentWorking(int64 nanosSpentWorking) {
  m_samplerStats->tookSample(nanosSpentWorking);
}

bool HostStatSampler::statisticsExists(const int64 id) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statMngr->getListMutex());
  std::vector<Statistics*>& statsList = m_statMngr->getStatsList();
  std::vector<Statistics*>::iterator i;
  for (i = statsList.begin(); i != statsList.end(); ++i) {
    if ((*i)->getUniqueId() == id) {
      return true;
    }
  }
  return false;
}

Statistics* HostStatSampler::findStatistics(const int64 id) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statMngr->getListMutex());
  std::vector<Statistics*>& statsList = m_statMngr->getStatsList();
  std::vector<Statistics*>::iterator i;
  for (i = statsList.begin(); i != statsList.end(); ++i) {
    if ((*i)->getUniqueId() == id) {
      return *i;
    }
  }
  return NULL;
}

ACE_Recursive_Thread_Mutex& HostStatSampler::getStatListMutex() {
  return m_statMngr->getListMutex();
}

int32 HostStatSampler::getStatisticsModCount() {
  return m_statMngr->getStatListModCount();
}

std::vector<Statistics*>& HostStatSampler::getStatistics() {
  return m_statMngr->getStatsList();
}

std::vector<Statistics*>& HostStatSampler::getNewStatistics() {
  return m_statMngr->getNewlyAddedStatsList();
}

int64 HostStatSampler::getSystemId() { return m_pid; }

int64 HostStatSampler::getSystemStartTime() { return m_startTime; }

std::string HostStatSampler::getSystemDirectoryPath() {
  ACE_utsname u;
  ACE_OS::uname(&u);
  std::string dirPath(u.nodename);
  return dirPath;
}

std::string HostStatSampler::getProductDescription() {
  return "Pivotal Gemfire Native Client ";
}

void HostStatSampler::changeArchive(std::string filename) {
  if (filename.length() == 0) {
    // terminate the sampling thread
    m_stopRequested = true;
    return;
  }
  filename = chkForGFSExt(filename);
  if (m_archiver != NULL) {
    g_previoussamplesize = m_archiver->getSampleSize();
    m_archiver->closeFile();
  }
  // create new file only when tis file has some data; otherwise reuse it
  if (rollArchive(filename) != 0) {
    delete m_archiver;
    m_archiver = NULL;
    // throw exception
    return;
  } else {
    /*
    if (m_archiver != NULL) {
      m_archiver->openFile(filename);
      m_archiver->close();
    }
    */
    delete m_archiver;
    m_archiver = NULL;
  }

  m_archiver = new StatArchiveWriter(filename, this);
}

std::string HostStatSampler::chkForGFSExt(std::string filename) {
  if (!m_isStatDiskSpaceEnabled) {
    int32 len = static_cast<int32>(filename.length());
    size_t posOfExt = filename.find_last_of('.', len);
    if (posOfExt == std::string::npos) {
      std::string newFilename = filename + "." + "gfs";
      return newFilename;
    }
    std::string extName = filename.substr(posOfExt + 1);
    if (extName != "gfs") {
      std::string newFilename = filename.substr(0, posOfExt) + "." + "gfs";
      return newFilename;
    }
    return filename;
  } else {
    std::string filebasename = ACE::basename(filename.c_str());
    int32 len = static_cast<int32>(filebasename.length());
    size_t fileExtPos = filebasename.find_last_of('.', len);
    // if no extension then add .gfs extension
    if (fileExtPos == std::string::npos) {
      std::string newFilename = filename + "." + "gfs";
      return newFilename;
    } else {
      std::string extName = filebasename.substr(fileExtPos + 1);
      // if extension other than .gfs change it to ext + .log
      if (extName != "gfs") {
        std::string newFilename = filename + "." + "gfs";
        return newFilename;
      }
      // .gfs Extension already provided, no need to append any extension.
      else {
        return filename;
      }
    }
  }
}

int32 HostStatSampler::rollArchive(std::string filename) {
  FILE* fpExist = fopen(filename.c_str(), "r");
  if (fpExist == NULL) {
    return 0;  // no need to roll
  } else {
    fclose(fpExist);
    ACE_stat statBuf = {0};
    ACE_OS::stat(filename.c_str(), &statBuf);
    if (statBuf.st_size == 0) return 0;  // reuse this file
  }

  std::string statsdirname;
  std::string statsbasename;
  std::string fnameBeforeExt;
  std::string extName;
  std::string newfilestr;
  bool gotNewFileName = false;
  int32 len = static_cast<int32>(filename.length());
  int32 lastPosOfSep = static_cast<int32>(
      filename.find_last_of(ACE_DIRECTORY_SEPARATOR_CHAR, len));
  if (lastPosOfSep == -1) {
    statsdirname = ".";
  } else {
    statsdirname = filename.substr(0, lastPosOfSep);
  }
  statsbasename = filename.substr(lastPosOfSep + 1, len);
  char gfsFileExtAfter = '.';
  int32 baselen = static_cast<int32>(statsbasename.length());
  int32 posOfExt = static_cast<int32>(
      statsbasename.find_last_of(gfsFileExtAfter, static_cast<int32>(baselen)));
  if (posOfExt == -1) {
    // throw IllegalArgument;
  } else {
    fnameBeforeExt = statsbasename.substr(0, posOfExt);
    extName = statsbasename.substr(posOfExt + 1, baselen);
  }

  int32 i = this->rollIndex;
  // 1000 is a good enough val to hold even a very int64 filename.
  while (!gotNewFileName) {
    char newfilename[1000] = {0};
    if (i < 10) {
      ACE_OS::snprintf(newfilename, 1000, "%s%c%s-%d.%s", statsdirname.c_str(),
                       ACE_DIRECTORY_SEPARATOR_CHAR, fnameBeforeExt.c_str(), i,
                       extName.c_str());
    } else {
      ACE_OS::snprintf(newfilename, 1000, "%s%c%s-%d.%s", statsdirname.c_str(),
                       ACE_DIRECTORY_SEPARATOR_CHAR, fnameBeforeExt.c_str(), i,
                       extName.c_str());
    }
    FILE* fp = fopen(newfilename, "r");

    if (fp != NULL) {
      // file exists; increment i and try the next filename
      i++;
      fclose(fp);
    } else {
      newfilestr = newfilename;
      // In the next call it will start looking from rollIndex = i+1
      if (rename(filename.c_str(), newfilestr.c_str()) < 0) {
        return -1;
      } else {
        this->rollIndex = i + 1;
        return 0;
      }
    }
  }
  // The control will come here in case of error only.
  // This return looks redundant but introduced to remove warnings
  // while compiling the code.
  return -1;
}

void HostStatSampler::initSpecialStats() {
  // After Special categories are decided initialize them here
  HostStatHelper::newProcessStats(m_pid, "ProcessStats");
}

void HostStatSampler::sampleSpecialStats() { HostStatHelper::refresh(); }

void HostStatSampler::closeSpecialStats() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_statMngr->getListMutex());
  HostStatHelper::close();
  HostStatHelper::cleanup();
}

void HostStatSampler::checkListeners() {}

void HostStatSampler::start() {
  if (!m_running) {
    m_running = true;
    this->activate();
  }
}

void HostStatSampler::stop() {
  m_stopRequested = true;
  this->wait();
  // while (m_running) {
  //  ACE_OS::sleep(100);
  //}
}

bool HostStatSampler::isRunning() { return m_running; }

void HostStatSampler::putStatsInAdminRegion() {
  try {
    // Get Values of gets, puts,misses,listCalls,numThreads
    static bool initDone = false;
    static std::string clientId = "";
    AdminRegionPtr adminRgn = m_statMngr->getAdminRegion();
    if (adminRgn == NULLPTR) return;
    TryReadGuard _guard(adminRgn->getRWLock(), adminRgn->isDestroyed());
    if (!adminRgn->isDestroyed()) {
      TcrConnectionManager* m_conn_man = adminRgn->getConnectionManager();
      if (m_conn_man->getNumEndPoints() > 0) {
        if (!initDone) {
          adminRgn->init();
          initDone = true;
        }
        int puts = 0, gets = 0, misses = 0, numListeners = 0, numThreads = 0,
            creates = 0;
        int64 cpuTime = 0;
        GemfireStatisticsFactory* gf =
            GemfireStatisticsFactory::getExistingInstance();
        if (gf) {
          StatisticsType* cacheStatType = gf->findType("CachePerfStats");
          if (cacheStatType) {
            Statistics* cachePerfStats =
                gf->findFirstStatisticsByType(cacheStatType);
            if (cachePerfStats) {
              puts = cachePerfStats->getInt((char*)"puts");
              gets = cachePerfStats->getInt((char*)"gets");
              misses = cachePerfStats->getInt((char*)"misses");
              creates = cachePerfStats->getInt((char*)"creates");
              numListeners =
                  cachePerfStats->getInt((char*)"cacheListenerCallsCompleted");
              puts += creates;
            }
          }
          numThreads = HostStatHelper::getNumThreads();
          cpuTime = HostStatHelper::getCpuTime();
        }
        static int numCPU = ACE_OS::num_processors();
        ClientHealthStatsPtr obj = ClientHealthStats::create(
            gets, puts, misses, numListeners, numThreads, cpuTime, numCPU);
        if (clientId.empty()) {
          ACE_TCHAR hostName[256];
          ACE_OS::hostname(hostName, sizeof(hostName) - 1);
          ACE_INET_Addr driver("", hostName, "tcp");
          uint32_t hostAddr = driver.get_ip_address();
          uint16_t hostPort = 0;
          SystemProperties* sysProp = DistributedSystem::getSystemProperties();
          const char* durableId =
              (sysProp != NULL) ? sysProp->durableClientId() : NULL;
          const uint32_t durableTimeOut =
              (sysProp != NULL) ? sysProp->durableTimeout() : 0;

          ClientProxyMembershipID memId(hostName, hostAddr, hostPort, durableId,
                                        durableTimeOut);
          clientId = memId.getDSMemberIdForThinClientUse();
        }

        CacheableKeyPtr keyPtr = CacheableString::create(clientId.c_str());
        adminRgn->put(keyPtr, obj);
      }
    }
  } catch (const Exception&) {
    m_adminError = true;
  }
}

void HostStatSampler::writeGfs() {
  std::string archivefilename = createArchiveFileName();
  changeArchive(archivefilename);
}

void HostStatSampler::forceSample() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> samplingGuard(m_samplingLock);

  if (m_archiver) {
    sampleSpecialStats();
    m_archiver->sample();
    m_archiver->flush();
  }
}

void HostStatSampler::doSample(std::string& archivefilename) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> samplingGuard(m_samplingLock);

  sampleSpecialStats();
  checkListeners();

  // Populate Admin Region for GFMon
  if (isSamplingEnabled() && !m_adminError &&
      !TcrConnectionManager::isNetDown) {
    putStatsInAdminRegion();
  }

  m_archiver->sample();

  if (m_archiveFileSizeLimit != 0) {
    int64 size = m_archiver->getSampleSize();
    int64 bytesWritten = m_archiver->bytesWritten();  // + g_previoussamplesize;
    if (bytesWritten > (m_archiveFileSizeLimit - size)) {
      // roll the archive
      changeArchive(archivefilename);
    }
  }
  g_spaceUsed += m_archiver->bytesWritten();
  // delete older stat files if disk limit is about to be exceeded.
  if ((m_archiveDiskSpaceLimit != 0) &&
      (g_spaceUsed >=
       (m_archiveDiskSpaceLimit - m_archiver->getSampleSize()))) {
    checkDiskLimit();
  }

  // It will flush the contents to the archive file, in every
  // sample run.

  m_archiver->flush();
}

void HostStatSampler::checkDiskLimit() {
  g_fileInfo fileInfo;
  g_spaceUsed = 0;
  char fullpath[512] = {0};
  std::string dirname = ACE::dirname(g_statFile.c_str());
  // struct dirent **resultArray;
  // int entries_count = ACE_OS::scandir(dirname.c_str(), &resultArray,
  // selector, comparator);
  ACE_stat statBuf = {0};
  ACE_Dirent_Selector sds;
  int status = sds.open(dirname.c_str(), selector, comparator);
  if (status != -1) {
    for (int i = 1; i < sds.length(); i++) {
      ACE_OS::snprintf(fullpath, 512, "%s%c%s", dirname.c_str(),
                       ACE_DIRECTORY_SEPARATOR_CHAR, sds[i]->d_name);
      ACE_OS::stat(fullpath, &statBuf);
      g_fileInfoPair = std::make_pair(fullpath, statBuf.st_size);
      fileInfo.push_back(g_fileInfoPair);
      g_spaceUsed += fileInfo[i - 1].second;
    }
    g_spaceUsed += m_archiver->bytesWritten();
    /*for(int i = 0; i < entries_count; i++) {
    ACE_OS::free ( resultArray[i] );
    }
    if (entries_count >= 0) {
    ACE_OS::free( resultArray );
    resultArray = NULL;
    }*/
    sds.close();
  }
  int fileIndex = 0;
  while ((g_spaceUsed >
          m_archiveDiskSpaceLimit) /*&& (fileIndex < entries_count)*/) {
    int64 fileSize = fileInfo[fileIndex].second;
    if (ACE_OS::unlink(fileInfo[fileIndex].first.c_str()) == 0) {
      g_spaceUsed -= fileSize;
    } else {
      LOGWARN("%s\t%s\n", "Could not delete",
              fileInfo[fileIndex].first.c_str());
    }
    fileIndex++;
  }
}

int32 HostStatSampler::svc(void) {
  DistributedSystemImpl::setThreadName(NC_HSS_Thread);
  try {
    initSpecialStats();
    // createArchiveFileName instead of getArchiveFileName here because
    // for the first time the sampler needs to add the pid to the filename
    // passed to it.
    std::string archivefilename = createArchiveFileName();
    if (!m_isStatDiskSpaceEnabled) {
      changeArchive(archivefilename);
    }
    int32 msSpentWorking = 0;
    int32 msRate = static_cast<int32>(getSampleRate());
    bool gotexception = false;
    int waitTime = 0;
    while (!m_stopRequested) {
      try {
        if (gotexception) {
          ACE_OS::sleep(1);

          waitTime++;
          if (waitTime < 60) {  // sleep for minute and then try to recreate
            continue;
          }
          waitTime = 0;
          gotexception = false;
          changeArchive(archivefilename);
        }
        int64 sampleStartNanos = NanoTimer::now();

        doSample(archivefilename);

        int64 sampleEndNanos = NanoTimer::now();
        int64 nanosSpentWorking = sampleEndNanos - sampleStartNanos;
        // updating the sampler statistics
        accountForTimeSpentWorking(nanosSpentWorking);
        msSpentWorking = static_cast<int32>(nanosSpentWorking / 1000000);

        // Continous check for m_stopRequested to sped up stop time.
        int32 msToWait = msRate - msSpentWorking;
        while (msToWait > 0) {
          ACE_Time_Value sleepTime;
          sleepTime.msec(msToWait > 100 ? 100 : msToWait);
          ACE_OS::sleep(sleepTime);
          msToWait -= 100;
          if (m_stopRequested) {
            break;
          }
        }
      } catch (Exception& e) {
        // log the exception and let the thread exit.
        LOGERROR("Exception in statistics sampler thread: %s: %s", e.getName(),
                 e.getMessage());
        // now close current archiver and see if we can start new one
        gotexception = true;
      } catch (...) {
        LOGERROR("Unknown Exception in statistics sampler thread: ");
        gotexception = true;
      }
    }
    closeSpecialStats();
    m_samplerStats->close();
    if (m_archiver != NULL) {
      m_archiver->close();
    }
  } catch (Exception& e) {
    // log the exception and let the thread exit.
    LOGERROR("Exception in statistics sampler thread: %s: %s", e.getName(),
             e.getMessage());
    closeSpecialStats();
  } /* catch (...) {
       // log the exception and let the thread exit.
       LOGERROR("Exception in sampler thread ");
       closeSpecialStats();
   }*/
  m_running = false;
  return 0;
}
