#pragma once

#ifndef GEODE_INTEGRATION_TEST_FW_SPAWN_H_
#define GEODE_INTEGRATION_TEST_FW_SPAWN_H_

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
// Spawn.cpp,v 1.4 2004/01/07 22:40:16 shuston Exp

// @TODO, this out this include list..
#include "ace/OS_NS_stdio.h"
#include "ace/OS_NS_fcntl.h"
#include "ace/OS_NS_pwd.h"
#include "ace/os_include/os_pwd.h"
#include "ace/OS_NS_stdlib.h"
#include "ace/OS_NS_string.h"
#include "ace/OS_NS_unistd.h"

#if defined(_WIN32)
#if (FD_SETSIZE != 1024)
+++bad fdsetsize...
#endif
#endif

#include "ace/Process.h"
#include "ace/Log_Msg.h"

  namespace dunit {

  // Listing 1 code/ch10
  class Manager : virtual public ACE_Process {
   public:
    Manager(const ACE_TCHAR *program_name) : ACE_Process() {
      ACE_OS::strcpy(programName_, program_name);
    }

    virtual int doWork(void) {
      // Spawn the new process; prepare() hook is called first.
      ACE_Process_Options options;
      pid_t pid = this->spawn(options);
      if (pid == -1) {
        ACE_ERROR_RETURN((LM_ERROR, ACE_TEXT("%p\n"), ACE_TEXT("spawn")), -1);
      }
      return pid;
    }

    virtual int doWait(void) {
      // Wait forever for my child to exit.
      if (this->wait() == -1) {
        ACE_ERROR_RETURN((LM_ERROR, ACE_TEXT("%p\n"), ACE_TEXT("wait")), -1);
      }

      // Dump whatever happened.
      this->dumpRun();
      return 0;
    }
    // Listing 1

   protected:
    // Listing 3 code/ch10
    virtual int dumpRun(void) {
      if (ACE_OS::lseek(this->outputfd_, 0, SEEK_SET) == -1) {
        ACE_ERROR_RETURN((LM_ERROR, ACE_TEXT("%p\n"), ACE_TEXT("lseek")), -1);
      }

      char buf[1024];
      int length = 0;

      // Read the contents of the error stream written
      // by the child and print it out.
      while ((length = static_cast<int>(
                  ACE_OS::read(this->outputfd_, buf, sizeof(buf) - 1))) > 0) {
        buf[length] = 0;
        ACE_DEBUG((LM_DEBUG, ACE_TEXT("%C\n"), buf));
      }

      ACE_OS::close(this->outputfd_);
      return 0;
    }
    // Listing 3

    // Listing 2 code/ch10
    // prepare() is inherited from ACE_Process.
    virtual int prepare(ACE_Process_Options &options) {
      options.command_line("%s", this->programName_);
      if (this->setStdHandles(options) == -1 ||
          this->setEnvVariable(options) == -1) {
        return -1;
      }
      return 0;
    }

    virtual int setStdHandles(ACE_Process_Options &options) {
      ACE_OS::unlink(this->programName_);
      this->outputfd_ = ACE_OS::open(this->programName_, O_RDWR | O_CREAT);
      return options.set_handles(ACE_STDIN, ACE_STDOUT, this->outputfd_);
    }

    virtual int setEnvVariable(ACE_Process_Options &options) {
      return options.setenv("PRIVATE_VAR=/that/seems/to/be/it");
    }
    // Listing 2

   private:
   protected:
    virtual ~Manager() {}

   private:
    ACE_HANDLE outputfd_;
    ACE_TCHAR programName_[2048];
  };

};  // namespace dunit.


#endif // GEODE_INTEGRATION_TEST_FW_SPAWN_H_
