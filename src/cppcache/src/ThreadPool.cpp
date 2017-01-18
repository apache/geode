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
/*
 * ThreadPool.cpp
 *
 *  Created on: 16-Mar-2010
 *      Author: ankurs
 */

#include "ThreadPool.hpp"
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/SystemProperties.hpp>
#include "DistributedSystemImpl.hpp"
using namespace apache::geode::client;

ThreadPoolWorker::ThreadPoolWorker(IThreadPool* manager)
    : manager_(manager), threadId_(0), queue_(msg_queue()), shutdown_(0) {}

ThreadPoolWorker::~ThreadPoolWorker() { shutDown(); }

int ThreadPoolWorker::perform(ACE_Method_Request* req) {
  ACE_TRACE(ACE_TEXT("Worker::perform"));
  return this->queue_.enqueue(req);
}

int ThreadPoolWorker::svc(void) {
  threadId_ = ACE_Thread::self();
  while (1) {
    ACE_Method_Request* request = this->queue_.dequeue();
    if (request == 0) {
      shutDown();
      break;
    }

    // Invoke the request
    request->call();

    // Return to work.
    this->manager_->returnToWork(this);
  }
  return 0;
}

int ThreadPoolWorker::shutDown(void) {
  if (shutdown_ != 1) {
    queue_.queue()->close();
    wait();
    shutdown_ = 1;
  }

  return shutdown_;
}

ACE_thread_t ThreadPoolWorker::threadId(void) { return threadId_; }

ThreadPool::ThreadPool()
    : shutdown_(0), workersLock_(), workersCond_(workersLock_) {
  SystemProperties* sysProp = DistributedSystem::getSystemProperties();
  poolSize_ = sysProp->threadPoolSize();
  activate();
}

ThreadPool::~ThreadPool() { shutDown(); }

int ThreadPool::perform(ACE_Method_Request* req) {
  return this->queue_.enqueue(req);
}

const char* ThreadPool::NC_Pool_Thread = "NC Pool Thread";
int ThreadPool::svc(void) {
  DistributedSystemImpl::setThreadName(NC_Pool_Thread);
  // Create pool when you get in the first time.
  createWorkerPool();
  while (!done()) {
    // Get the next message
    ACE_Method_Request* request = this->queue_.dequeue();
    if (request == 0) {
      shutDown();
      break;
    }
    // Choose a worker.
    ThreadPoolWorker* worker = chooseWorker();
    // Ask the worker to do the job.
    worker->perform(request);
  }
  return 0;
}

int ThreadPool::shutDown(void) {
  if (shutdown_ != 1) {
    queue_.queue()->close();
    wait();
    shutdown_ = 1;
  }

  return shutdown_;
}

int ThreadPool::returnToWork(ThreadPoolWorker* worker) {
  ACE_GUARD_RETURN(ACE_Thread_Mutex, workerMon, this->workersLock_, -1);
  this->workers_.enqueue_tail(worker);
  this->workersCond_.signal();
  return 0;
}

ThreadPoolWorker* ThreadPool::chooseWorker(void) {
  ACE_GUARD_RETURN(ACE_Thread_Mutex, workerMon, this->workersLock_, 0);
  while (this->workers_.is_empty()) workersCond_.wait();
  ThreadPoolWorker* worker;
  this->workers_.dequeue_head(worker);
  return worker;
}

int ThreadPool::createWorkerPool(void) {
  ACE_GUARD_RETURN(ACE_Thread_Mutex, worker_mon, this->workersLock_, -1);
  for (int i = 0; i < poolSize_; i++) {
    ThreadPoolWorker* worker;
    ACE_NEW_RETURN(worker, ThreadPoolWorker(this), -1);
    this->workers_.enqueue_tail(worker);
    worker->activate();
  }
  return 0;
}

int ThreadPool::done(void) { return (shutdown_ == 1); }

ACE_thread_t ThreadPool::threadId(ThreadPoolWorker* worker) {
  return worker->threadId();
}
