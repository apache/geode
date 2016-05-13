#include "PersistenceManager.hpp"
#include "Region.hpp"

using namespace gemfire;


PersistenceManager::PersistenceManager(const RegionPtr& regionPtr):m_regionPtr(regionPtr) {
  //printf("Base constructor PersistenceManager \n");
}

PersistenceManager::~PersistenceManager() {}
PersistenceManager::PersistenceManager() {}
