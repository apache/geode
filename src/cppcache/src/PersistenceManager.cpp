#include <gfcpp/PersistenceManager.hpp>
#include <gfcpp/Region.hpp>

using namespace gemfire;

PersistenceManager::PersistenceManager(const RegionPtr& regionPtr)
    : m_regionPtr(regionPtr) {
  // printf("Base constructor PersistenceManager \n");
}

PersistenceManager::~PersistenceManager() {}
PersistenceManager::PersistenceManager() {}
