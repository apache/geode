#include <gfcpp/CqListener.hpp>

using namespace gemfire;
CqListener::CqListener() {}
void CqListener::onEvent(const CqEvent& aCqEvent) {}
void CqListener::onError(const CqEvent& aCqEvent) {}
void CqListener::close() {}
