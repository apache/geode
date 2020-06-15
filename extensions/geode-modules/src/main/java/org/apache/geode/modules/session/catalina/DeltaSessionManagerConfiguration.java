package org.apache.geode.modules.session.catalina;

/**
 * Method used by Catalina XML configuration.
 */
@SuppressWarnings("unused")
interface DeltaSessionManagerConfiguration {

  void setRegionName(String regionName);

  String getRegionName();

  void setEnableLocalCache(boolean enableLocalCache);

  boolean getEnableLocalCache();

  void setMaxActiveSessions(int maxActiveSessions);

  int getMaxActiveSessions();

  void setRegionAttributesId(String regionType);

  String getRegionAttributesId();

  void setEnableGatewayDeltaReplication(boolean enableGatewayDeltaReplication);

  boolean getEnableGatewayDeltaReplication();

  void setEnableGatewayReplication(boolean enableGatewayReplication);

  boolean getEnableGatewayReplication();

  void setEnableDebugListener(boolean enableDebugListener);

  boolean getEnableDebugListener();

  boolean isCommitValveEnabled();

  void setEnableCommitValve(boolean enable);

  boolean isCommitValveFailfastEnabled();

  void setEnableCommitValveFailfast(boolean enable);

  boolean isBackingCacheAvailable();

  void setPreferDeserializedForm(boolean enable);

  boolean getPreferDeserializedForm();

}