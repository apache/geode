package com.gemstone.gemfire.internal.security;

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.geode.security.ResourcePermission;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;

public class IntegratedSecurityService implements SecurityService {

  private static Logger logger = LogService.getLogger(LogService.SECURITY_LOGGER_NAME);

  private static SecurityService defaultInstance = new IntegratedSecurityService();

  public static SecurityService getSecurityService() {
    return defaultInstance;
  }

  @Override
  public ThreadState bindSubject(final Subject subject) {
    return GeodeSecurityUtil.bindSubject(subject);
  }

  @Override
  public Subject getSubject() {
    return GeodeSecurityUtil.getSubject();
  }

  @Override
  public Subject login(final String username, final String password) {
    return GeodeSecurityUtil.login(username, password);
  }

  @Override
  public void logout() {
    GeodeSecurityUtil.logout();
  }

  @Override
  public Callable associateWith(final Callable callable) {
    return GeodeSecurityUtil.associateWith(callable);
  }

  @Override
  public void authorize(final ResourceOperation resourceOperation) {
    GeodeSecurityUtil.authorize(resourceOperation);
  }

  @Override
  public void authorizeClusterManage() {
    GeodeSecurityUtil.authorizeClusterManage();
  }

  @Override
  public void authorizeClusterWrite() {
    GeodeSecurityUtil.authorizeClusterWrite();
  }

  @Override
  public void authorizeClusterRead() {
    GeodeSecurityUtil.authorizeClusterRead();
  }

  @Override
  public void authorizeDataManage() {
    GeodeSecurityUtil.authorizeDataManage();
  }

  @Override
  public void authorizeDataWrite() {
    GeodeSecurityUtil.authorizeDataWrite();
  }

  @Override
  public void authorizeDataRead() {
    GeodeSecurityUtil.authorizeDataRead();
  }

  @Override
  public void authorizeRegionManage(final String regionName) {
    GeodeSecurityUtil.authorizeRegionManage(regionName);
  }

  @Override
  public void authorizeRegionManage(final String regionName, final String key) {
    GeodeSecurityUtil.authorizeRegionManage(regionName, key);
  }

  @Override
  public void authorizeRegionWrite(final String regionName) {
    GeodeSecurityUtil.authorizeRegionWrite(regionName);
  }

  @Override
  public void authorizeRegionWrite(final String regionName, final String key) {
    GeodeSecurityUtil.authorizeRegionWrite(regionName, key);
  }

  @Override
  public void authorizeRegionRead(final String regionName) {
    GeodeSecurityUtil.authorizeRegionRead(regionName);
  }

  @Override
  public void authorizeRegionRead(final String regionName, final String key) {
    GeodeSecurityUtil.authorizeRegionRead(regionName, key);
  }

  @Override
  public void authorize(final String resource, final String operation) {
    GeodeSecurityUtil.authorize(resource, operation);
  }

  @Override
  public void authorize(final ResourcePermission context) {
    GeodeSecurityUtil.authorize(context);
  }

  @Override
  public void initSecurity(final Properties securityProps) {
    GeodeSecurityUtil.initSecurity(securityProps);
  }

  @Override
  public void close() {
    GeodeSecurityUtil.close();
  }

  @Override
  public boolean needPostProcess() {
    return GeodeSecurityUtil.needPostProcess();
  }

  @Override
  public Object postProcess(final String regionPath, final Object key, final Object value, final boolean valueIsSerialized) {
    return GeodeSecurityUtil.postProcess(regionPath, key, value, valueIsSerialized);
  }

  @Override
  public boolean isClientSecurityRequired() {
    return GeodeSecurityUtil.isClientSecurityRequired();
  }

  @Override
  public boolean isPeerSecurityRequired() {
    return GeodeSecurityUtil.isPeerSecurityRequired();
  }

  @Override
  public boolean isIntegratedSecurity() {
    return GeodeSecurityUtil.isIntegratedSecurity();
  }
}
