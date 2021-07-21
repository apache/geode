package org.apache.geode.security;

public class AuthenticationExpiredException extends AuthenticationRequiredException {
  public AuthenticationExpiredException(String message) {
    super(message);
  }

  public AuthenticationExpiredException(String message, Throwable cause) {
    super(message, cause);
  }
}
