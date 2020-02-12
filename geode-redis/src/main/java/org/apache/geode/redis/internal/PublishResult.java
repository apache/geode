package org.apache.geode.redis.internal;

/**
 * Represents the results of publishing a message to a subscription. Contains the client the message
 * was published to as well as whether or not the message was published successfully.
 */
public class PublishResult {
  private final Client client;
  private final boolean result;

  public PublishResult(Client client, boolean result) {
    this.client = client;
    this.result = result;
  }

  public Client getClient() {
    return client;
  }

  public boolean isSuccessful() {
    return result;
  }
}
