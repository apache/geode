#Managing Backwards Compatibility

##Public API and Javadocs
In general, nothing can be removed or modified in way that is not backwards compatible. If something is deprecated, it can be removed after a couple of major releases, but in practice removing deprecated features requires sign off from product management.

All public and internal APIs should have useful javadocs. For all public 
APIs (anything not in */internal/*), new additions to the javadocs 
should include an @since tag to let the users know when the feature was 
added. Eg

    /**
       * Returns a collection of all of the CacheServers
       * that can serve the contents of this Cache> to clients.
       *
       * @see #addCacheServer
       *
       * @since 5.7
       */
      public List<CacheServer> getCacheServers();

Any feature which is deprecated should specify a link to the new alternative.



##Client server compatibility

Clients of any version going back to 5.7 should be able to interact with any 
newer server. That means that if you modify existing client server messages, 
you must add support so that old clients can use the old messages.

*TBD* need much more on how this works


##Peer to peer compatibility for minor versions

Peers should also be compatible both in serialization and in distributed
algorithms between releases.

*TBD* need much more on how this works

##Serialization compatibility

The interface SerializationVersions allows you to mark a class as having multiple
serialization formats and note the versions in which the format changed.
GemFire's DataSerializer pays attention to these markings when serializing
content to be transmitted to peers.

*TBD* add an example

##Persistent file compatibility

Persistent file compatibility between releases is still somewhat ad hoc. For 
minor versions, newer versions should be able to start up using the persistent 
files from old versions. For major versions, ideally we will also allow new 
versions to start up from the persistent files of older versions. However, 
for some releases we may choose to provide a conversion tool that must be 
run before the new version will recover the files.
