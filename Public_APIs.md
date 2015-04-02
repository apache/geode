#Public API and Javadocs
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


