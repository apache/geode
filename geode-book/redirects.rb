r301 %r{/releases/latest/javadoc/(.*)}, 'http://geode.incubator.apache.org/releases/latest/javadoc/$1'
rewrite '/', '/docs/about_geode.html'
rewrite '/index.html', '/docs/about_geode.html'
