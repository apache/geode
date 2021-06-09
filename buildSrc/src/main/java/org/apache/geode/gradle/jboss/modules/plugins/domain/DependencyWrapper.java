package org.apache.geode.gradle.jboss.modules.plugins.domain;

import org.gradle.api.artifacts.Dependency;

public class DependencyWrapper {
    private boolean embed = false;
    private Dependency dependency;

    public DependencyWrapper(Dependency dependency,boolean embed) {
        this.embed = embed;
        this.dependency = dependency;
    }

    public boolean isEmbedded() {
        return embed;
    }

    public Dependency getDependency() {
        return dependency;
    }
}
