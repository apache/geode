package org.apache.geode.internal.metrics;

import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

public class GeodeCompositeMeterRegistry extends CompositeMeterRegistry {

    final private JvmMemoryMetrics jvmMemoryMetrics =  new JvmMemoryMetrics();
    final private JvmThreadMetrics jvmThreadMetrics =  new JvmThreadMetrics();
    final private JvmGcMetrics jvmGcMetrics =  new JvmGcMetrics();
    final private ProcessorMetrics processorMetrics =  new ProcessorMetrics();
    final private UptimeMetrics uptimeMetrics =  new UptimeMetrics();
    final private FileDescriptorMetrics fileDescriptorMetrics =  new FileDescriptorMetrics();


    void registerBinders() {
        jvmMemoryMetrics.bindTo(this);
        jvmThreadMetrics.bindTo(this);
        jvmGcMetrics.bindTo(this);
        processorMetrics.bindTo(this);
        uptimeMetrics.bindTo(this);
        fileDescriptorMetrics.bindTo(this);

    }

    @Override
    public void close() {
        jvmGcMetrics.close();
        super.close();
     }
}
