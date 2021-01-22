package org.bg.kudu.config;

import org.bg.kudu.web.NosqlResource;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.stereotype.Component;

@Component
public class JerseyConfig extends ResourceConfig {

    public JerseyConfig() {
        packages("org.bg.kudu.web");
        register(NosqlResource.class);
    }
}
