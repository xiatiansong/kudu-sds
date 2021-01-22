package org.bg.kudu;

import com.alibaba.dcm.DnsCacheManipulator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * 启动类
 */
@SpringBootApplication
public class GwApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        DnsCacheManipulator.loadDnsCacheConfig();
        new GwApplication().configure(new SpringApplicationBuilder(GwApplication.class)).run(args);
    }
}
