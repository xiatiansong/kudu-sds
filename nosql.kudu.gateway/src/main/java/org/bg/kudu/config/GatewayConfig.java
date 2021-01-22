package org.bg.kudu.config;

import org.bg.kudu.core.ImpalaOptHelper;
import org.bg.kudu.core.KuduHelper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Arrays;
import java.util.List;

/**
 * 配置
 */
@Configuration
@PropertySource("classpath:application.yml")
public class GatewayConfig implements InitializingBean {

    @Value("${kudu.url}")
    String KUDU_HOST_URL;

    @Value("${impala.url}")
    String IMPALA_CONNECT_URL;

    @Value("${impala.driver}")
    String IMPALA_CONNECT_DRIVER;

    public static class Kudu {
        public static String KUDU_HOST_URL;
    }

    public static class Impala {
        public static String IMPALA_CONNECT_URL;
        public static String IMPALA_CONNECT_DRIVER;
    }

    @Override
    public void afterPropertiesSet() {
        Kudu.KUDU_HOST_URL = KUDU_HOST_URL;
        Impala.IMPALA_CONNECT_URL = IMPALA_CONNECT_URL;
        Impala.IMPALA_CONNECT_DRIVER = IMPALA_CONNECT_DRIVER;
        initInstance(IMPALA_CONNECT_URL, IMPALA_CONNECT_DRIVER, "", "", KUDU_HOST_URL);
    }

    private void initInstance(String url, String driverName, String username, String password, String masterHosts) {
        List<String> masterInfos = Arrays.asList(masterHosts.split(","));
        KuduHelper.getInstance(masterInfos);
        ImpalaOptHelper.getInstance(url, driverName, username, password);
    }
}
