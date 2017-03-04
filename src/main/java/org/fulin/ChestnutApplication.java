package org.fulin;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.io.Files;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ChestnutApplication {

    // change to /dev/shm for performance
    public static String DATA_PATH = System.getProperty("pcc.data.dir", System.getProperty("java.io.tmpdir") + "/pcc");

    public static final int DEFAULT_LIST_LEN = 20;

    public static MetricRegistry metricRegistry = new MetricRegistry();

    public static void main(String[] args) throws IOException {
        Files.createParentDirs(new File(DATA_PATH + "/dir"));

        Slf4jReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()
                .start(10, TimeUnit.SECONDS);

        SpringApplication.run(ChestnutApplication.class, args);
    }
}
