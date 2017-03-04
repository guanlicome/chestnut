package org.fulin.chestnut;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.fulin.ChestnutApplication.DATA_PATH;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
public class BloomFilterService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private BloomFilter<CharSequence> bloomFilter;
    private String backupFileName = "bloomFilter.data";
    private AtomicLong updateCount = new AtomicLong(0);
    private long lastCount = 0;
    File f;
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public BloomFilterService(int size) throws IOException {
        Files.createParentDirs(new File(DATA_PATH + "/dir"));

        String filename = DATA_PATH + "/" + backupFileName;
        f = new File(filename);
        if (f.exists()) {
            bloomFilter = BloomFilter.readFrom(new FileInputStream(f), Funnels.unencodedCharsFunnel());
        } else {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), size);
        }

        executorService.schedule(() -> {
            if (updateCount.get() - lastCount > 100) {
                lastCount = updateCount.get();
                try {
                    bloomFilter.writeTo(new FileOutputStream(f));
                    logger.info("succ backup bloom filter to file {} with size {}", filename, size);
                } catch (Exception e) {
                    logger.error("backup bloom filter error", e);
                }
            }
        }, 60, TimeUnit.SECONDS);
    }

    public void close() {
        try {
            bloomFilter.writeTo(new FileOutputStream(f));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean add(long key, long value) {
        if (key <= 0 || value <= 0) {
            // or throw exception?
            return false;
            //throw new IllegalArgumentException("key and value must be positive");
        }

        updateCount.incrementAndGet();
        
        String str = key + "." + value;
        return bloomFilter.put(str);
    }

    // mightContain
    public boolean contains(long key, long value) {
        String str = key + "." + value;
        return bloomFilter.mightContain(str);
    }

}
