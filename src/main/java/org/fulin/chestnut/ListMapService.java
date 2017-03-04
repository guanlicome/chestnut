package org.fulin.chestnut;

import com.google.common.io.Files;
import net.openhft.chronicle.map.ChronicleMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.fulin.ChestnutApplication.DATA_PATH;
import static org.fulin.ChestnutApplication.metricRegistry;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
public class ListMapService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String listName;

    private ChronicleMap<Long, long[]> smallListMap;
    private ChronicleMap<Long, long[]> medianListMap;
    private ChronicleMap<Long, long[]> largeListMap;

    private ChronicleMap<Long, Long> countMap;

    private int smallThreshold = 10;
    private int medianThreshold = 100;

    public ListMapService(String listName,
                          long smallEntriesSize, int smallValueLength,
                          long mediaEntriesSize, int medianValueLength,
                          long largeEntriesSize, int largeValueLength) throws IOException {
        this.listName = listName;
        this.smallThreshold = smallValueLength;
        this.medianThreshold = medianValueLength;

        Files.createParentDirs(new File(DATA_PATH + "/dir"));

        logger.info("chronicle map {} put data in dir: {}", listName, DATA_PATH);
        logger.info("chronicle map {} with config: small {}/{}, media {}/{}, large {}/{}",
                listName, smallEntriesSize, smallValueLength,
                mediaEntriesSize, medianValueLength, largeEntriesSize, largeValueLength);

        smallListMap = ChronicleMap
                .of(Long.class, long[].class)
                .name("small-" + listName)
                .entries(smallEntriesSize)
                .averageValue(new long[smallValueLength])
                .createPersistedTo(new File(DATA_PATH + "/small-" + listName));

        medianListMap = ChronicleMap
                .of(Long.class, long[].class)
                .name("median-" + listName)
                .entries(mediaEntriesSize)
                .averageValue(new long[medianValueLength])
                .createPersistedTo(new File(DATA_PATH + "/median-" + listName));

        largeListMap = ChronicleMap
                .of(Long.class, long[].class)
                .name("large-" + listName)
                .entries(largeEntriesSize)
                .averageValue(new long[largeValueLength])
                .createPersistedTo(new File(DATA_PATH + "/large-" + listName));

        countMap = ChronicleMap
                .of(Long.class, Long.class)
                .name("count-" + listName)
                .entries(smallEntriesSize + mediaEntriesSize + largeEntriesSize)
                .createPersistedTo(new File(DATA_PATH + "/count-" + listName));
    }

    public void close() {
        smallListMap.close();
        medianListMap.close();
        largeListMap.close();
        countMap.close();
    }

    public boolean add(long key, long[] values) {
        if (key <= 0 || values.length <= 0) {
            // or throw exception?
            return false;
            //throw new IllegalArgumentException("key and value must be positive");
        }

        int len = getCount(key);

        if (len > 0 || values.length < medianThreshold) {
            for (long v : values) {
                add(key, v);
            }
            return true;
        }

        largeListMap.put(key, values);
        metricRegistry.counter("listMap.large.addMulti").inc();
        logger.info("addMulti to large for key {}, size: {}", key, values.length);
        setCount(key, values.length, false);

        return true;
    }

    // TODO add lock
    // assume all positive numbers
    // TODO add to tail or add to head ? do we need reverse ?
    public boolean add(long key, long value) {
        try {
            return addToTail(key, value);
        } catch (Exception e) {
            logger.error("error when add {}:{} list:{}", key, value, listName);
            throw e;
        }
    }

    public boolean addToTail(long key, long value) {
        if (key <= 0 || value <= 0) {
            // or throw exception?
            return false;
            //throw new IllegalArgumentException("key and value must be positive");
        }

        int len = getCount(key);

        long[] v = getList(key, 0);

        if (v == null || len <= 0) {
            v = new long[smallThreshold];
            v[0] = value;
            smallListMap.put(key, v);
            metricRegistry.counter("listMap.small.new").inc();
        } else if (len < smallThreshold && v[len] <= 0) {
            v[len] = value;
            smallListMap.put(key, v);
            metricRegistry.counter("listMap.small.put").inc();
        } else if (len > smallThreshold && len < medianThreshold && v[len] <= 0) {
            v[len] = value;
            medianListMap.put(key, v);
            metricRegistry.counter("listMap.median.put").inc();
        } else if (len > medianThreshold && len < v.length && v[len] <= 0) {
            v[len] = value;
            largeListMap.put(key, v);
            metricRegistry.counter("listMap.large.put").inc();
        } else if (len == smallThreshold && v[len - 1] > 0) {
            long[] v2 = new long[medianThreshold];
            System.arraycopy(v, 0, v2, 0, smallThreshold);
            v = v2;
            v[smallThreshold] = value;
            medianListMap.put(key, v);
            // TODO performance optimize : do not remove from smallListMap
            // so get page 1,2,3... of the list will be fast
            smallListMap.remove(key);
            metricRegistry.counter("listMap.small.promote").inc();
            logger.info("promote from small to median for key {}", key);
        } else if (len == medianThreshold && v[len - 1] > 0) {
            long[] v2 = new long[medianThreshold * 2];
            System.arraycopy(v, 0, v2, 0, medianThreshold);
            v = v2;
            v[medianThreshold] = value;
            largeListMap.put(key, v);
            medianListMap.remove(key);
            metricRegistry.counter("listMap.median.promote").inc();
            logger.info("promote from median to large for key {}", key);
        } else if (len > medianThreshold && v[v.length - 1] > 0) {
            int p = v.length;
            long[] v2 = new long[p * 2];
            System.arraycopy(v, 0, v2, 0, p);
            v = v2;
            v[p] = value;
            largeListMap.put(key, v);
            metricRegistry.counter("listMap.large.promote").inc();
            logger.info("promote from large to large*2 for key {}, size: {}", key, p * 2);
        } else {
            // wtf?
            // TODO log more info
            metricRegistry.counter("listMap.unknown").inc();
            logger.error("unknown state of list {}, for key: {}, value len {}, value: {}",
                    listName, key, len, Arrays.asList(v));
            throw new IllegalStateException("unknown state");
        }

        setCount(key, (long) (len + 1), true);

        return true;
    }

    // so the list size can not > INT.MAX
    public int getCount(long key) {
        Long v = countMap.get(key);
        if (v == null) {
            metricRegistry.counter("listMap.getCount.null").inc();
            return 0;
        }
        metricRegistry.counter("listMap.getCount").inc();
        return v.intValue();
    }

    private void setCount(long key, long value, boolean isAdd) {
        countMap.put(key, value);

        if ((isAdd && value == 1000_000L) || (!isAdd && value >= 1000_000L)) {
            metricRegistry.counter(listName + ".count.1m").inc();
        } else if ((isAdd && value == 100_000L) || (!isAdd && value >= 100_000L)) {
            metricRegistry.counter(listName + ".count.100k").inc();
        } else if ((isAdd && value == 10_000L) || (!isAdd && value >= 10_000L)) {
            metricRegistry.counter(listName + ".count.10k").inc();
        } else if ((isAdd && value == 1000L) || (!isAdd && value >= 1000L)) {
            metricRegistry.counter(listName + ".count.1000").inc();
        } else if ((isAdd && value == 100L) || (!isAdd && value >= 100L)) {
            metricRegistry.counter(listName + ".count.100").inc();
        } else if ((isAdd && value == 80L) || (!isAdd && value >= 80L)) {
            metricRegistry.counter(listName + ".count.80").inc();
        } else if ((isAdd && value == 40L) || (!isAdd && value >= 40L)) {
            metricRegistry.counter(listName + ".count.40").inc();
        } else if ((isAdd && value == 20L) || (!isAdd && value >= 20L)) {
            metricRegistry.counter(listName + ".count.20").inc();
        }
    }

    // pool performance, need a bloom filter before this
    public boolean contains(long key, long value) {
        long[] v = getList(key, 0);
        if (v == null || v.length <= 0) {
            return false;
        }

        for (long vv : v) {
            if (vv == value) {
                return true;
            }
        }
        return false;
    }

    public long[] getList(long key) {
        return getList(key, Integer.MAX_VALUE);
    }

    /**
     * @param key
     * @param size 0 means no truncation, INT.MAX means truncate to len
     * @return
     */
    public long[] getList(long key, int size) {

        int len = getCount(key);

        long[] v;
        if (len <= 0) {
            v = null;
        } else if (len <= smallThreshold) {
            v = smallListMap.get(key);
        } else if (len <= medianThreshold) {
            v = medianListMap.get(key);
        } else {
            v = largeListMap.get(key);
        }

        if (size <= 0 || v == null) {
            return v;
        }

        int copyLen = len > size ? size : len;

        long[] v2 = new long[copyLen];
        System.arraycopy(v, 0, v2, 0, copyLen);
        return v2;
    }
}
