package org.fulin.chestnut;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
public class BloomFilterService {

    private BloomFilter<String> bloomFilter;

    public BloomFilterService(int size) {
        bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), size);
    }

    public boolean add(long key, long value) {
        if (key <= 0 || value <= 0) {
            // or throw exception?
            //return false;
            throw new IllegalArgumentException("key and value must be positive");
        }

        String str = key + "." + value;
        return bloomFilter.put(str);
    }

    // mightContain
    public boolean contains(long key, long value) {
        String str = key + "." + value;
        return bloomFilter.mightContain(str);
    }

}
