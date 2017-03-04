package org.fulin.chestnut;

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static org.fulin.ChestnutApplication.metricRegistry;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
@Service
public class PccService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    // key: uid, value: friend uid list
    private ListMapService friendListMap;

    // key: uid, value: liked oid list
    private ListMapService userLikeListMap;

    // key: oid, value: uid list who liked this
    private ListMapService objectLikedListMap;

    private BloomFilterService bloomFilterService;


    @PostConstruct
    public void init() throws IOException {
        try {
            friendListMap = new ListMapService("friendList", 9000000, 10, 900000, 40, 100000, 100);
            userLikeListMap = new ListMapService("userLikeList", 9000000, 10, 900000, 40, 100000, 100);
            objectLikedListMap = new ListMapService("objectLikedList", 9000000, 10, 900000, 40, 100000, 100);

            // last 7 days
            bloomFilterService = new BloomFilterService(7 * 10000000);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public boolean addFriend(long uid, long friendUid) {
        metricRegistry.counter("addFriend").inc();
        return friendListMap.add(uid, friendUid);
    }

    // return the oid 's liked uid list
    public long[] like(long uid, long oid) {
        metricRegistry.counter("like").inc();
        
        userLikeListMap.add(uid, oid);
        objectLikedListMap.add(oid, uid);
        bloomFilterService.add(uid, oid);

        return objectLikedListMap.getList(oid);
    }

    // 1 for like, 0 for not
    public boolean isLike(long uid, long oid) {
        if (bloomFilterService.contains(uid, oid)) {
            metricRegistry.counter("is_like.bloom.yes").inc();
            boolean ret = objectLikedListMap.contains(oid, uid);

            metricRegistry.counter("is_like.map." + (ret ? "yes" : "no")).inc();
            return ret;
        }

        metricRegistry.counter("is_like.bloom.no").inc();
        return false;
    }

    public long count(long oid) {
        long cnt = objectLikedListMap.getCount(oid);
        metricRegistry.meter("count").mark(cnt);
        return cnt;
    }

    public long[] list(long oid, int pageSize) {
        metricRegistry.meter("list.pageSize").mark(pageSize);
        return objectLikedListMap.getList(oid, pageSize);
    }

    public long[] listFriend(long oid, int pageSize, long uid) {
        metricRegistry.meter("listFriend.pageSize").mark(pageSize);

        long[] friendList = friendListMap.getList(uid);
        if (friendList == null || friendList.length <= 0) {
            return null;
        }
        Set<Long> friends = Sets.newHashSet(Longs.asList(friendList));

        long[] ous = objectLikedListMap.getList(oid);
        ArrayList<Long> uids = new ArrayList<>();

        for (long ou : ous) {
            if (friends.contains(ou)) {
                uids.add(ou);
            }
            if (uids.size() >= pageSize) {
                break;
            }
        }

        return Longs.toArray(uids);
    }

}
