package org.fulin.chestnut;

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.fulin.ChestnutApplication.DATA_PATH;
import static org.fulin.ChestnutApplication.DEFAULT_LIST_LEN;
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

    public static int TOTAL_USER_CNT = 100_000_000;
    public static int AVG_FRIENDS_CNT = 100;
    public static int TOTAL_OBJECT_CNT = 100_000_000;
    public static int AVG_OBJECT_LIKED_CNT = 20;
    public static int AVG_USER_LIKE_CNT = 20;

    // key: uid, value: friend uid list
    private ListMapService friendListMap;

    // key: uid, value: liked oid list
    private ListMapService userLikeListMap;

    // key: oid, value: uid list who liked this
    private ListMapService objectLikedListMap;

    // key: uid, value: nickname
    private ChronicleMap<Long, String> nicknames;

    private BloomFilterService bloomFilterService;


    @PostConstruct
    public void init() throws IOException {
        try {
            friendListMap = new ListMapService("friendList",
                    (int) (TOTAL_USER_CNT * 0.9), (int) (AVG_FRIENDS_CNT * 0.2),
                    (int) (TOTAL_USER_CNT * 0.09), AVG_FRIENDS_CNT,
                    (int) (TOTAL_USER_CNT * 0.02), AVG_FRIENDS_CNT * 2);

            userLikeListMap = new ListMapService("userLikeList",
                    (int) (TOTAL_USER_CNT * 0.9), (int) (AVG_USER_LIKE_CNT * 0.5),
                    (int) (TOTAL_USER_CNT * 0.09), AVG_USER_LIKE_CNT,
                    (int) (TOTAL_USER_CNT * 0.02), AVG_USER_LIKE_CNT * 2);

            objectLikedListMap = new ListMapService("objectLikedList",
                    (int) (TOTAL_OBJECT_CNT * 0.9), (int) (AVG_OBJECT_LIKED_CNT * 0.5),
                    (int) (TOTAL_OBJECT_CNT * 0.09), AVG_OBJECT_LIKED_CNT * 2,
                    (int) (TOTAL_OBJECT_CNT * 0.02), AVG_OBJECT_LIKED_CNT * 20);

            nicknames = ChronicleMap
                    .of(Long.class, String.class)
                    .name("nickname")
                    .entries(TOTAL_USER_CNT)
                    .averageValue("TangFulin")
                    .createPersistedTo(new File(DATA_PATH + "/nickname"));

            // last 7 days
            bloomFilterService = new BloomFilterService(7 * 10000000);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PreDestroy
    public void close() {
        long start = System.currentTimeMillis();
        friendListMap.close();
        userLikeListMap.close();
        objectLikedListMap.close();
        nicknames.close();
        bloomFilterService.close();
        long cost = System.currentTimeMillis() - start;
        System.out.println("Succ closed all maps, cost time " + cost);
    }

    public static void prepareForTest() throws IOException {
        File f = new File(DATA_PATH);
        FileUtils.deleteDirectory(f);
        TOTAL_USER_CNT = TOTAL_USER_CNT / 1000;
        TOTAL_OBJECT_CNT = TOTAL_OBJECT_CNT / 1000;
    }

    public User addUser(long uid, String nickname) {
        metricRegistry.counter("addUser").inc();
        nicknames.put(uid, nickname);
        return new User(uid, nickname);
    }

    public List<User> getUsers(long[] uids) {
        if (uids == null || uids.length <= 0) {
            return Collections.emptyList();
        }

        metricRegistry.meter("getUserNames").mark(uids.length);

        ArrayList<User> users = new ArrayList<>();
        for (long uid : uids) {
            String nick = nicknames.get(uid);
            if (nick == null) {
                nick = "";
            }
            users.add(new User(uid, nick));
        }
        return users;
    }

    public boolean addFriend(long uid, long friendUid) {
        metricRegistry.counter("addFriend").inc();
        return friendListMap.add(uid, friendUid);
    }

    public long[] getFriend(long uid) {
        metricRegistry.counter("getFriend").inc();
        return friendListMap.getList(uid);
    }

    // return the oid 's liked uid list
    public long[] like(long uid, long oid) {
        metricRegistry.counter("like").inc();

        //userLikeListMap.add(uid, oid);

        objectLikedListMap.add(oid, uid);
        bloomFilterService.add(uid, oid);

        return objectLikedListMap.getList(oid, DEFAULT_LIST_LEN);
    }

    public long[] addMultiLike(long[] uids, long oid) {
        metricRegistry.counter("multiLike").inc();

        //userLikeListMap.add(uid, oid);

        objectLikedListMap.add(oid, uids);

        for (long uid : uids) {
            bloomFilterService.add(uid, oid);
        }

        return objectLikedListMap.getList(oid, DEFAULT_LIST_LEN);
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

    public long[] list(long oid, int pageSize, long cursor) {
        metricRegistry.meter("list.pageSize").mark(pageSize);
        if (cursor <= 0) {
            return objectLikedListMap.getList(oid, pageSize);
        } else {
            // TODO optimize
            long[] all = objectLikedListMap.getList(oid);
            int pos = 0;
            while (pos < all.length && all[pos] != cursor) {
                pos++;
            }
            // not found, or cursor is the last one
            if (pos >= all.length - 1) {
                return new long[0];
            }

            int len = pageSize;
            if (pos + pageSize >= all.length) {
                len = all.length - pos - 1;
            }
            long[] result = new long[len];
            System.arraycopy(all, pos + 1, result, 0, len);
            return result;
        }
    }

    public long[] listFriend(long oid, int pageSize, long uid, long cursor) {
        metricRegistry.meter("listFriend.pageSize").mark(pageSize);

        long[] friendList = friendListMap.getList(uid);
        if (friendList == null || friendList.length <= 0) {
            return null;
        }
        Set<Long> friends = Sets.newHashSet(Longs.asList(friendList));

        long[] ous = objectLikedListMap.getList(oid);
        ArrayList<Long> uids = new ArrayList<>();
        boolean found = false;

        for (long ou : ous) {
            // loop until we found cursor
            if (cursor > 0 && !found && ou != cursor) {
                continue;
            }

            found = true;
            if (ou != cursor && friends.contains(ou)) {
                uids.add(ou);
            }
            if (uids.size() >= pageSize) {
                break;
            }
        }

        return Longs.toArray(uids);
    }

}
