package org.fulin.chestnut;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

import static org.fulin.chestnut.Response.*;

/**
 * chestnut
 *
 * @author tangfulin
 * @since 17/3/3
 */
@RestController
@RequestMapping(path = "")
public class PccController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    PccService pccService;

    String[] actions = new String[] {"like", "is_like", "count", "list"};
    Random random = new Random(System.currentTimeMillis());

    @RequestMapping(path = "/pcc")
    public Response action(@RequestParam(value = "action") String action,
                           @RequestParam(value = "uid", defaultValue = "0", required = false) long uid,
                           @RequestParam(value = "oid", defaultValue = "0", required = false) long oid,
                           @RequestParam(value = "page_size", defaultValue = "10", required = false) int pageSize,
                           @RequestParam(value = "is_friend", defaultValue = "0", required = false) int isFriend) {
        try {

            if (action.toLowerCase().startsWith("press")) {
                if (action.equalsIgnoreCase("press")) {
                    int p = Math.abs(random.nextInt()) % actions.length;
                    action = actions[p];
                } else {
                    action = action.substring("press_".length());
                }
                uid = Math.abs(random.nextInt()) % 10000000;
                oid = Math.abs(random.nextInt()) % 10000000;
                pageSize = Math.abs(random.nextInt()) % 20;
                isFriend = Math.abs(random.nextInt()) % 2;
            }

            if (action.equalsIgnoreCase("like")) {
                return like(uid, oid);
            }

            if (action.equalsIgnoreCase("is_like")) {
                return isLike(uid, oid);
            }

            if (action.equalsIgnoreCase("count")) {
                return count(oid);
            }

            if (action.equalsIgnoreCase("list")) {
                if (isFriend > 0) {
                    return listFriend(oid, pageSize, uid);
                } else {
                    return list(oid, pageSize);
                }
            }

            return CLIENT_ERROR_RESPONSE;
        } catch (Exception e) {
            logger.error("error for action {}", action, e);
            return SERVER_ERROR_RESPONSE;
        }
    }

    // return the oid 's liked uid list
    // return error for uid already like oid
    @RequestMapping(path = "/pcc/like")
    public Response<long[]> like(long uid, long oid) {
        if (pccService.isLike(uid, oid)) {
            return ALREADY_LIKE_ERROR_RESPONSE;
        }
        return Response.of("like", uid, oid, pccService.like(uid, oid));
    }

    // 1 for like, 0 for not
    @RequestMapping(path = "/pcc/is_like")
    public Response<Integer> isLike(long uid, long oid) {
        int result = pccService.isLike(uid, oid) ? 1 : 0;
        return Response.of("is_like", uid, oid, result);
    }

    @RequestMapping(path = "/pcc/count")
    public Response<Long> count(long oid) {
        return Response.of("count", 0, oid, pccService.count(oid));
    }

    @RequestMapping(path = "/pcc/list")
    public Response<long[]> list(long oid, int pageSize) {
        return Response.of("list", 0, oid, pccService.list(oid, pageSize));
    }

    @RequestMapping(path = "/pcc/list_friend")
    public Response<long[]> listFriend(long oid, int pageSize, long uid) {
        return Response.of("list_friend", uid, oid, pccService.listFriend(oid, pageSize, uid));
    }
}
