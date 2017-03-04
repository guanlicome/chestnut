package org.fulin.chestnut;

import com.codahale.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.fulin.ChestnutApplication.metricRegistry;
import static org.fulin.chestnut.Response.CLIENT_ERROR_RESPONSE;
import static org.fulin.chestnut.Response.SERVER_ERROR_RESPONSE;

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

    String[] actions = new String[]{"like", "is_like", "count", "list"};
    Random random = new Random(System.currentTimeMillis());

    @RequestMapping(path = "/pcc")
    public Response action(@RequestParam(value = "action") String action,
                           @RequestParam(value = "uid", defaultValue = "0", required = false) long uid,
                           @RequestParam(value = "oid", defaultValue = "0", required = false) long oid,
                           @RequestParam(value = "cursor", defaultValue = "0", required = false) long cursor,
                           @RequestParam(value = "page_size", defaultValue = "20", required = false) int pageSize,
                           @RequestParam(value = "is_friend", defaultValue = "0", required = false) int isFriend) {
        try {

            metricRegistry.counter("action." + action).inc();

            if (action.toLowerCase().startsWith("press")) {
                if (action.equalsIgnoreCase("press")) {
                    int p = Math.abs(random.nextInt()) % actions.length;
                    action = actions[p];
                    metricRegistry.counter("action.random." + action).inc();
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
                    return listFriend(oid, pageSize, uid, cursor);
                } else {
                    return list(oid, pageSize, cursor);
                }
            }

            metricRegistry.counter("error.client").inc();

            return CLIENT_ERROR_RESPONSE;
        } catch (Exception e) {
            logger.error("error for action {}", action, e);

            metricRegistry.counter("error.server").inc();

            return SERVER_ERROR_RESPONSE;
        }
    }

    // return the oid 's liked uid list
    // return error for uid already like oid
    @Timed
    @RequestMapping(path = "/pcc/like")
    public Response<List<User>> like(long uid, long oid) {
        if (pccService.isLike(uid, oid)) {
            metricRegistry.counter("error.already_like").inc();
            return new Response<List<User>>(499, "User Already Liked this Object").with(uid, oid);
        }
        return Response.of("like", uid, oid, pccService.getUsers(pccService.like(uid, oid)));
    }

    // 1 for like, 0 for not
    @Timed
    @RequestMapping(path = "/pcc/is_like")
    public Response<Integer> isLike(long uid, long oid) {
        int result = pccService.isLike(uid, oid) ? 1 : 0;
        return Response.of("is_like", uid, oid, result);
    }

    @RequestMapping(path = "/pcc/count")
    @Timed
    public Response<Long> count(long oid) {
        return Response.of("count", 0, oid, pccService.count(oid));
    }

    @RequestMapping(path = "/pcc/list")
    @Timed
    public Response<List<User>> list(long oid, int pageSize, long cursor) {
        List<User> users = pccService.getUsers(pccService.list(oid, pageSize, cursor));
        long newCursor = 0;
        if (users != null && users.size() > 0) {
            newCursor = users.get(users.size() - 1).getUid();
        }
        return Response.of("list", 0, oid, newCursor, users);
    }

    @RequestMapping(path = "/pcc/list_friend")
    @Timed
    public Response<List<User>> listFriend(long oid, int pageSize, long uid, long cursor) {
        List<User> users = pccService.getUsers(pccService.listFriend(oid, pageSize, uid, cursor));
        long newCursor = 0;
        if (users != null && users.size() > 0) {
            newCursor = users.get(users.size() - 1).getUid();
        }
        return Response.of("list_friend", uid, oid, newCursor, users);
    }

    @Timed
    @RequestMapping(path = "/pcc/add_user")
    public Response<User> addUser(@RequestParam(value = "uid") long uid,
                                  @RequestParam(value = "nickname") String nickname) {
        return Response.of("add_user", uid, 0, pccService.addUser(uid, nickname));
    }

    @Timed
    @RequestMapping(path = "/pcc/add_friend")
    public Response<List<User>> addFriend(@RequestParam(value = "uid") long uid,
                                          @RequestParam(value = "friend_uid") long friendUid) {
        pccService.addFriend(uid, friendUid);
        List<User> friends = pccService.getUsers(pccService.getFriend(uid));

        return Response.of("add_user", uid, 0, friends);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(100);

    @RequestMapping(path = "/pcc/load")
    public Response<Long> loadData(@RequestParam(value = "file_path") String filePath,
                                   @RequestParam(value = "type") String type) {
        long start = System.currentTimeMillis();

        final AtomicLong lineNo = new AtomicLong();
        boolean succ = true;

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach((line) -> {
                if (lineNo.incrementAndGet() % 10000 == 0) {
                    logger.info("load data for file {}, type {}, lineNo {}",
                            filePath, type, lineNo.get());
                }

                if (type.equalsIgnoreCase("user")) {
                    int p = line.indexOf(',');
                    if (p < 0) {
                        logger.warn("{} line has no , {}", type, line);
                        return;
                    }
                    long uid = Long.parseLong(line.substring(0, p));
                    String nickname = line.substring(p + 1);
                    pccService.addUser(uid, nickname);
                } else if (type.equalsIgnoreCase("friends")) {
                    int p = line.indexOf(',');
                    if (p < 0) {
                        logger.warn("{} line has no , {}", type, line);
                        return;
                    }
                    long uid = Long.parseLong(line.substring(0, p));
                    long friendUid = Long.parseLong(line.substring(p + 1));

                    pccService.addFriend(uid, friendUid);
                } else if (type.equalsIgnoreCase("like")) {
                    int p = line.indexOf(':');
                    if (p < 0) {
                        logger.warn("{} line has no : {}", type, line);
                        return;
                    }
                    long oid = Long.parseLong(line.substring(0, p));
                    String uidStr = line.substring(p + 1);
                    String[] parts = uidStr.split(",");
                    long[] uids = new long[parts.length];

                    for (int i = 0; i < parts.length; ++i) {
                        String part = parts[i];
                        if (part.contains("[")) {
                            part = part.replace("[", "");
                        }
                        if (part.contains("]")) {
                            part = part.replace("]", "");
                        }
                        if (part.trim().length() <= 0) {
                            continue;
                        }
                        long uid = Long.parseLong(part);
                        uids[i] = uid;
                    }

                    executorService.submit(() -> {
                        pccService.addMultiLike(uids, oid);
                    });

                } else {
                    throw new IllegalArgumentException("type error");
                }
            });
        } catch (Exception e) {
            logger.error("load data error for file {}, type {}", filePath, type, e);
            succ = false;
        }

        long cost = System.currentTimeMillis() - start;
        return Response.of("load", succ ? 0 : -1, 0, cost);
    }

}
