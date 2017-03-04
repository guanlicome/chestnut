package org.fulin;

import org.fulin.chestnut.PccService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ChestnutApplicationTests {

    static {
        try {
            PccService.prepareForTest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Before
    public void cleanOldData() throws IOException {

    }

    //@Test
    public void contextLoads() {
    }

    @Before
    public void prepareUser() {
        String ret = restTemplate.getForObject("/pcc/add_user?uid=20001&nickname=tangfl", String.class);
        ret = restTemplate.getForObject("/pcc/add_user?uid=20002&nickname=tim", String.class);
        ret = restTemplate.getForObject("/pcc/add_user?uid=20003&nickname=wang", String.class);
        ret = restTemplate.getForObject("/pcc/add_user?uid=20004&nickname=yang", String.class);
        System.out.println("add_user: " + ret);
        ret = restTemplate.getForObject("/pcc/add_friend?uid=20001&friend_uid=20002", String.class);
        ret = restTemplate.getForObject("/pcc/add_friend?uid=20001&friend_uid=20003", String.class);
        System.out.println("add_friend: " + ret);
    }

    @Test
    public void testLike() {
        String ret = restTemplate.getForObject("/pcc?action=like&uid=20001&oid=10001", String.class);
        ret = restTemplate.getForObject("/pcc?action=like&uid=20004&oid=10001", String.class);
        ret = restTemplate.getForObject("/pcc?action=like&uid=20002&oid=10001", String.class);
        ret = restTemplate.getForObject("/pcc?action=like&uid=20003&oid=10001", String.class);
        System.out.println("like: " + ret);
        ret = restTemplate.getForObject("/pcc?action=like&uid=20003&oid=10001", String.class);
        System.out.println("already liked: " + ret);
        ret = restTemplate.getForObject("/pcc?action=is_like&uid=20001&oid=10001", String.class);
        System.out.println("is_like(1): " + ret);
        ret = restTemplate.getForObject("/pcc?action=is_like&uid=21001&oid=10001", String.class);
        System.out.println("is_like(0): " + ret);
        ret = restTemplate.getForObject("/pcc?action=count&oid=10001", String.class);
        System.out.println("count(4): " + ret);
        ret = restTemplate.getForObject("/pcc?action=list&oid=10001&page_size=2&is_friend=0", String.class);
        System.out.println("list(20001,20004): " + ret);
        ret = restTemplate.getForObject("/pcc?action=list&oid=10001&cursor=20004&page_size=2", String.class);
        System.out.println("list with cursor(20002,20003): " + ret);
        //ret = restTemplate.getForObject("/pcc?action=list&oid=10001&uid=20001&page_size=2&is_friend=1", String.class);
        //System.out.println("list with friend(20002,20003): " + ret);
        ret = restTemplate.getForObject("/pcc?action=list&oid=10001&uid=20001&cursor=20002&page_size=2&is_friend=1", String.class);
        System.out.println("list with cursor and friend(20003): " + ret);
    }

    @Test
    public void pressLike() {
        pressLike(100, null);
        pressLike(200, "like");
    }

    public void pressLike(long times, String action) {
        long begin = System.currentTimeMillis();
        String url = "/pcc?action=press";
        if (action != null && action.length() > 0) {
            url = url + "_" + action;
        }
        for (long i = 0; i < times; i++) {
            restTemplate.getForObject(url, String.class);

        }
        long cost = System.currentTimeMillis() - begin;
        System.out.println("action:" + action + ", press " + times + " times, " +
                "cost time: " + cost);
    }

    @Test
    public void testLoadData() {
        String home = System.getProperty("user.dir");
        String ret;

        ret = restTemplate.getForObject("/pcc/load?type=user&file_path=" + home + "/src/test/resources/user.csv", String.class);
        System.out.println("load user: " + ret);

        ret = restTemplate.getForObject("/pcc/load?type=friends&file_path=" + home + "/src/test/resources/friends.csv", String.class);
        System.out.println("load friends: " + ret);

        ret = restTemplate.getForObject("/pcc/load?type=like&file_path=" + home + "/src/test/resources/like.csv", String.class);
        System.out.println("load like: " + ret);
        
    }

}
