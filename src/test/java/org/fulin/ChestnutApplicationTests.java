package org.fulin;

import org.fulin.chestnut.ListMapService;
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

    @Autowired
    private TestRestTemplate restTemplate;

    @Before
    public void cleanOldData() throws IOException {
        ListMapService.cleanData();
    }

    @Test
    public void contextLoads() {
    }

    @Test
    public void testLike() {
        String ret = restTemplate.getForObject("/pcc?action=like&uid=20001&oid=10001", String.class);
        ret = restTemplate.getForObject("/pcc?action=like&uid=20002&oid=10001", String.class);
        ret = restTemplate.getForObject("/pcc?action=like&uid=20003&oid=10001", String.class);
        System.out.println("like: " + ret);
        ret = restTemplate.getForObject("/pcc?action=is_like&uid=20001&oid=10001", String.class);
        System.out.println("is_like(1): " + ret);
        ret = restTemplate.getForObject("/pcc?action=is_like&uid=21001&oid=10001", String.class);
        System.out.println("is_like(0): " + ret);
        ret = restTemplate.getForObject("/pcc?action=count&oid=10001", String.class);
        System.out.println("count(3): " + ret);
        ret = restTemplate.getForObject("/pcc?action=list&oid=10001&page_size=10", String.class);
        System.out.println("list: " + ret);
    }

    @Test
    public void loadLikeData() {
        long begin = System.currentTimeMillis();
        for (long uid = 20001; uid < 20010; uid++) {
            for (long oid = 10001; oid < 10020; oid++) {
                restTemplate.getForObject("/pcc?action=like&uid=" + uid + "&oid=" + oid, String.class);
            }
        }
        long cost = System.currentTimeMillis() - begin;
        System.out.println("cost time: " + cost);

        String ret;
        long uid = 20002;
        long oid = 10003;
        ret = restTemplate.getForObject("/pcc?action=is_like&uid=" + uid + "&oid=" + oid, String.class);
        System.out.println("is_like: " + ret);
    }

}
