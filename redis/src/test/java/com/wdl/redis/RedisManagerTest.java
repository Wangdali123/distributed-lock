package com.wdl.redis;

import com.wdl.redis.simple.RedisLock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.BoundZSetOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * <p>Title: RedisManagerTest</p>
 * <p>Description: redis 测试类</p>
 * <p>Copyright: Copyright (c) 2019</p>
 * <p>Company: sodo</p>
 *
 * @author wangdali
 * @version 1.0
 * @date 2019/5/8 23:55
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisManagerTest {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Test
    public void redisTemplate(){
        redisTemplate.opsForValue().setIfAbsent("LOCK", "UUID", 1000, TimeUnit.SECONDS);
        redisTemplate.opsForValue().set("SK", "StringKey"); //key value
        redisTemplate.opsForList().rightPush("UserList", "WDL001"); //list 尾插
        redisTemplate.opsForSet(); //Set
        redisTemplate.opsForZSet(); //排序Set
        redisTemplate.opsForHash().put("UserMap", "name", "整个牛皮啊"); //map
    }

    /**
     * 简单限流（滑动窗口）
     * 原理：通过ZSet 统计一段时间内用户访问成功次数，每次删除一段时间外的数据 避免堆积并设置key的过期时间
     */
    @Test
    public void currentLimiting() {
        int userId = 1002;
        String key = "current_" + userId;
        // boundZSetOps 和 ops 区别： ops后续操作需要指定key
        final BoundZSetOperations zSetOperations = redisTemplate.boundZSetOps(key);
        IntStream.range(0, 100).forEach(i -> {
            long val = System.currentTimeMillis();
            // 限制10s内访问次数不能大于10
            zSetOperations.removeRangeByScore(0, val - 10 * 1000);
            if (zSetOperations.zCard() < 10) {
                zSetOperations.add("temp_" + i, val);
                System.out.println("temp" + i + " 访问成功");
            } else {
                System.out.println("temp" + i + " 超过限流次数，暂停1S 进行下一次访问");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            redisTemplate.expire(key, 10, TimeUnit.SECONDS);
        });
    }

    static String USER_SUBMIT = "USER_SUBMIT";

    @Test
    public void simpleDistinct() {
        long userId = 1000L;
        String key = USER_SUBMIT + LocalDate.now().toString();
        // 判断是否存在，不存在则添加元素后设置超时时间
        if (!redisTemplate.hasKey(key)) {
            redisTemplate.opsForSet().add(key, userId);
            redisTemplate.expireAt(key, new Date());
        } else {
            redisTemplate.opsForSet().add(key, userId);
        }
    }

    @Test
    public void dcsRedisLock() {
        String lockKey = "DCS_REDIS_LOCK_KEY";
        long timeOut = 20;
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(20, 20, 1000, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        IntStream.range(1, 10).forEach(i -> {
            threadPoolExecutor.execute(()->{
                System.out.println("【线程】 " + i + " 开始执行");
                RedisLock redisLock = new RedisLock(lockKey, "UUID:" + i, timeOut, redisTemplate);
                while (!redisLock.lock()) {
                    System.out.println("【线程】 " + i + " 休眠等待锁");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("【线程】 " + i + " 获取到锁资源");
                redisLock.unlock();
                System.out.println("【线程】 " + i + " 释放锁资源 线程结束");
            });
        });
        try {
            threadPoolExecutor.shutdown();
            threadPoolExecutor.awaitTermination(10, TimeUnit.MINUTES); // 等待线程池任务全部执行完毕
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
