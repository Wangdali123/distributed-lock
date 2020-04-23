package com.wdl.redis.simple;


import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * <p>Title: RedisLock.class</p>
 * <p>Description: Class Description</p>
 * <p>Copyright: Copyright (c) 2019</p>
 *
 * @author wangdali
 * @version 1.0
 * @date 2020/4/21 21:52
 */
public class RedisLock {

    private String lockKey;

    private String lockVal;

    private Long timeout;

    private RedisTemplate<String, Object> redisTemplate;

    private static String unlockCommand = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    /**
     * 需要指定lua脚本的返回类型，只支持org.springframework.data.redis.connection.ReturnType内少量类型
     */
    private static RedisScript<Long> redisScript = new DefaultRedisScript<>(unlockCommand, Long.class);

    public RedisLock(String lockKey, String lockVal, Long timeout, RedisTemplate<String, Object> redisTemplate) {
        this.lockKey = lockKey;
        this.lockVal = lockVal;
        this.timeout = timeout;
        this.redisTemplate = redisTemplate;

    }

    public Boolean lock() {
        return redisTemplate.opsForValue().setIfAbsent(lockKey, lockVal, timeout, TimeUnit.SECONDS);
    }

    public Boolean unlock() {
        Long result = redisTemplate.execute(redisScript, Collections.singletonList(lockKey), lockVal);
        return Objects.equals(result, 1L);
    }
}
