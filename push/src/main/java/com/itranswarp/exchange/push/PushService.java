package com.itranswarp.exchange.push;


import jakarta.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.itranswarp.exchange.redis.RedisCache;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;
import io.vertx.redis.client.impl.types.BulkType;

@Slf4j
@Component
public class PushService {

    @Value("${server.port}")
    private int serverPort;

    @Value("${exchange.config.hmac-key}")
    String hmacKey;

    @Value("${spring.redis.standalone.host:localhost}")
    private String redisHost;

    @Value("${spring.redis.standalone.port:6379}")
    private int redisPort;

    @Value("${spring.redis.standalone.password:}")
    private String redisPassword;

    @Value("${spring.redis.standalone.database:0}")
    private int redisDatabase = 0;

    private Vertx vertx;

    @PostConstruct
    public void startVertx() {
        log.info("start vertx...");
        this.vertx = Vertx.vertx();

        var push = new PushVerticle(this.hmacKey, this.serverPort);
        vertx.deployVerticle(push);

        String url = "redis://" + (this.redisPassword.isEmpty() ? "" : ":" + this.redisPassword + "@") + this.redisHost
                + ":" + this.redisPort + "/" + this.redisDatabase;

        log.info("create redis client: {}", url);
        Redis redis = Redis.createClient(vertx, url);

        redis.connect().onSuccess(conn -> {
            log.info("connect to redis ok.");
            conn.handler(response -> {
                if (response.type() == ResponseType.PUSH) {
                    int size = response.size();
                    if (size == 3) {
                        Response type = response.get(2);
                        if (type instanceof BulkType) {
                            String msg = type.toString();
                            if (log.isDebugEnabled()) {
                                log.debug("receive push message: {}", msg);
                            }
                            push.broadcast(msg);
                        }
                    }
                }
            });
            log.info("try subscribe...");
            conn.send(Request.cmd(Command.SUBSCRIBE).arg(RedisCache.Topic.NOTIFICATION)).onSuccess(resp -> {
                log.info("subscribe ok.");
            }).onFailure(err -> {
                log.error("subscribe failed.", err);
                System.exit(1);
            });
        }).onFailure(err -> {
            log.error("connect to redis failed.", err);
            System.exit(1);
        });
    }

    void exit(int exitCode) {
        this.vertx.close();
        System.exit(exitCode);
    }
}