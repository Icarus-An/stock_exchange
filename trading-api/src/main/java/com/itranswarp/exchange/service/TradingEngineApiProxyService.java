package com.itranswarp.exchange.service;


import com.itranswarp.exchange.enums.ApiError;
import com.itranswarp.exchange.error.ApiException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Proxy to access trading engine.
 */
@Slf4j
@Component
public class TradingEngineApiProxyService {

    @Value("#{exchangeConfiguration.apiEndpoints.tradingEngineApi}")
    private String tradingEngineInternalApiEndpoint;

    private OkHttpClient okhttpClient = new OkHttpClient.Builder()
            // set connect timeout:
            .connectTimeout(1, TimeUnit.SECONDS)
            // set read timeout:
            .readTimeout(1, TimeUnit.SECONDS)
            // set connection pool:
            .connectionPool(new ConnectionPool(20, 60, TimeUnit.SECONDS))
            // do not retry:
            .retryOnConnectionFailure(false).build();

    public String get(String url) throws IOException {
        Request request = new Request.Builder().url(tradingEngineInternalApiEndpoint + url).header("Accept", "*/*")
                .build();
        try (Response response = okhttpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                log.error("Internal api failed with code {}: {}", Integer.valueOf(response.code()), url);
                throw new ApiException(ApiError.OPERATION_TIMEOUT, null, "operation timeout.");
            }
            try (ResponseBody body = response.body()) {
                String json = body.string();
                if (json == null || json.isEmpty()) {
                    log.error("Internal api failed with code 200 but empty response: {}", json);
                    throw new ApiException(ApiError.INTERNAL_SERVER_ERROR, null, "response is empty.");
                }
                return json;
            }
        }
    }
}