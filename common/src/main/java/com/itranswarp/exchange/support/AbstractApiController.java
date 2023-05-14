package com.itranswarp.exchange.support;

import com.itranswarp.exchange.enums.ApiError;
import com.itranswarp.exchange.error.ApiErrorResponse;
import com.itranswarp.exchange.error.ApiException;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;


public abstract class AbstractApiController {

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(ApiException.class)
    @ResponseBody
    public ApiErrorResponse handleException(HttpServletResponse response, Exception ex) throws Exception {
        response.setContentType("application/json;charset=utf-8");
        ApiException apiEx = null;
        if (ex instanceof ApiException) {
            apiEx = (ApiException) ex;
        } else {
            apiEx = new ApiException(ApiError.INTERNAL_SERVER_ERROR, null, ex.getMessage());
        }
        return apiEx.error;
    }
}