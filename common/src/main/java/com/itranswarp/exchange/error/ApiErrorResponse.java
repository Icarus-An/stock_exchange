package com.itranswarp.exchange.error;


import com.itranswarp.exchange.enums.ApiError;

public record ApiErrorResponse(ApiError error, String data, String message) {

}
