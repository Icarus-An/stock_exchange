package com.itranswarp.exchange.support;


import jakarta.servlet.Filter;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("init filter: {}...", getClass().getName());
    }

    @Override
    public void destroy() {
        log.info("destroy filter: {}...", getClass().getName());
    }
}