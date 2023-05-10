package com.itranswarp.exchange.assets;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Asset {
    BigDecimal available;
    // 冻结余额:
    BigDecimal frozen;

    public Asset() {
        this(BigDecimal.ZERO, BigDecimal.ZERO);
    }

    public Asset(BigDecimal available, BigDecimal frozen) {
        this.available = available;
        this.frozen = frozen;
    }
}
