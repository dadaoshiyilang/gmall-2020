package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauHours(String date);

    //查询GMV总额
    public Double getOrderAmount(String date);

    //查询GMV分时统计
    public Map getOrderAmountHour(String date);

    public Map getSaleDetail(String date, int startPage, int size, String keyWord);

}
