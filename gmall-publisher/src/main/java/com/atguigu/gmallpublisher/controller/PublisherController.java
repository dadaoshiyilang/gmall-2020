package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeHourDate(@RequestParam("date") String date) {
        List<Map> list = new ArrayList<Map>();

        // 日活总数
        int dauTotal = publisherService.getDauTotal(date);
        Map dauMap = new HashMap<String,Object>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        list.add(dauMap);

        // 新增用户
        Map newMidMap = new HashMap<String,Object>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",233);
        list.add(newMidMap);

        // 创建GMV
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getOrderAmount(date));
        list.add(gmvMap);

        return JSON.toJSONString(list);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHour(@RequestParam("id") String id, @RequestParam("date") String date) {

        HashMap<String, Map> result = new HashMap<>();

        String yesterday = getYesterdayString(date);
        Map todayMap = null;
        Map yesterdayMap = null;

        //4.根据访问地址中的id参数,选择返回不同的数据集
        if ("dau".equals(id)) {
            //查询今日数据
            todayMap = publisherService.getDauHours(date);
            //查询昨日数据
            yesterdayMap = publisherService.getDauHours(yesterday);
        } else if ("order_amount".equals(id)) {
            //查询今日数据
            todayMap = publisherService.getOrderAmountHour(date);
            //查询昨日数据
            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }

        //5.将查询结果放入result
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //6.将result转换为JSON串返回
        return JSONObject.toJSONString(result);
    }


    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int startpage, @RequestParam("size") int size, @RequestParam("keyword") String keyword) {

        //1.查询ES中的数据
        Map saleDetailMap = publisherService.getSaleDetail(date, startpage, size, keyword);

        //2.创建Map用于存放返回给用户的结果数据
        HashMap<String, Object> result = new HashMap<>();

        //3.取出saleDetailMap中的数据
        Long total = (Long) saleDetailMap.get("total");
        List detail = (List) saleDetailMap.get("detail");
        Map ageMap = (Map) saleDetailMap.get("age");
        Map genderMap = (Map) saleDetailMap.get("gender");

        //4.将ageMap转换为饼图所需的数据
        Long lower20 = 0L;
        Long start20to30 = 0L;

        for (Object o : ageMap.keySet()) {
            int age = Integer.parseInt((String) o);
            if (age < 20) {
                lower20 += (Long) ageMap.get(o);
            } else if (age < 30) {
                start20to30 += (Long) ageMap.get(o);
            }
        }

        double lower20Ratio = Math.round(lower20 * 1000 / total) / 10D;
        double start20to30Ratio = Math.round(start20to30 * 1000 / total) / 10D;
        double upper30Ratio = 100D - lower20Ratio - start20to30Ratio;

        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option start20to30Opt = new Option("20岁到30岁", start20to30Ratio);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(start20to30Opt);
        ageOptions.add(upper30Opt);

        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        //5.将genderMap转换为饼图所需的数据
        Long femaleCount = (Long) genderMap.get("F");
        double femaleRatio = Math.round(femaleCount * 1000 / total) / 10D;
        double maleRatio = 100D - femaleRatio;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);
        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        Stat genderStat = new Stat(genderList, "用户性别占比");

        //6.创建集合用于存放两张饼图所需的数据
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //7.将准备好的数据存放至结果Map
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        //8.将结果Map转换为JSON串返回
        return JSONObject.toJSONString(result);
    }

    //获取昨天日期
    private String getYesterdayString(String date) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            calendar.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        return sdf.format(calendar.getTime());
    }
}