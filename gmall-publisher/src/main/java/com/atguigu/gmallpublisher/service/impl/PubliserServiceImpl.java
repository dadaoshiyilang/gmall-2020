package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstant;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PubliserServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        Map dauHourMap = new HashMap();
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);

        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"), map.get("CT"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
        Map orderAmountHourMap = new HashMap();
        for (Map map : mapList) {
            orderAmountHourMap.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;
    }

    @Override
    public Map getSaleDetail(String date, int startPage, int size, String keyWord) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //添加查询过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //添加聚合组
        TermsBuilder ageAggs = AggregationBuilders.terms("countByAge").field("user_age").size(100);
        TermsBuilder genderAggs = AggregationBuilders.terms("countByGender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(ageAggs);
        searchSourceBuilder.aggregation(genderAggs);

        //分页
        searchSourceBuilder.from((startPage - 1) * size);
        searchSourceBuilder.size(size);

        //2.构建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.GMALL_SALE_DETAIL_SEARCH).addType("_doc").build();

        SearchResult searchResult = null;

        //3.执行查询
        try {
            searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //4.解析结果
        assert searchResult != null;

        //获取总数
        Long total = searchResult.getTotal();

        //获取明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        ArrayList<Map> details = new ArrayList<>();
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        //获取聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation countByAge = aggregations.getTermsAggregation("countByAge");
        HashMap<String, Long> ageMap = new HashMap<>();
        for (TermsAggregation.Entry entry : countByAge.getBuckets()) {
            ageMap.put(entry.getKey(), entry.getCount());
        }
        TermsAggregation countByGender = aggregations.getTermsAggregation("countByGender");
        HashMap<String, Long> genderMap = new HashMap<>();
        for (TermsAggregation.Entry entry : countByGender.getBuckets()) {
            genderMap.put(entry.getKey(), entry.getCount());
        }

        //5.创建Map用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("detail", details);
        result.put("age", ageMap);
        result.put("gender", genderMap);

        return result;
    }
}
