package com.zjt.learn.elasticsearch.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * 功能：
 *
 * @Author zhaojiatao
 * @Date 2021-05-19 22:27
 */

public class QueryDemo {
    ObjectMapper mapper=new ObjectMapper();
    RestHighLevelClient client=EsClient.getClient();
    String index="person";
    String type="man";

    /**
     * term查询
     * @throws IOException
     */
    @Test
    public void termQuery() throws IOException{
        //1、创建Request对象
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.from(0);
        builder.size(5);
        builder.query(QueryBuilders.termQuery("province","北京"));

        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request, RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }




    /**
     * terms查询
     * @throws IOException
     */
    @Test
    public void termsQuery() throws IOException{
        //1、创建Request对象
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.termsQuery("province","北京","山西"));

        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request, RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }

    @Test
    public void matchAllQuery() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        //ES默认只查询10条数据，如果想查询更多，添加size
        builder.size(20);
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }
        System.out.println(resp.getHits().getHits().length);

    }

    @Test
    public void matchQuery() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("smsContent","收货安装"));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }

    @Test
    public void booleanMatchQuery() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("smsContent","中国 健康")
                .operator(Operator.OR)
        );
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }


    }

    @Test
    public void multiMatchQuery() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.multiMatchQuery("北京","province","smsContent"));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }
    }











}
