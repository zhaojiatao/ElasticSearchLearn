package com.zjt.learn.elasticsearch.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * 功能：
 *
 * @Author zhaojiatao
 * @Date 2021-05-18 23:00
 */

public class EsClient {

    public static RestHighLevelClient getClient(){
        HttpHost httpHost=new HttpHost("127.0.0.1",9200);
        RestClientBuilder clientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient client=new RestHighLevelClient(clientBuilder);
        return client;
    }

}
