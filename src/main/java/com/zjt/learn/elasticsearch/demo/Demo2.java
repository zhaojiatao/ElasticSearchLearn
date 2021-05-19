package com.zjt.learn.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;

/**
 * 功能：
 *
 * @Author zhaojiatao
 * @Date 2021-05-19 08:53
 */

public class Demo2 {
    RestHighLevelClient client=EsClient.getClient();
    String index="person";
    String type="man";

    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    public void createIndex() throws IOException{
        //1、准备关于索引的settings
        Settings.Builder settings=Settings.builder()
                .put("number_of_shards",3)
                .put("number_of_replicas",1);

        //2、准备关于索引的结构mappings
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("name")
                            .field("type","text")
                        .endObject()
                        .startObject("age")
                            .field("type","integer")
                        .endObject()
                        .startObject("birthday")
                            .field("type","date")
                            .field("format","yyyy-MM-dd")
                        .endObject()
                    .endObject()
                .endObject();

        //3、将settings和mappings封装到一个Request对象
        CreateIndexRequest request=new CreateIndexRequest(index)
                .settings(settings)
                .mapping(type,mappings);

        //4、通过client对象去连接ES并执行创建索引
        CreateIndexResponse resp=client.indices().create(request, RequestOptions.DEFAULT);

        //5、输出
        System.out.println("resp："+ JSON.toJSONString(resp));
    }

    /**
     * 检查索引是否存在
     * @throws IOException
     */
    @Test
    public void exists() throws IOException{
        //1、准备request对象
        GetIndexRequest request=new GetIndexRequest();
        request.indices(index);
        //2、通过client去操作
        boolean exists=client.indices().exists(request,RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引对象
     * @throws IOException
     */
    @Test
    public void delete() throws IOException{
        DeleteIndexRequest request=new DeleteIndexRequest();
        request.indices(index);
        AcknowledgedResponse delete=client.indices().delete(request,RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }





}
