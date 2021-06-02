package com.zjt.learn.elasticsearch.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import sun.reflect.generics.tree.VoidDescriptor;

import javax.lang.model.element.VariableElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    /**
     * 根据id查询
     * @throws IOException
     */
    @Test
    public void findById() throws IOException{
        //1、创建GetRequest
        GetRequest request=new GetRequest(index,type,"1");
        //2、执行查询
        GetResponse resp=client.get(request,RequestOptions.DEFAULT);
        //3、输出结果
        System.out.println(resp.getSourceAsMap());

    }

    /**
     * 根据ids查询
     * @throws IOException
     */
    @Test
    public void findByIds() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.idsQuery().addIds("1","2","3"));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }


    /**
     * 根据前缀查询
     * @throws IOException
     */
    @Test
    public void findByPrefix() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.prefixQuery("corpName","盒马"));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }

    /**
     * Fuzzy模糊查询
     * @throws IOException
     */
    @Test
    public void findByFuzzy() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.fuzzyQuery("corpName","盒马").prefixLength(2));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }

    /**
     * wildCard查询
     * @throws IOException
     */
    @Test
    public void findByWildCard() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.wildcardQuery("corpName","中国*"));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }


    }


    /**
     * 范围查询
     * @throws IOException
     */
    @Test
    public void findByRange() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.rangeQuery("fee").lte(10).gte(5));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }


    }




    /**
     * 正则查询
     * @throws IOException
     */
    @Test
    public void findByRegexp() throws IOException{
        //1、创建Request
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.regexpQuery("mobile","139[0-9]{8}"));
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、获取到_source的数据，并展示
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }


    }

    /**
     * scroll分页
     * @throws IOException
     */
    @Test
    public void scrollQuery() throws IOException{
        //1、创建SearchRequest
        SearchRequest request=new SearchRequest(index);
        request.types(type);
        //2、指定scroll信息
        request.scroll(TimeValue.timeValueMinutes(1L));
        //3、指定查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(4);
        builder.sort("fee", SortOrder.DESC);
        builder.query(QueryBuilders.matchAllQuery());

        request.source(builder);
        //4、获取返回结果scrollId，source
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        String scrollId=resp.getScrollId();
        System.out.println("---------首页---------");
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

        while (true){
            //5、循环-创建SearchScrollRequest
            SearchScrollRequest scrollRequest=new SearchScrollRequest(scrollId);
            //6、指定scrollId的生存时间
            scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
            //7、执行查询获取返回结果
            SearchResponse scrollResp=client.scroll(scrollRequest,RequestOptions.DEFAULT);
            //8、判断是否查询到了数据，数据
            SearchHit[] hits = scrollResp.getHits().getHits();
            if(hits != null && hits.length>0){
                System.out.println("------------下一页------------");
                for(SearchHit hit:hits){
                    System.out.println(hit.getSourceAsMap());
                }
            }else{
                //9、判断没有查询到数据-退出循环
                System.out.println("-------------结束---------------");
                break;
            }

        }

        //10、创建ClearScrollRequest
        ClearScrollRequest clearScrollRequest=new ClearScrollRequest();
        //11、指定ScrollId
        clearScrollRequest.addScrollId(scrollId);
        //12、删除ScrollId
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        //13、输出结果
        System.out.println("删除scroll："+clearScrollResponse.isSucceeded());

    }

    /**
     *
     * @throws IOException
     */
    @Test
    public void deleteByQuery() throws IOException{
        //1、创建DeleteByQueryRequest
        DeleteByQueryRequest request=new DeleteByQueryRequest(index);
        request.types(type);
        //2、指定检索的条件和SearchRequest指定Query的方式不一样
        request.setQuery(QueryBuilders.rangeQuery("fee").lt(4));
        //3、执行删除
        BulkByScrollResponse resp=client.deleteByQuery(request,RequestOptions.DEFAULT);
        //4、输出返回结果
        System.out.println(resp.toString());
    }

    /**
     * bool查询
     * @throws IOException
     */
    @Test
    public void BoolQuery() throws IOException{
        //1、创建SearchRequest
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        BoolQueryBuilder boolQuery=QueryBuilders.boolQuery();
        // #查询省份为武汉或者北京
        boolQuery.should(QueryBuilders.termQuery("province","武汉"));
        boolQuery.should(QueryBuilders.termQuery("province","北京"));
        //#运营商不是联通
        boolQuery.mustNot(QueryBuilders.termQuery("operatorId",2));
        //#smsContent中包含中国或平安
        boolQuery.must(QueryBuilders.matchQuery("smsContent","中国"));
        boolQuery.must(QueryBuilders.matchQuery("smsContent","平安"));

        builder.query(boolQuery);
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、输出结果
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }

    /**
     *
     * @throws IOException
     */
    @Test
    public void BoostingQuery() throws IOException{
        //1、创建SearchRequest
        SearchRequest request=new SearchRequest(index);
        request.types(type);
        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        BoostingQueryBuilder boostingQuery=QueryBuilders.boostingQuery(
          QueryBuilders.matchQuery("smsContent","收货安装"),
          QueryBuilders.matchQuery("smsContent","王五")
        ).negativeBoost(0.5f);
        builder.query(boostingQuery);
        request.source(builder);

        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、输出结果
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }


    }

    /**
     *
     * @throws IOException
     */
    @Test
    public void filter() throws IOException {
        //1、创建SearchRequest
        SearchRequest request=new SearchRequest(index);
        request.types(type);
        //2、指定查询条件
        SearchSourceBuilder builder=new SearchSourceBuilder();
        BoolQueryBuilder boolQuery=QueryBuilders.boolQuery();
        boolQuery.filter(QueryBuilders.termQuery("corpName","盒马鲜生"));
        boolQuery.filter(QueryBuilders.rangeQuery("fee").lte(5));

        builder.query(boolQuery);
        request.source(builder);
        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、输出结果
        for(SearchHit hit:resp.getHits().getHits()){
            Map<String,Object> result=hit.getSourceAsMap();
            System.out.println(result);
        }

    }

    /**
     * 高亮查询
     * @throws IOException
     */
    @Test
    public void highLightQuery() throws IOException {
        //1、创建SearchRequest
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定查询条件(高亮)
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("smsContent","盒马"));

        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.field("smsContent",10)
                .preTags("<font color='red'>")
                .postTags("</font>");
        builder.highlighter(highlightBuilder);

        request.source(builder);
        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、输出结果
        for(SearchHit hit:resp.getHits().getHits()){
            System.out.println(hit.getHighlightFields().get("smsContent"));
        }

    }




    @Test
    public void cardinality() throws IOException {
        //1、创建SearchRequest
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        //2、指定使用的聚合查询方式
        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.cardinality("agg").field("province"));
        request.source(builder);
        //3、执行查询
        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        //4、输出结果
        Cardinality agg = resp.getAggregations().get("agg");
        long value = agg.getValue();
        System.out.println(value);

    }

    /**
     * java实现数值 范围统计
     * @throws IOException
     */
    @Test
    public void range() throws IOException{
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.range("agg").field("fee")
                .addUnboundedTo(5)
                .addRange(5,10)
                .addUnboundedFrom(10)
        );
        request.source(builder);

        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        Range agg=resp.getAggregations().get("agg");
        for (Range.Bucket bucket:agg.getBuckets()){
            String key = bucket.getKeyAsString();
            Object from = bucket.getFrom();
            Object to = bucket.getTo();
            long docCount = bucket.getDocCount();
            System.out.println(String.format("key:%s,from:%s,to:%s,docCount:%s",key,from,to,docCount));
        }


    }

    /**
     * 统计聚合查询
     * @throws IOException
     */
    @Test
    public void extendedStats() throws IOException{
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        SearchSourceBuilder builder=new SearchSourceBuilder();
        builder.aggregation(AggregationBuilders.extendedStats("agg").field("fee"));
        request.source(builder);

        SearchResponse resp=client.search(request,RequestOptions.DEFAULT);

        ExtendedStats agg=resp.getAggregations().get("agg");
        double max = agg.getMax();
        double min = agg.getMin();
        System.out.println("fee的最大值为："+max+",最小值为："+min);
    }

    /**
     * 基于Java实现坐标查询
     * @throws IOException
     */
    @Test
    public void geoPolygon() throws IOException{
        SearchRequest request=new SearchRequest(index);
        request.types(type);

        SearchSourceBuilder builder=new SearchSourceBuilder();
        List<GeoPoint> points=new ArrayList<>();
        points.add(new GeoPoint(39.99878,116.298916));
        points.add(new GeoPoint(39.972576,116.29561));
        points.add(new GeoPoint(39.984739,116.327661));
        builder.query(QueryBuilders.geoPolygonQuery("location",points));
        request.source(builder);

        SearchResponse resp = client.search(request, RequestOptions.DEFAULT);

        for(SearchHit hit:resp.getHits().getHits()){
            System.out.println(hit.getSourceAsMap());
        }

    }



}
