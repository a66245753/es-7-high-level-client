package com.david.coltroller;

import com.david.dto.request.EsRangeParams;
import com.david.dto.request.EsSearchRequest;
import com.david.dto.response.EsSearchResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/es-doc")
public class EsDocController {

    @Autowired
    private RestHighLevelClient restHighLevelClient;
    private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * id查询
     *
     * @return
     * @throws IOException
     */
    @PostMapping("/get")
    public ResponseEntity<Object> get() throws IOException {
        GetRequest getRequest = new GetRequest("ssp_ad_union_log_202403");
        getRequest.id("fcBZZZUBJ3krEJ13KbOG");
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        return new ResponseEntity<>(response.getSourceAsMap(), HttpStatus.OK);
    }

    /**
     * ids查询
     *
     * @return
     * @throws IOException
     */
    @PostMapping("/getBatch")
    public ResponseEntity<Object> getBatch() throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add("ssp_ad_union_log_202403","fcBZZZUBJ3krEJ13KbOG");
        multiGetRequest.add("ssp_ad_union_log_202403","VsBZZZUBJ3krEJ13KbOG");
        MultiGetResponse multiGetItemResponses = restHighLevelClient.multiGet(multiGetRequest, RequestOptions.DEFAULT);
        return new ResponseEntity<>(multiGetItemResponses.getResponses(), HttpStatus.OK);
    }

    /**
     * 分页查询
     *
     * @param request
     * @return
     * @throws IOException
     */
    @PostMapping("/search")
    public ResponseEntity<EsSearchResult> search(@RequestBody EsSearchRequest request) throws IOException {
        SearchRequest searchRequest = new SearchRequest(request.getIndex());
        // 构建搜索请求
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.trackTotalHits(true);
        sourceBuilder.from((request.getPageIndex() - 1) * request.getPageSize());
        sourceBuilder.size(request.getPageSize());
        searchRequest.source(sourceBuilder);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (request.getEqualsParams() != null && !request.getEqualsParams().isEmpty()) {
            for (Map.Entry<String, Object> entry : request.getEqualsParams().entrySet()) {
                queryBuilder.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
            }
        }
        if (request.getLikeParams() != null && !request.getLikeParams().isEmpty()) {
            for (Map.Entry<String, String> entry : request.getLikeParams().entrySet()) {
                queryBuilder.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
            }
        }
        if (request.getRangeParams() != null && !request.getRangeParams().isEmpty()) {
            for (EsRangeParams rangeParam : request.getRangeParams()) {
                RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(rangeParam.getField());
                if (rangeParam.getGte() != null) {
                    rangeQuery.gte(rangeParam.getGte());
                }
                if (rangeParam.getLte() != null) {
                    rangeQuery.lte(rangeParam.getLte());
                }
                queryBuilder.filter(rangeQuery);
            }
        }
        sourceBuilder.query(queryBuilder);
        // 打印查询语句，可以放到kibana中执行并分析性能
        System.out.println(searchRequest.source().toString());
        // 执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        EsSearchResult result = new EsSearchResult();
        result.setPageIndex(request.getPageIndex());
        result.setPageSize(request.getPageSize());
        result.setTotal(searchResponse.getHits().getTotalHits().value);
        result.setList(Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getSourceAsMap).collect(Collectors.toList()));
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    /**
     * 聚合统计
     *
     * @param request
     * @return
     * @throws IOException
     */
    @PostMapping("/aggs")
    public ResponseEntity<List<Object>> aggs(@RequestBody EsSearchRequest request) throws IOException {

        // 创建 SearchSourceBuilder 实例
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 设置 track_total_hits
        searchSourceBuilder.trackTotalHits(true);

        // 设置分页参数
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1);

        // 构建 bool 查询
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("provinceName", "浙江"));

        // 添加查询到 SearchSourceBuilder
        searchSourceBuilder.query(boolQuery);

        // 构建聚合
        TermsAggregationBuilder cityGroup = AggregationBuilders.terms("city_group")
                .field("cityName").size(10)
                .subAggregation(AggregationBuilders.terms("network_group").field("network").size(10))
                .subAggregation(AggregationBuilders.terms("phoneBrand_group").field("phoneBrandName").size(10))
                .subAggregation(AggregationBuilders.terms("sdkVersion_group").field("sdkVersion").size(10))
                .subAggregation(AggregationBuilders.terms("platform_group").field("platformName").size(10))
                .subAggregation(AggregationBuilders.terms("req_group").field("bizType").size(10))
                .subAggregation(
                        AggregationBuilders.filter("ecpm_group", QueryBuilders.termQuery("bizType", 2))
                                .subAggregation(AggregationBuilders.avg("avg_ecpm").field("ecpm"))
                );

        // 添加聚合到 SearchSourceBuilder
        searchSourceBuilder.aggregation(cityGroup);

        // 创建 SearchRequest 并指定索引名称
        SearchRequest searchRequest = new SearchRequest("ssp_ad_union_log_202403");
        searchRequest.source(searchSourceBuilder);

        // 执行搜索请求
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        List<Object> result = new ArrayList<>(searchResponse.getAggregations().asList());
        for (Aggregation aggregation : searchResponse.getAggregations().asList()) {
            String json = aggregation.toString();
//            String json = OBJECT_MAPPER.writeValueAsString(aggregation);
//            Map<String, Object> map = new HashMap<>();
//            BeanUtils.copyProperties(aggregation, map);
            result.add(json);
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    /**
     * 通过json新增
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(path = "add")
    public ResponseEntity<IndexResponse> add() throws IOException {
        IndexRequest indexRequest = new IndexRequest("ssp_ad_union_log_202403");
//        indexRequest.id("1234567890");
        indexRequest.opType(DocWriteRequest.OpType.CREATE);


        indexRequest.source("{\n" +
                "          \"id\": 27731976,\n" +
                "          \"reqId\": \"d63e0377-639a-4ac4-96a7-677d507e627e\",\n" +
                "          \"device\": \"android\",\n" +
                "          \"platform\": 3,\n" +
                "          \"platformName\": \"快手\",\n" +
                "          \"clientType\": 1,\n" +
                "          \"myAppId\": \"300001\",\n" +
                "          \"deviceId\": \"d63e0377-639a-4ac4-96a7-677d507e627e\",\n" +
                "          \"adSiteGroupId\": 100000055,\n" +
                "          \"adSiteId\": \"6827003034\",\n" +
                "          \"packagePath\": \"com.jihuomiao.app\",\n" +
                "          \"ecpm\": 10700,\n" +
                "          \"location\": null,\n" +
                "          \"ip\": null,\n" +
                "          \"cityId\": 422800,\n" +
                "          \"areaId\": 422822,\n" +
                "          \"cityName\": \"恩施\",\n" +
                "          \"areaName\": \"建始\",\n" +
                "          \"provinceId\": 420000,\n" +
                "          \"provinceName\": \"湖北\",\n" +
                "          \"phoneBrand\": \"OPPO\",\n" +
                "          \"phoneBrandName\": \"oppo\",\n" +
                "          \"phoneModel\": null,\n" +
                "          \"idfa\": null,\n" +
                "          \"bizType\": 1,\n" +
                "          \"sdkVersion\": \"1.0.2\",\n" +
                "          \"network\": \"5g\",\n" +
                "          \"logTime\": \"2025-03-03 15:31:10\",\n" +
                "          \"createdAt\": \"2025-03-03 15:31:10\"\n" +
                "        }", XContentType.JSON);

        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        return new ResponseEntity<>(indexResponse, HttpStatus.OK);

    }

    @RequestMapping(path = "addBatch")
    public ResponseEntity<BulkResponse> addBatch() throws IOException {

        // 创建 BulkRequest
        BulkRequest bulkRequest = new BulkRequest();
        // 添加多个
        bulkRequest.add(new IndexRequest("ssp_ad_union_log_202403")
//                .id("12345678909")
                .source("{ \"field1\": \"value1\", \"field2\": \"value2\" }", XContentType.JSON));

        bulkRequest.add(new IndexRequest("my_index")
//                .id("12345678987654")
                .source("{ \"field1\": \"value2\", \"field2\": \"value2\" }", XContentType.JSON));

        // 执行批量创建操作
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 检查是否有错误
        if (bulkResponse.hasFailures()) {
            System.err.println("Bulk operation had failures: " + bulkResponse.buildFailureMessage());
        } else {
            System.out.println("All documents created successfully.");
        }
        return new ResponseEntity<>(bulkResponse, HttpStatus.OK);
    }
}
