package INFO7255.assignment.service;

import java.io.IOException;
import java.util.*;

import INFO7255.assignment.planDao.MessageQueueDao;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;
@Service
public class ConsumerMessageQueue {

    private static Jedis jedis;
    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));
    private static final String IndexName="planindex";

    private static LinkedHashMap<String, Map<String, Object>> MapOfDocuments = new LinkedHashMap<>();

    private static ArrayList<String> listOfKeys = new ArrayList<>();

    public static void Consumer() throws IOException {
        jedis = new Jedis();
        System.out.println("Consumer MQ started");
        while (true) {
            String message = jedis.rpoplpush("messageQueue", "WorkingMQ");
            if (message == null) {
                break;
            }
            JSONObject result = new JSONObject(message);

            // Get action
            Object obj = result.get("isDelete");
            System.out.println("isDelete: " + obj.toString());

            boolean isDelete = Boolean.parseBoolean(obj.toString());
            if(!isDelete) {
                JSONObject plan= new JSONObject(result.get("message").toString());
                postDocument(plan);
            }else {
                System.out.println("PlanisDelete ");
                JSONObject plan= new JSONObject(result.get("message").toString());
                deleteDocument(plan);
            }
        }


    }

    private static boolean indexExists() throws IOException {
        GetIndexRequest request = new GetIndexRequest(IndexName);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }
/*
    private static String postDocument(JSONObject plan) throws IOException {
        if(!indexExists()) {
            createElasticIndex();
        }
        IndexRequest request = new IndexRequest(IndexName);
        request.id(plan.get("objectId").toString());
        request.source(plan.toString(), XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response id: "+indexResponse.getId());
        return indexResponse.getResult().name();
    }*/

    private static void postDocument(JSONObject plan) throws IOException {
        if (!indexExists()) {
            createElasticIndex();
        }

        MapOfDocuments = new LinkedHashMap<>();
        convertMapToDocumentIndex(plan, "", "plan");

        for (Map.Entry<String, Map<String, Object>> entry : MapOfDocuments.entrySet()) {
            String parentId = entry.getKey().split(":")[0];
            String objectId = entry.getKey().split(":")[1];
            IndexRequest request = new IndexRequest(IndexName);
            request.id(objectId);
            request.source(entry.getValue());
            request.routing(parentId);
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            System.out.println("response id: " + indexResponse.getId() + " parent id: " + parentId);
        }
    }

    private static void createElasticIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(IndexName);
        request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
       // String mapping = getMapping();
        //request.mapping(mapping, XContentType.JSON);
        //test
        XContentBuilder mapping = getMapping();
        request.mapping(mapping);

        client.indices().create(request, RequestOptions.DEFAULT);
    }

    private static void deleteDocument(JSONObject jsonObject) throws IOException {
       /*listOfKeys = new ArrayList<>();
        convertToKeys(jsonObject);

        for(String key : listOfKeys){
            DeleteRequest request = new DeleteRequest(IndexName, key);
            DeleteResponse deleteResponse = client.delete(
                    request, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                System.out.println("Document " + key + " Not Found!!");
            }
        }*/

        //test
        MapOfDocuments = new LinkedHashMap<>();
        convertMapToDocumentIndex(jsonObject, "", "plan");

        for (Map.Entry<String, Map<String, Object>> entry : MapOfDocuments.entrySet()) {
            String parentId = entry.getKey().split(":")[0];
            String objectId = entry.getKey().split(":")[1];
            DeleteRequest request = new DeleteRequest(IndexName);
            request.id(objectId);
            request.routing(parentId);
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                System.out.println("Document " + objectId + " Not Found!!");
            }
        }
    }

    private static Map<String, Map<String, Object>> convertToKeys(JSONObject jsonObject){

        Map<String, Map<String, Object>> map = new HashMap<>();
        Map<String, Object> valueMap = new HashMap<>();

        for (String key : jsonObject.keySet()) {
            String redisKey = jsonObject.get("objectId").toString();
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {
                convertToKeys((JSONObject) value);
            } else if (value instanceof JSONArray) {
                convertToKeysList((JSONArray) value);
            } else {
                valueMap.put(key, value);
                map.put(redisKey, valueMap);
            }
        }

        listOfKeys.add(jsonObject.get("objectId").toString());
        return map;
    }

    private static List<Object> convertToKeysList(JSONArray jsonArray) {
        List<Object> list = new ArrayList<>();
        for (Object value : jsonArray) {
            if (value instanceof JSONArray) {
                value = convertToKeysList((JSONArray) value);
            } else if (value instanceof JSONObject) {
                value = convertToKeys((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }

    private static XContentBuilder getMapping() throws IOException {

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("plan");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("planType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("creationDate");
                        {
                            builder.field("type", "date");
                            builder.field("format", "MM-dd-yyyy");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("planCostShares");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("copay");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("deductible");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("linkedPlanServices");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("linkedService");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("name");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("planserviceCostShares");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("copay");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("deductible");
                        {
                            builder.field("type", "long");
                        }
                        builder.endObject();
                        builder.startObject("_org");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                        builder.startObject("objectId");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("objectType");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("plan_join");
                {
                    builder.field("type", "join");
                    builder.field("eager_global_ordinals", "true");
                    builder.startObject("relations");
                    {
                        builder.array("plan", "planCostShares", "linkedPlanServices");
                        builder.array("linkedPlanServices", "linkedService", "planserviceCostShares");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;

    }

    private static Map<String, Map<String, Object>> convertMapToDocumentIndex (JSONObject jsonObject,
                                                                        String parentId,
                                                                        String objectName ) {

        Map<String, Map<String, Object>> map = new HashMap<>();
        Map<String, Object> valueMap = new HashMap<>();
        Iterator<String> iterator = jsonObject.keys();

        while (iterator.hasNext()){
            String key = iterator.next();
            String redisKey = jsonObject.get("objectType") + ":" + parentId;
            Object value = jsonObject.get(key);

            if (value instanceof JSONObject) {

                convertMapToDocumentIndex((JSONObject) value, jsonObject.get("objectId").toString(), key);

            } else if (value instanceof JSONArray) {

                convertToList((JSONArray) value, jsonObject.get("objectId").toString(), key);

            } else {
                valueMap.put(key, value);
                map.put(redisKey, valueMap);
            }
        }

        Map<String, Object> temp = new HashMap<>();
        if(objectName == "plan"){
            valueMap.put("plan_join", objectName);
        } else {
            temp.put("name", objectName);
            temp.put("parent", parentId);
            valueMap.put("plan_join", temp);
        }

        String id = parentId + ":" + jsonObject.get("objectId").toString();
        System.out.println(valueMap);
        MapOfDocuments.put(id, valueMap);


        return map;
    }

    private static List<Object> convertToList(JSONArray array, String parentId, String objectName) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                value = convertToList((JSONArray) value, parentId, objectName);
            } else if (value instanceof JSONObject) {
                value = convertMapToDocumentIndex((JSONObject) value, parentId, objectName);
            }
            list.add(value);
        }
        return list;
    }
}
