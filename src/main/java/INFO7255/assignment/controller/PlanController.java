package INFO7255.assignment.controller;


import INFO7255.assignment.constant.CommonConstants;
import INFO7255.assignment.service.ConsumerMessageQueue;
import INFO7255.assignment.service.MessageQueueService;
import INFO7255.assignment.service.PlanService;
import INFO7255.assignment.service.RedisService;
import INFO7255.assignment.util.JsonUtil;
import INFO7255.assignment.validator.JsonValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.everit.json.schema.ValidationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;


import javax.validation.Valid;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;


import INFO7255.assignment.util.JsonUtil;

@RestController
public class PlanController {
    @Autowired
    JsonValidator validator;

    @Autowired
    PlanService planservice ;

    @Autowired
    private MessageQueueService messageQueueService;

    @Autowired
    ConsumerMessageQueue cmq ;



    Map<String, Object> m = new HashMap<String, Object>();

    //test
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private RedisService redisService;


    @GetMapping("/user")
    public ResponseEntity<String> gettest() {
        String t="test";
        return ResponseEntity.ok(t);
    }

    @PostMapping("/validate")
    public ResponseEntity<String> validateToken() {
        String t="validate";
        return ResponseEntity.ok(t);
    }




    @PostMapping(path = "/plan", produces = "application/json")
    public ResponseEntity<Object> createPlan(@Valid @RequestBody(required = false) String medicalPlan, @RequestHeader HttpHeaders headers) throws JSONException, Exception {
        m.clear();
        if (medicalPlan == null || medicalPlan.isEmpty()){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("Error", "Please provide the JSON!").toString());
        }


        JSONObject json = new JSONObject(medicalPlan);
        try{
            validator.validateJson(json);
        }catch(ValidationException ex){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new JSONObject().put("Error",ex.getErrorMessage()).toString());

        }

        String key = json.get("objectType").toString() + "_" + json.get("objectId").toString();
        if(planservice.checkIfKeyExists(key)){
            return ResponseEntity.status(HttpStatus.CONFLICT).body(new JSONObject().put("Message", " Plan with this id already exist").toString());
        }

        String newEtag = planservice.savePlanToRedisAndMQ(json, key);
        //index
        cmq.Consumer();


        //output objectId and objectType with json format
        return ResponseEntity.ok().eTag(newEtag).body(" {" +
                "\"objectId\": \"" + json.get("objectId") + "\" " +",\n"+
                "\"objectType\": \"" + json.get("objectType") + "\" "+
                "}");

    }


    @GetMapping(path = "/{type}/{objectId}", produces = "application/json")
    public ResponseEntity<Object> getPlan(@RequestHeader HttpHeaders headers, @PathVariable String objectId,@PathVariable String type) throws JSONException, Exception {



        if (!planservice.checkIfKeyExists(type + "_" + objectId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("Message", "ObjectId does not exist").toString());
        }

        String actualEtag = null;
        if (type.equals("plan")) {
            actualEtag = planservice.getEtag(type + "_" + objectId, "eTag");
            String eTag = headers.getFirst("If-None-Match");
            if (eTag != null && eTag.equals(actualEtag)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(actualEtag).build();
            }
        }

        String key = type + "_" + objectId;
        Map<String, Object> plan = planservice.getPlan(key);

        if (type.equals("plan")) {
            return ResponseEntity.ok().eTag(actualEtag).body(new JSONObject(plan).toString());
        }

        return ResponseEntity.ok().body(new JSONObject(plan).toString());


/*
        String key = type + "_" + objectId;
        String plan = planservice.getMedicalPlan(key);


        return ResponseEntity.ok().body(new JSONObject(plan).toString());

 */
    }


    @DeleteMapping(path = "/plan/{objectId}", produces = "application/json")
    public ResponseEntity<Object> getPlan(@RequestHeader HttpHeaders headers, @PathVariable String objectId) throws IOException {



        if (!planservice.checkIfKeyExists("plan"+ "_" + objectId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("Message", "ObjectId does not exist").toString());
        }

        //get object and put to queue set as delete
        String key = "plan" + "_" + objectId;
        Map<String, Object> plan = planservice.getPlan(key);
        String deletedplan = new JSONObject(plan).toString();
        messageQueueService.addToMessageQueue(deletedplan, true);

        //indexer consumer queue
        cmq.Consumer();

        //redis delete object
        planservice.deletePlan("plan" + "_" + objectId);

        

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new JSONObject().put("Message", "Deleted successfully!").toString());


    }


    @PutMapping(path = "/plan/{objectId}", produces = "application/json")
    public ResponseEntity<Object> updatePlan(@RequestHeader HttpHeaders headers, @Valid @RequestBody String medicalPlan,
                                             @PathVariable String objectId) throws IOException {



        JSONObject planObject = new JSONObject(medicalPlan);
        try {
            validator.validateJson(planObject);
        } catch (ValidationException ex) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new JSONObject().put("Validation Error", ex.getMessage()).toString());
        }

        String key = "plan_" + objectId;
        if (!planservice.checkIfKeyExists(key)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("Message", "ObjectId does not exist").toString());
        }

        // Get eTag value
        String actualEtag = planservice.getEtag(key, "eTag");
        String eTag = headers.getFirst("If-Match");
        if (eTag != null && !eTag.equals(actualEtag)) {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).eTag(actualEtag).build();
        }

        //get object and put to queue set as delete

        Map<String, Object> plan = planservice.getPlan(key);
        String deletedplan = new JSONObject(plan).toString();
        messageQueueService.addToMessageQueue(deletedplan, true);

        planservice.deletePlan("plan" + "_" + objectId);

        String newEtag = planservice.savePlanToRedisAndMQ(planObject, key);
        //index
        cmq.Consumer();

        return ResponseEntity.ok().eTag(newEtag)
                .body(new JSONObject().put("Message: ", "Resource updated successfully").toString());
    }

    @PatchMapping(path = "/plan/{objectId}", produces = "application/json")
    public ResponseEntity<Object> patchPlan(@RequestHeader HttpHeaders headers, @Valid @RequestBody String medicalPlan,
                                            @PathVariable String objectId) throws IOException {


        JSONObject planObject = new JSONObject(medicalPlan);

        if (!planservice.checkIfKeyExists("plan_" + objectId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("Message", "ObjectId does not exist").toString());
        }
        String key = "plan_" + objectId;
        // Get eTag value
        String actualEtag = planservice.getEtag(key, "eTag");
        String eTag = headers.getFirst("If-Match");
        if (eTag != null && !eTag.equals(actualEtag)) {
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).eTag(actualEtag).build();
        }

        String newEtag = planservice.savePlanToRedisAndMQ(planObject, key);
        //index
        cmq.Consumer();

        return ResponseEntity.ok().eTag(newEtag)
                .body(new JSONObject().put("Message: ", "Resource updated successfully").toString());
    }



}
