package INFO7255.assignment.controller;


import INFO7255.assignment.service.PlanService;
import INFO7255.assignment.validator.JsonValidator;
import org.apache.commons.codec.digest.DigestUtils;
import org.everit.json.schema.ValidationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.validation.Valid;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PlanController {
    @Autowired
    JsonValidator validator;

    @Autowired
    PlanService planservice ;



    Map<String, Object> m = new HashMap<String, Object>();



    @GetMapping("/user")
    public ResponseEntity<String> gettest() {
        String t="test";
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

        Map<String, Object> savedPlanMap = planservice.savePlan(key,json);

        //create etag for create
        String savedPlan = json.toString();

        String newEtag = DigestUtils.md5Hex(savedPlan);

        //output objectId and objectType with json format
        return ResponseEntity.ok().eTag(newEtag).body(" {" +
                "\"objectId\": \" " + json.get("objectId") + "\" " +",\n"+
                "\"objectType\": \"" + json.get("objectType") + "\" "+
                "}");

    }


    @GetMapping(path = "/{type}/{objectId}", produces = "application/json")
    public ResponseEntity<Object> getPlan(@RequestHeader HttpHeaders headers, @PathVariable String objectId,@PathVariable String type) throws JSONException, Exception {



        if (!planservice.checkIfKeyExists(type + "_" + objectId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("Message", "ObjectId does not exist").toString());
        }



        String key = type + "_" + objectId;
        Map<String, Object> plan = planservice.getPlan(key);


        return ResponseEntity.ok().body(new JSONObject(plan).toString());
    }


    @DeleteMapping(path = "/plan/{objectId}", produces = "application/json")
    public ResponseEntity<Object> getPlan(@RequestHeader HttpHeaders headers, @PathVariable String objectId){



        if (!planservice.checkIfKeyExists("plan"+ "_" + objectId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("Message", "ObjectId does not exist").toString());
        }

        planservice.deletePlan("plan" + "_" + objectId);

        

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new JSONObject().put("Message", "Deleted successfully!").toString());


    }
}
