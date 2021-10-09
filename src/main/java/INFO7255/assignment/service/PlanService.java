package INFO7255.assignment.service;

import INFO7255.assignment.planDao.PlanDao;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class PlanService {
    @Autowired
    PlanDao planDao;

//save plan
    public void saveMedicalPlan(String rediskey,String filed,JSONObject planObject){
        planDao.hSet(rediskey, filed, planObject.toString());
        return;

    }

    //get plan
    public String getMedicalPlan(String redisKey){

        String value= planDao.hGet(redisKey,"content");
        return value;

    }
//delete plan
    public void deletetMedicalPlan(String redisKey){

       planDao.deletehSet(redisKey);


    }

  //check if exists
    public boolean checkIfKeyExists(String key){
        return planDao.checkIfKeyExist(key);
    }



    private boolean isStringDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }



}
