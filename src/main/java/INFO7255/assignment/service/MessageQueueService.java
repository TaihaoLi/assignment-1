package INFO7255.assignment.service;

import INFO7255.assignment.planDao.MessageQueueDao;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MessageQueueService {

    @Autowired
    private MessageQueueDao messageQueueDao;

    public void addToMessageQueue(String message, boolean isDelete) {
        JSONObject object = new JSONObject();
        object.put("message", message);
        object.put("isDelete", isDelete);

        // save plan to message queue "messageQueue"
        messageQueueDao.addToQueue("messageQueue", object.toString());
        System.out.println("Message saved successfully: " + object.toString());
    }
}