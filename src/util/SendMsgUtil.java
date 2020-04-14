package util;

import com.alibaba.fastjson.JSON;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.HashMap;
import java.util.Map;

/**
 * @ date: 2020/04/14 15:52
 * @ author: Cheney
 * @ description: 消息发送
 */
public class SendMsgUtil {

    //默认token
    public static final String DINGDING_ACCESS_TOKEN_BIGDATA="eaa8d6f88b8fb87ed93e30e9077924cb918bf48a2015776735e4bbced0792740";


    /**
     * 发送信息至钉钉
     * @param msg 所要发送的内容
     * @return	发送返回信息
     */
    public static String sendMsgToDingDing(String msg,String token){
        if(token == null || "".equals(token)){
            token = DINGDING_ACCESS_TOKEN_BIGDATA;
        }
        String url="https://oapi.dingtalk.com/robot/send?access_token="+token;

        Map<String,Object> params=new HashMap<String,Object>();
        params.put("msgtype","text");

        Map<String,Object> textParams=new HashMap<String,Object>();
        textParams.put("content",msg);

        params.put("text",textParams);

        String jsonStr = JSON.toJSONString(params);

        return HttpClient4.doPost(url,jsonStr);

    }

}
