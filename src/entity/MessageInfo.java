package entity;

import java.io.Serializable;

/**
 * @ date: 2020/04/14 15:08
 * @ author: Cheney
 * @ description: 简单的信息实体类
 */
public class MessageInfo implements Serializable {

    public String hostName;

    public String msgTime;

    public String status;/*RUNNING 正常 DEAD 宕机*/

    public MessageInfo(String hostName, String msgTime, String status) {
        this.hostName = hostName;
        this.msgTime = msgTime;
        this.status = status;
    }

    public MessageInfo() {}

    public String getHostName() {
        return hostName;
    }

    public String getMsgTime() {
        return msgTime;
    }

    public String getStatus() {
        return status;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setMsgTime(String msgTime) {
        this.msgTime = msgTime;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "hostName='" + hostName + '\'' +
                ", msgTime='" + msgTime + '\'' +
                ", status='" + status + '\'' +
                '}';
    }

}
