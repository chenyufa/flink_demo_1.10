package entity;

import java.io.Serializable;

/**
 * @ date: 2020/04/24 16:02
 * @ author: Cheney
 * @ description: 商品浏览行为
 */
public class ProductViewData implements Serializable {

    public String productId;

    public String userName;

    public int eventType;

    public Long eventTime;

    public ProductViewData() {}

    public ProductViewData(String productId, String userName, int eventType, Long eventTime) {
        this.productId = productId;
        this.userName = userName;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "ProductViewData{" +
                "productId='" + productId + '\'' +
                ", userName='" + userName + '\'' +
                ", eventType=" + eventType +
                ", eventTime=" + eventTime +
                '}';
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
