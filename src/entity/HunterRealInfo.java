package entity;

/**
 * @ date: 2020/10/4 19:29
 * @ author: FatCheney
 * @ description:
 * @ version: 1.0.0
 */
public class HunterRealInfo {

    public String id;
    public String bid;
    public String realName;
    public String idCardNumber;
    public String idCardFront;
    public String status;
    public String bank;

    public Double statusNum;

    public HunterRealInfo() {
    }

    public HunterRealInfo(String id, String bid, String realName, String idCardNumber, String idCardFront, String status, String bank) {
        this.id = id;
        this.bid = bid;
        this.realName = realName;
        this.idCardNumber = idCardNumber;
        this.idCardFront = idCardFront;
        this.status = status;
        this.bank = bank;
    }

    public Double getStatusNum() {
        return statusNum;
    }

    public void setStatusNum(Double statusNum) {
        this.statusNum = statusNum;
    }

    public String getId() {
        return id;
    }

    public String getBid() {
        return bid;
    }

    public String getRealName() {
        return realName;
    }

    public String getIdCardNumber() {
        return idCardNumber;
    }

    public String getIdCardFront() {
        return idCardFront;
    }

    public String getStatus() {
        return status;
    }

    public String getBank() {
        return bank;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setBid(String bid) {
        this.bid = bid;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public void setIdCardNumber(String idCardNumber) {
        this.idCardNumber = idCardNumber;
    }

    public void setIdCardFront(String idCardFront) {
        this.idCardFront = idCardFront;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setBank(String bank) {
        this.bank = bank;
    }

}
