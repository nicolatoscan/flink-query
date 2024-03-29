package org.apache.flink.generator;

public class FileDataEntry {

    String payLoad;
    String msgId;
    Long sourceInTimestamp;

    // timeStamp

    public FileDataEntry() {
        this.payLoad = "";
        this.msgId = "0";
        this.sourceInTimestamp = -1L;
    }

    public FileDataEntry(String payLoad, String msgId, Long sourceInTimestamp) {
        this.payLoad = payLoad;
        this.msgId = msgId;
        this.sourceInTimestamp = sourceInTimestamp;
    }

    @Override
    public String toString() {
        return "{ #" + msgId +
                " : { payLoad : " + payLoad 
                + " , timeStamp : " + sourceInTimestamp + " }";
    }

    public String getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(String payLoad) {
        this.payLoad = payLoad;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public Long getSourceInTimestamp() {
        return sourceInTimestamp;
    }

    public void setSourceInTimestamp(Long sourceInTimestamp) {
        this.sourceInTimestamp = sourceInTimestamp;
    }

    public int getLength(){
        // System.out.println(this.msgId.length() + this.getPayLoad().length());
        return this.msgId.getBytes().length + this.payLoad.getBytes().length;
    }
}

