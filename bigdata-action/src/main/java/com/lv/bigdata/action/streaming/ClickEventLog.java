package com.lv.bigdata.action.streaming;

import java.io.Serializable;

/**
 * @author lvsicheng
 * @date 2019-05-05 16:24
 */
public class ClickEventLog implements Serializable {

    private String visitTime;

    private String uid;

    private String queryWord;

    private String rank;

    private String seq;

    private String url;

    public ClickEventLog(String visitTime, String uid, String queryWord, String rank, String seq, String url) {
        this.visitTime = visitTime;
        this.uid = uid;
        this.queryWord = queryWord;
        this.rank = rank;
        this.seq = seq;
        this.url = url;
    }

    public String getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(String visitTime) {
        this.visitTime = visitTime;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getQueryWord() {
        return queryWord;
    }

    public void setQueryWord(String queryWord) {
        this.queryWord = queryWord;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String getSeq() {
        return seq;
    }

    public void setSeq(String seq) {
        this.seq = seq;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ClickEventLog{" +
                "visitTime='" + visitTime + '\'' +
                ", uid='" + uid + '\'' +
                ", queryWord='" + queryWord + '\'' +
                ", rank='" + rank + '\'' +
                ", seq='" + seq + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
