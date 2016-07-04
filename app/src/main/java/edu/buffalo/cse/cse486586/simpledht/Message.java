package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by mihir on 3/30/16.
 */
//MESSAGE
class Message implements Serializable {
    public String type=null;
    public String key=null;
    public String value=null;
    public String from=null;
    public String to=null;
    public boolean waitForReply;

    public HashMap<String,String> keyValueMap=null;
    public ArrayList<Node> nodeArrayList= null;

    public Message(String type, String key,String value,String from,String to,boolean waitForReply) {
        this.type=type;
        this.key=key;
        this.value=value;
        this.from=from;
        this.to=to;
        this.waitForReply=waitForReply;
    }
    public Message(String type, String from,String to,ArrayList<Node> nodeArrayList,boolean waitForReply) {//For joinreply
        this.type=type;
        this.from=from;
        this.to=to;
        this.waitForReply=waitForReply;
        this.nodeArrayList=nodeArrayList;
    }
    public Message(String type,String from,String to,HashMap<String ,String> keyValueMap){//For query reply
        this.type=type;
        this.from=from;
        this.to=to;
        this.keyValueMap=keyValueMap;
    }
}
