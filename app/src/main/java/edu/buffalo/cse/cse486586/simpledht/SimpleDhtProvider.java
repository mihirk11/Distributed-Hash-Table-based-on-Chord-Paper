/*
DS PA3
Mihir Kulkarni
mihirdha@buffalo.edu
50168610
 */
package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.renderscript.Element;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.os.SystemClock.sleep;

public class SimpleDhtProvider extends ContentProvider {
    String PROVIDERTAG="PROVIDERTAG";
    boolean initialized=false;
    Context c;
    SQLiteDatabase readDb;
    SQLiteDatabase writeDb;
    String myPort;
    String myId;
    Message joinReply;

    public static final int SERVER_PORT=10000;

    Boolean amIFirst=null;
    ArrayList<Node> nodeList=new ArrayList<Node>();
    Node myNode,successorNode,predecessorNode;
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    class NodeComparator implements Comparator<Node>{

        @Override
        public int compare(Node lhs, Node rhs) {
            return lhs.hash.compareTo(rhs.hash);
        }
    }
    DictionaryOpenHelper mDbHelper;

    public SimpleDhtProvider() {

    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("@")||selection.equals("*")){
            Log.d(PROVIDERTAG,"Selection is @");
            return deleteMe(uri,selection,selectionArgs,true);
        }else if(selection.equals("*")){
            Log.d(PROVIDERTAG,"Selection is *");
            Message message=new Message("delete",selection,null,myNode.id,successorNode.id,false);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            return deleteMe(uri,selection,selectionArgs,true);//TODO remove for * this should be implemented differently
        }else{

            Log.d(PROVIDERTAG,"Selection is "+selection);
            Message message=new Message("delete",selection,null,myNode.id,successorNode.id,false);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            return deleteMe(uri,selection,selectionArgs,false);//It is single key
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            if(isMyRange(values.get("key").toString())){
                    //My Range
                    return insertMe(uri, values);
            }else{
                Message message=new Message("insert",values.get("key").toString(),values.get("value").toString(),myNode.id,successorNode.id,false);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
        //return insertMe(uri, values);
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try{
            initDB();//Init DB
            init();//Init self


            nodeList.add(myNode);
            /*for (int i = 5554; i <= 5562; i=i+2) {
                Node node = new Node(Integer.toString(i), genHash(Integer.toString(i)));
                Log.d(PROVIDERTAG, "Node added id:" + Integer.toString(i) + " hash:" + genHash(Integer.toString(i)));
                nodeList.add(node);
            }*/

            Message joinRequest=new Message("joinrequest",null,null,myNode.id,"5554",false);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinRequest);

            //joinReply=sendBlockingMessage(joinRequest);
           // joinReply.wait();

            //Log.d(PROVIDERTAG,"Join reply received");



            calculateVarsFromNodelist();

            //Deploy servertask
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);



        }catch (NoSuchAlgorithmException nsae){
            nsae.printStackTrace();
        } catch (IOException e) {
            Log.e("TAG", "Can't create a ServerSocket" + e);
        } /*catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        return false;
    }

    private void calculateVarsFromNodelist() {
        Collections.sort(nodeList,new NodeComparator());
        Log.d(PROVIDERTAG,"Calculating variable from nodelist");
        int count=-1;
        int myIndex=-1;
        for(Node n:nodeList){
            count++;
            if(n.id.equals(myId)){
                myIndex=count;
            }
            Log.d(PROVIDERTAG,"Sorted id:"+n.id+" hash:"+n.hash);
        }
        Log.d(PROVIDERTAG,"My Index is "+myIndex+" My id is "+nodeList.get(myIndex).id);

        int nextIndex=myIndex+1;
        if(nextIndex>=nodeList.size())nextIndex=0;

        int prevIndex=myIndex-1;
        if(prevIndex<0) prevIndex=nodeList.size()-1;

        Log.d(PROVIDERTAG,"nextIndex: "+nextIndex+" prevIndex;"+prevIndex);

        predecessorNode=nodeList.get(prevIndex);
        successorNode=nodeList.get(nextIndex);

        if(myIndex==0)amIFirst=true;
        else amIFirst=false;

        Log.d(PROVIDERTAG," Successor id:"+successorNode.id+" Predecessor id"+predecessorNode.id);


    }

    private void init() throws NoSuchAlgorithmException {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myNode= new Node(myId,genHash(myId));
        Log.d(PROVIDERTAG,"My Id is "+myNode.id);
        myPort = String.valueOf((Integer.parseInt(myId) * 2));
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        if(selection.equals("@")){//TODO remove for * this should be implemented differently
            return queryMe(uri,projection,selection,selectionArgs,sortOrder,true);
        }else if(selection.equals("*")||!isMyRange(selection)){
            return queryRemote(uri, projection, selection, selectionArgs, sortOrder, false);
        }
        return queryMe(uri, projection, selection, selectionArgs, sortOrder,false);
    }

    private Cursor queryRemote(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder, boolean b) {
        Message message= new Message("query",selection,null,myNode.id,myNode.id,true);

        try {
            Message queryReply=new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
            for(String key:queryReply.keyValueMap.keySet()){
                Log.d(PROVIDERTAG,"Map element key:"+key+" value:"+queryReply.keyValueMap.get(key));
            }
            String[] columnNames = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames, 1);
            for(String key:queryReply.keyValueMap.keySet()) {

                String[] columnValues = new String[2];
                columnValues[0] = key;
                columnValues[1] = queryReply.keyValueMap.get(key);
                mc.addRow(columnValues);
            }
            return mc;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        //  Message queryReply=sendBlockingMessage(message);
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    private Uri insertMe(Uri uri, ContentValues values){
        Log.d(PROVIDERTAG,"In Insert Uri:"+uri.toString()+" ContentValues:"+values.toString());
        try {


            writeDb.insertWithOnConflict("dictionary", null, values,SQLiteDatabase.CONFLICT_REPLACE);

            Log.v("insert", values.toString());
            //delete(uri,values.get("key").toString(),null);//Just for testing. Remove later
            return uri;
        }catch (Exception e){
            e.printStackTrace();
            //Log.e(PROVIDERTAG,e.printStackTrace());
        }
        return null;
    }
    public int deleteMe(Uri uri, String selection, String[] selectionArgs,boolean lDump) {

        if(lDump==true){
            int rowsDeleted=writeDb.delete("dictionary", null, null);
            Log.d(PROVIDERTAG,"Delete local: rowsdeleted:"+rowsDeleted);

            return rowsDeleted;
        }
        int rowsDeleted=writeDb.delete("dictionary", "key ='" + selection + "'", selectionArgs);
        Log.d(PROVIDERTAG,"Delete row:  key:"+selection+" rowsdeleted:"+rowsDeleted);

        return rowsDeleted;
    }
    public Cursor queryMe(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder,boolean lDump) {
        Log.d(PROVIDERTAG, "In Query Uri:" + uri.toString() + " Selection:" + selection);



        if(lDump==true){
            //Cursor cursor=readDb.rawQuery("select * from dictionary",null);
            Cursor cursor=readDb.query("dictionary", projection, null, selectionArgs, null, null, sortOrder);
            Log.d(PROVIDERTAG,"Cursor has values "+cursor.getCount());

            return cursor;
        }
        Cursor cursor=readDb.query("dictionary", projection, "key ='" + selection + "'", selectionArgs, null, null, sortOrder);
        Log.d(PROVIDERTAG,"Cursor has values "+cursor.getCount());

        return cursor;

    }
    private void initDB(){
        initialized=true;
        Log.d(PROVIDERTAG,"init called");
        c=SimpleDhtProvider.this.getContext();
        //c=;
        if(c==null){
            Log.d(PROVIDERTAG,"Context is null");
        }
        mDbHelper = new DictionaryOpenHelper(this.getContext());
        readDb = mDbHelper.getReadableDatabase();
        writeDb = mDbHelper.getWritableDatabase();
    }

    private boolean isMyRange(String key){
        String keyHash= null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(amIFirst) {
            if (keyHash.compareTo(myNode.hash) <= 0 || keyHash.compareTo(predecessorNode.hash) > 0) {
                Log.d(PROVIDERTAG,"My Range key:"+key+" keyhash:"+keyHash);
                return true;
            }
        }else{
            if(keyHash.compareTo(myNode.hash)<=0 && keyHash.compareTo(predecessorNode.hash)>0) {
                Log.d(PROVIDERTAG,"My Range key:"+key+" keyhash:"+keyHash);
                return true;
            }
        }
        Log.d(PROVIDERTAG,"Not my Range key:"+key+" keyhash:"+keyHash);
        return false;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }















    //SERVERTASK
    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.e("mihir", "Inside servertask");

            //Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html

            String str="";
            while(true) {
                try {
                    String jsonMessageStr;
                    Log.e(PROVIDERTAG, "before accept");
                    Socket s = serverSocket.accept();
                    Log.e(PROVIDERTAG, "Data incoming");

                    ObjectOutputStream op;
                    ObjectInputStream ip = new ObjectInputStream(s.getInputStream());



                    Message recievedMessage= (Message) ip.readObject();
                    Log.d(PROVIDERTAG, "Message recieved type:" + recievedMessage.type + " key:" + recievedMessage.key + " value:" + recievedMessage.value + " from:" + recievedMessage.from + " to:" + recievedMessage.to);


                    if(recievedMessage.type.equals("joinrequest")){
                        Log.d(PROVIDERTAG,"Processing joinrequest");
                        if(!myNode.id.equals("5554")){
                            Log.e(PROVIDERTAG,"joinrequest came to wrong Node");
                            continue;
                        }

                        boolean found=false;
                        for(Node n:nodeList){
                            if(n.id.equals(recievedMessage.from))found=true;

                        }

                        if(found==false){
                            Log.d(PROVIDERTAG,"Adding new node to nodeList:"+recievedMessage.from);
                            Node newNode= new Node(recievedMessage.from,genHash(recievedMessage.from));
                            nodeList.add(newNode);
                            calculateVarsFromNodelist();
                        }

                        for(Node n:nodeList) {
                            Message replyMessage = new Message("joinreply", myNode.id, n.id, nodeList, false);
                            publishProgress(replyMessage);
                        }




                        /*
                        //TODO Just sending msg back. Change later
                        op = new ObjectOutputStream(s.getOutputStream());
                        //Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
                        op.writeObject(replyMessage);
                        op.flush();
                        op.close();*/

                    }
                    else if(recievedMessage.type.equals("joinreply")){
                        Log.d(PROVIDERTAG,"Processing joinreply");
                        nodeList=recievedMessage.nodeArrayList;
                        calculateVarsFromNodelist();
                    }
                    else if(recievedMessage.type.equals("insert")){
                        if(isMyRange(recievedMessage.key)){
                            Log.d(PROVIDERTAG,"Storing message key:"+recievedMessage.key+" value:"+recievedMessage.value);
                            //Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            ContentValues contentValues=new ContentValues();
                            contentValues.put("key",recievedMessage.key);
                            contentValues.put("value",recievedMessage.value);
                            insertMe(mUri,contentValues);
                        }else{
                            Log.d(PROVIDERTAG,"Forwarding message to"+successorNode.id+"  message key:"+recievedMessage.key+" value:"+recievedMessage.value);
                            recievedMessage.to=successorNode.id;
                            publishProgress(recievedMessage);
                        }
                   }
                else if(recievedMessage.type.equals("query")){

                    if(recievedMessage.key.equals("*")){
                        //get data from self
                        Cursor cursor=queryMe(mUri, null, recievedMessage.key, null, null, true);
                        //queryMe(mUri,null,recievedMessage.key,null,null,true);
                        HashMap<String,String> hashMap= cursorToMap(cursor);
                        //forward message and wait for reply if successor is not sender
                        if(!recievedMessage.from.equals(successorNode.id)){
                            recievedMessage.to=successorNode.id;
                            Message queryReply=sendBlockingMessage(recievedMessage);
                            hashMap.putAll(queryReply.keyValueMap);
                        }
                        Message queryReply=new Message("queryreply",myNode.id,recievedMessage.from, hashMap);
                        op = new ObjectOutputStream(s.getOutputStream());
                        op.writeObject(queryReply);
                        op.flush();
                        //combine both maps and send it as reply
                    }else if(isMyRange(recievedMessage.key)){
                        //get data from self
                        Log.d(PROVIDERTAG,"queried key is in my range key:"+recievedMessage.key);
                        Cursor cursor=queryMe(mUri, null, recievedMessage.key, null, null, false);


                        // send reply
                        Message queryReply=new Message("queryreply",myNode.id,recievedMessage.from, cursorToMap(cursor));
                        op = new ObjectOutputStream(s.getOutputStream());
                        op.writeObject(queryReply);
                       // op.flush();
                    }else{
                        //not my range and not Gdump
                        //send message forward if successor is not sender and wait for reply
                        recievedMessage.to=successorNode.id;
                        Message queryReply=sendBlockingMessage(recievedMessage);
                        op = new ObjectOutputStream(s.getOutputStream());
                        op.writeObject(queryReply);
                        op.flush();
                        //If successor is sender then something is fishy
                        //send the received reply to predecessor
                    }


                    /*ObjectOutputStream op = new ObjectOutputStream(s.getOutputStream());
                                            op.writeObject(recievedMessage);
                                            op.flush();*//*
                    */
                }
                    else if(recievedMessage.type.equals("delete")){
                        if(!recievedMessage.from.equals(successorNode.id)){
                            Log.d(PROVIDERTAG,"Forwarding message to"+successorNode.id+"  message key:"+recievedMessage.key+" value:"+recievedMessage.value);
                            recievedMessage.to=successorNode.id;
                            publishProgress(recievedMessage);
                        }
                        if(recievedMessage.key.equals("*")){
                            deleteMe(mUri, recievedMessage.key, null, true);
                        }else{
                            deleteMe(mUri,recievedMessage.key,null,false);
                        }
                    }


                    ip.close();
                    s.close();
                    //publishProgress(str);
                }catch (Exception e){
                    e.printStackTrace();
                    continue;
                }
            }
            //return null;
        }

        private HashMap<String, String> cursorToMap(Cursor cursor) {
            HashMap<String,String> hashMap= new HashMap<String, String>();
            int keyIndex = cursor.getColumnIndex("key");
            int valueIndex = cursor.getColumnIndex("value");
            //publishProgress("LOCAL DUMP:\n");
            while(cursor.moveToNext()){
                String returnKey = cursor.getString(keyIndex);
                String returnValue = cursor.getString(valueIndex);
                //publishProgress("Key: "+returnKey+" value: "+returnValue+"\n");
                hashMap.put(returnKey,returnValue);
            }
            return hashMap;
        }


        Message sendBlockingMessage(Message message){

            Log.d("mihir", "In sendBlockingMessage type:" + message.type+" key:"+message.key+" value:"+message.value+" from:"+message.from+" to:"+message.to);

            ObjectOutputStream op;
            Socket socket;
            // Socket socket=new Socket();
            Integer toPort=Integer.valueOf(message.to)*2;
            try {
                socket=new Socket();
                socket.connect(new InetSocketAddress("10.0.2.2", toPort));

                //Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                op = new ObjectOutputStream(socket.getOutputStream());
                //Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
                op.writeObject(message);
               // op.flush();
             //   op.close();



                //wait for reply

                ObjectInputStream ip = new ObjectInputStream(socket.getInputStream());
                Message queryReply = (Message) ip.readObject();
                Log.d(PROVIDERTAG, "Query reply recieved type:" + queryReply.type + " key:" + queryReply.key);
                socket.close();
                return queryReply;




            }catch (StreamCorruptedException e){
                //socket.close();
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;
        }
        //TODO http://stackoverflow.com/questions/5517641/publishprogress-from-inside-a-function-in-doinbackground



        protected void onProgressUpdate(Message...msgs) {
            /*
             * The following code displays what is received in doInBackground().
             */
            Message message=msgs[0];

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

            return;
        }

    }









    //CLIENTTASK
    private class ClientTask extends AsyncTask<Message, Void, Message> {

        @Override
        protected Message doInBackground(Message... msgs) {

          /*  Message message = msgs[0];
            Log.d("mihir", "In ClientTask Message  type:" + message.type+" key:"+message.key+" value:"+message.value);

            ObjectOutputStream op;

            try {
                if(message.type.equals("joinrequest")){
                   int retryCount=0;

                    while(retryCount<5) {
                        Socket socket=new Socket();
                        try {
                            Log.d(PROVIDERTAG,"Trying to contact 5554");
                            socket.connect(new InetSocketAddress("10.0.2.2", Integer.valueOf(11108)));
                            //Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                            op = new ObjectOutputStream(socket.getOutputStream());
                            //Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
                            op.writeObject(message);
                            op.flush();
                            op.close();
                            Log.d(PROVIDERTAG,"Contact with 5554 successful");
                            break;
                        } catch (StreamCorruptedException sce) {
                            retryCount++;
                            socket.close();
                            sleep(1000);

                        }
                    }
                }else {
                    Socket socket=new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.valueOf(successorNode.port)));
                    //Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                    op = new ObjectOutputStream(socket.getOutputStream());
                    //Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
                    op.writeObject(message);
                    op.flush();

                    if (message.type.equals("query")) {
                        //wait for reply

                        ObjectInputStream ip = new ObjectInputStream(socket.getInputStream());
                        Message queryReply = (Message) ip.readObject();
                        Log.d(PROVIDERTAG, "Query reply recieved type:" + queryReply.type + " key:" + queryReply.key);
                    }
                    op.close();
                    socket.close();
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        */




            Message message = msgs[0];
            Log.d("mihir", "In ClientTask Message  type:" + message.type + " key:" + message.key + " value:" + message.value);

            /*if(message.waitForReply==true){
                joinReply=sendBlockingMessage(message);
                joinReply.notify();
                return null;
            }
            */
            int retryCount=0;

            ObjectOutputStream op;
            Socket socket = new Socket();
            Integer toPort = Integer.valueOf(message.to) * 2;
            Log.d(PROVIDERTAG,"Sending message");
            try {
                socket.connect(new InetSocketAddress("10.0.2.2", toPort));

                //Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                op = new ObjectOutputStream(socket.getOutputStream());
                //Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
                op.writeObject(message);
                //op.flush();
                //op.close();
                //socket.close();
                /*if(!message.type.equals("joinrequest")){
                    socket.close();
                    return null;
                }
                */
                //wait for reply
                if(message.waitForReply==true){
                    Log.d(PROVIDERTAG,"Reading reply in Clienttask");
                    ObjectInputStream ip = new ObjectInputStream(socket.getInputStream());
                    Message queryReply = (Message) ip.readObject();
                    Log.d(PROVIDERTAG, "Query reply recieved in Client task type:" + queryReply.type);
                    /*for(String key:queryReply.keyValueMap.keySet()){
                        Log.d(PROVIDERTAG,"Map element key:"+key+" value:"+queryReply.keyValueMap.get(key));
                    }*/
                    socket.close();
                    return queryReply;
                }


            }catch (StreamCorruptedException e){
                e.printStackTrace();
            }
            catch (IOException e) {
                retryCount++;
                sleep(3000);
                e.printStackTrace();
                Log.d(PROVIDERTAG,"retrying again");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }

    }
 /*   Message sendBlockingMessage(Message message){

        Log.d("mihir", "In ClientTask Message  type:" + message.type+" key:"+message.key+" value:"+message.value);

        ObjectOutputStream op;
        Socket socket;
       // Socket socket=new Socket();
        Integer toPort=Integer.valueOf(message.to)*2;
        try {
            socket=new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", toPort));

            //Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
            op = new ObjectOutputStream(socket.getOutputStream());
            //Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
            op.writeObject(message);
            op.flush();
            op.close();
            socket.close();


            //wait for reply

            ObjectInputStream ip = new ObjectInputStream(socket.getInputStream());
            Message queryReply = (Message) ip.readObject();
            Log.d(PROVIDERTAG, "Query reply recieved type:" + queryReply.type + " key:" + queryReply.key);
            socket.close();
            return queryReply;




        }catch (StreamCorruptedException e){
            //socket.close();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            sendBlockingMessage(message);//TODO change it grader wont run with this
        }
        catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

*/
}