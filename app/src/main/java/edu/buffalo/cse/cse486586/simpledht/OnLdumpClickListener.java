package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

/**
 * Created by mihir on 3/27/16.
 */
public class OnLdumpClickListener implements View.OnClickListener {
    private static final String TAG = OnLdumpClickListener.class.getName();
    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    public OnLdumpClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    @Override
    public void onClick(View v) {new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);}

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }




    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {



            Cursor resultCursor=localQuery();
            if(resultCursor==null){
                publishProgress("No rows returned\n");
                return null;
            }

            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
            publishProgress("LOCAL DUMP:\n");
            while(resultCursor.moveToNext()){
                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                publishProgress("Key: "+returnKey+" value: "+returnValue+"\n");
            }


            resultCursor.close();
/*
            //Just for testing. Remove later
            mContentResolver.delete(mUri,"@",null);

            resultCursor=localQuery();
            if(resultCursor==null){
                publishProgress("No rows returned\n");
                return null;
            }

            keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
            valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
            publishProgress("LOCAL DUMP:\n");
            while(resultCursor.moveToNext()){
                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                publishProgress("Key: "+returnKey+" value: "+returnValue+"\n");
            }


            resultCursor.close();

*/

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }


        private Cursor localQuery() {
            try {
                Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                if (keyIndex == -1 || valueIndex == -1) {
                    Log.e(TAG, "Wrong columns");
                    resultCursor.close();
                    throw new Exception();
                }
                return resultCursor;

            } catch (Exception e) {
                return null;
            }

        }
    }
}
