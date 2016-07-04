package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by mihir on 3/27/16.
 */
public class DictionaryOpenHelper extends SQLiteOpenHelper {

    String TAG=this.getClass().getName()+"_PROVIDERTAG";
    private static final String MY_KEY="key";
    private static final String MY_VALUE="value";

    private static final int DATABASE_VERSION = 2;
    public static final String DATABASE_NAME = "Dictionary.db";

    private static final String DICTIONARY_TABLE_NAME = "dictionary";
    private static final String DICTIONARY_TABLE_CREATE =
            "CREATE TABLE " + DICTIONARY_TABLE_NAME + " (" +
                    MY_KEY + " TEXT NOT NULL UNIQUE, " +
                    MY_VALUE + " TEXT);";

    DictionaryOpenHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {

        db.execSQL("DROP TABLE IF EXISTS " + DICTIONARY_TABLE_NAME);
        db.execSQL(DICTIONARY_TABLE_CREATE);
    }


    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.e(TAG, "In Database onUpgrade- Guesss this shouldn't happen");
        db.execSQL("DROP TABLE IF EXISTS " + DICTIONARY_TABLE_NAME);
        onCreate(db);
    }

    @Override
    public void onOpen(SQLiteDatabase db) {//TODO Do I really want this to happen?
        super.onOpen(db);
        db.execSQL("DROP TABLE IF EXISTS " + DICTIONARY_TABLE_NAME);
        db.execSQL(DICTIONARY_TABLE_CREATE);
    }
}