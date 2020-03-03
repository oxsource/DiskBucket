package com.pizzk.android.diskbucket.app

import android.content.Context
import android.util.Log
import com.pizzk.android.diskbucket.DiskBucket
import java.io.ByteArrayInputStream
import java.io.File

object DiskBucketTest {
    private const val TAG: String = "DiskBucketTest"
    private val bucket: DiskBucket by lazy { DiskBucket.get("test") }
    private val key: String = "file1"

    fun doWrite(context: Context) {
        val line = "hello, this is a text."
        val ins = ByteArrayInputStream(line.toByteArray())
        bucket.put(context, key, "$key.txt", ins)
    }

    fun doRead(context: Context) {
        val file: File = bucket.get(context, key) ?: return
        Log.d(TAG, file.absolutePath)
    }

    fun doPrint(context: Context) {
        val vs = bucket.keyValues(context)
        Log.d(TAG, "${vs.size}")
    }

    fun doDel(context: Context) {
        bucket.del(context, listOf(key))
    }

    fun doRemove(context: Context) {
        bucket.remove(context)
    }
}
