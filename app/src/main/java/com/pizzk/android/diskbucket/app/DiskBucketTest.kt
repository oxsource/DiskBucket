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
        for (i: Int in 0..10) {
            val line = "hello, this is a text $i."
            val ins = ByteArrayInputStream(line.toByteArray())
            val symbol = "$key$i"
            bucket.put(context, symbol, "$symbol.txt", ins)
        }
    }

    fun doRead(context: Context) {
        val file: File = bucket.get(context, key) ?: return
        Log.d(TAG, file.absolutePath)
    }

    fun doPrint(context: Context) {
        val vs = bucket.ls(context)
        Log.d(TAG, "${vs.size}")
    }

    fun doDel(context: Context) {
        bucket.del(context, listOf(key))
    }

    fun doClean(context: Context) {
        bucket.clean(context)
    }

    fun doGC(context: Context) {
        bucket.gc(context, 5 * 1024 * 1024, 5)
    }
}
