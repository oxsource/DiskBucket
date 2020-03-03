package com.pizzk.android.diskbucket

import android.content.Context
import android.util.Log
import java.io.*
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class DiskBucket private constructor(private val bucket: String) {
    companion object {
        private const val NAMESPACE = "DiskBucket"
        private const val SPLIT_FLAG = ","
        private val map: MutableMap<String, DiskBucket> = Collections.synchronizedMap(HashMap())

        fun get(bucket: String): DiskBucket {
            val e: DiskBucket? = map[bucket]
            if (null != e) return e
            val d = DiskBucket(bucket)
            map[bucket] = d
            return d
        }

        fun release(bucket: String) {
            map[bucket] ?: return
            map.remove(bucket)
        }
    }

    private val locker: ReadWriteLock by lazy { ReentrantReadWriteLock(true) }

    private fun dir(context: Context): File? {
        return try {
            val root: File = context.filesDir
            val child: String = arrayOf(NAMESPACE, bucket).joinToString(File.separator)
            val file = File(root, child)
            if (!file.exists()) file.mkdirs()
            if (!file.exists()) throw Exception("mkdirs failed.")
            file
        } catch (e: Exception) {
            Log.e(NAMESPACE, e.message)
            e.printStackTrace()
            null
        }
    }

    private inline fun <reified T> doWithLock(
        things: () -> T?,
        crossinline finally: () -> Unit = {},
        readonly: Boolean = false
    ): T? {
        val lock: Lock = if (readonly) locker.readLock() else locker.writeLock()
        val holdLock: Boolean = lock.tryLock(1, TimeUnit.SECONDS)
        if (!holdLock) return null
        return try {
            things()
        } catch (e: Exception) {
            Log.e(NAMESPACE, e.message)
            e.printStackTrace()
            null
        } finally {
            Files.catcher { finally() }
            Files.catcher { lock.unlock() }
        }
    }

    fun keyValues(context: Context): List<String> {
        val dir: File = dir(context) ?: return emptyList()
        return doWithLock<List<String>>(things = {
            val map: File = Maps.get(dir)
            return Files.readLines(map)
        }, readonly = true) ?: emptyList()
    }

    fun get(context: Context, key: String): File? {
        if (key.isEmpty()) return null
        val dir: File = dir(context) ?: return null
        return doWithLock<File>(things = {
            val maps: File = Maps.get(dir)
            val line: String = Files.readLines(maps).find { it.startsWith(key) } ?: return null
            val keyValues: List<String> = line.split(SPLIT_FLAG)
            if (keyValues.size != 2) return null
            val value: String = keyValues[1]
            val file = File(dir, value)
            return if (file.exists() && file.isFile) file else null
        }, readonly = true)
    }

    fun put(context: Context, key: String, name: String, ins: InputStream): File? {
        if (key.isEmpty() || name.isEmpty()) return null
        val dir: File = dir(context) ?: return null
        return doWithLock<File>(things = {
            val map: File = Maps.backup(dir) ?: return null
            if (name == map.name) throw Exception("bucket map file name conflict.")
            val sFile = File(dir, name)
            //文件存储
            Files.save(sFile, ins)
            if (!sFile.exists() || !sFile.isFile) return null
            val line = "$key${SPLIT_FLAG}$name"
            //更新map文件
            val lines: List<String> = Files.readLines(map)
            val retains: List<String> = lines.filterNot { it.startsWith(key) }
            val newLines: MutableList<String> = LinkedList()
            newLines.addAll(retains)
            newLines.add(line)
            if (!Maps.update(dir, newLines)) {
                sFile.delete()
                return null
            }
            return sFile
        }, finally = { Maps.recover(dir) }, readonly = false)
    }

    fun del(context: Context, keys: List<String>): Boolean {
        if (keys.isEmpty()) return false
        val dir: File = dir(context) ?: return false
        return doWithLock<Boolean>(things = {
            val map: File = Maps.backup(dir) ?: return false
            val lines: List<String> = Files.readLines(map)
            val hits: List<String> = keys.map { key: String ->
                val line: String = lines.find { it.startsWith(key) } ?: return@map null
                val keyValues: List<String> = line.split(SPLIT_FLAG)
                if (keyValues.size == 2) {
                    val value: String = keyValues[1]
                    val file = File(dir, value)
                    if (file.exists() && file.isFile) file.delete()
                }
                return@map line
            }.filterNotNull()
            val newLines: List<String> = lines.filterNot { hits.contains(it) }
            if (newLines.size == lines.size) return false
            return Maps.update(dir, newLines)
        }, finally = { Maps.recover(dir) }, readonly = false) ?: false
    }

    fun remove(context: Context) {
        val dir: File = dir(context) ?: return
        doWithLock(things = {
            Files.deletes(dir)
        }, readonly = false)
    }

    /**
     * 映射相关接口
     */
    object Maps {
        private const val MAP_FILE = "bucket.map"
        private const val MAP_BAK_EXT = ".bak"

        fun get(dir: File): File {
            val file = File(dir, MAP_FILE)
            if (!file.exists()) file.createNewFile()
            return file
        }

        fun backup(dir: File): File? {
            val file: File = get(dir)
            val bak = File("${file.absolutePath}${MAP_BAK_EXT}")
            if (!file.renameTo(bak)) return null
            file.delete()
            if (file.exists()) {
                bak.delete()
                return null
            }
            return bak
        }

        fun recover(dir: File) {
            val file: File = get(dir)
            if (file.exists()) return
            val bak = File("${file.absolutePath}${MAP_BAK_EXT}")
            if (!bak.exists()) return
            bak.renameTo(file)
        }

        fun update(dir: File, lines: List<String>): Boolean {
            val file: File = get(dir)
            val bak = File("${file.absolutePath}${MAP_BAK_EXT}")
            Files.writeLines(file, lines)
            if (!file.exists() || !file.isFile || file.length() <= 0) {
                if (bak.renameTo(file)) bak.delete()
                return false
            }
            bak.delete()
            return true
        }
    }

    /**
     * 文件相关接口
     */
    object Files {
        private const val BUFFER_SIZE = 8 * 1024

        private fun closeSafely(closeable: Closeable?) {
            closeable ?: return
            try {
                closeable.close()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }

        fun save(file: File, ins: InputStream, bSize: Int = BUFFER_SIZE) {
            var outs: OutputStream? = null
            try {
                if (file.exists()) file.delete()
                outs = FileOutputStream(file)
                val buffer = ByteArray(bSize)
                var length: Int = -1
                while ({ length = ins.read(buffer);length }() > 0) {
                    outs.write(buffer, 0, length)
                }
                outs.flush()
            } catch (e: Exception) {
                e.printStackTrace()
            } finally {
                closeSafely(ins)
                closeSafely(outs)
            }
        }

        fun writeLines(file: File, lines: List<String>) {
            var writer: PrintWriter? = null
            try {
                writer = PrintWriter(BufferedWriter(FileWriter(file)))
                lines.forEach { line: String ->
                    writer.println(line)
                }
            } catch (e: Exception) {
                e.printStackTrace()
            } finally {
                closeSafely(writer)
            }
        }

        fun readLines(file: File): List<String> {
            var reader: BufferedReader? = null
            return try {
                reader = BufferedReader(FileReader(file))
                reader.readLines()
            } catch (e: Exception) {
                e.printStackTrace()
                emptyList()
            } finally {
                closeSafely(reader)
            }
        }

        fun deletes(file: File) {
            if (file.isFile) {
                file.delete()
                return
            }
            val fs: Array<File> = file.listFiles()
            if (fs.isEmpty()) {
                file.delete()
                return
            }
            for (f: File in fs) {
                deletes(f)
            }
        }

        fun catcher(block: () -> Unit) {
            try {
                block()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}