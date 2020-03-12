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
        private const val MAX_KEY_LEN = 48
        private const val MAX_META_LEN = 64
        private const val EXT = ".ds"
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

    private fun catcher(block: () -> Unit) {
        try {
            block()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun withExt(key: String) = "$key.$EXT"

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
            catcher { finally() }
            catcher { lock.unlock() }
        }
    }

    fun ls(context: Context): List<String> {
        val dir: File = dir(context) ?: return emptyList()
        return doWithLock<List<String>>(things = {
            val map: File = Maps.get(dir)
            return map.readLines()
        }, readonly = true) ?: emptyList()
    }

    fun get(context: Context, key: String): File? {
        if (key.isEmpty()) return null
        val dir: File = dir(context) ?: return null
        return doWithLock<File>(things = {
            val maps: File = Maps.get(dir)
            val lines: List<String> = maps.readLines()
            val line: String = lines.find { it.startsWith(key) } ?: return null
            val entity: Entity = Entity.parse(line) ?: return null
            val file = File(dir, withExt(entity.key))
            if (!file.exists() || !file.isFile) return null
            //更新引用计数
            Maps.backup(dir)
            val newLine: String = entity.string(increase = true)
            val newLines: MutableList<String> = LinkedList()
            newLines.addAll(lines)
            val index: Int = lines.indexOf(line)
            newLines[index] = newLine
            Maps.update(dir, newLines)
            return file
        }, finally = { Maps.recover(dir) }, readonly = false)
    }

    fun put(context: Context, key: String, ins: InputStream, meta: String = ""): File? {
        if (key.trim().isEmpty()) return null
        if (key.length > MAX_KEY_LEN) throw Exception("key is too long.")
        if (key.contains(SPLIT_FLAG)) throw Exception("key can't contains '${SPLIT_FLAG}'.")
        if (meta.length > MAX_META_LEN) throw Exception("meta is too long.")
        val dir: File = dir(context) ?: return null
        return doWithLock<File>(things = {
            val map: File = Maps.backup(dir) ?: return null
            //文件存储
            val sFile = File(dir, withExt(key))
            sFile.outputStream().use { ins.copyTo(it) }
            if (!sFile.exists() || !sFile.isFile) return null
            val stamp: Long = System.currentTimeMillis()
            val line: String = Entity(key, meta, 0, stamp).string(increase = false)
            //更新映射文件
            val lines: List<String> = map.readLines()
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

    private fun deleteByKey(dir: File, keys: List<String>): Boolean {
        if (keys.isEmpty()) return false
        val map: File = Maps.backup(dir) ?: return false
        val lines: List<String> = map.readLines()
        val hits: List<String> = keys.map { key: String ->
            val line: String = lines.find { it.startsWith(key) } ?: return@map null
            val entity: Entity = Entity.parse(line) ?: return@map null
            val file = File(dir, withExt(entity.key))
            if (file.exists() && file.isFile) file.delete()
            return@map line
        }.filterNotNull()
        if (hits.isEmpty()) return false
        val newLines: List<String> = lines.filterNot { hits.contains(it) }
        return Maps.update(dir, newLines)
    }

    fun del(context: Context, keys: List<String>): Boolean {
        if (keys.isEmpty()) return false
        val dir: File = dir(context) ?: return false
        return doWithLock<Boolean>(things = {
            return deleteByKey(dir, keys)
        }, finally = { Maps.recover(dir) }, readonly = false) ?: false
    }

    fun clean(context: Context) {
        val dir: File = dir(context) ?: return
        doWithLock(things = { dir.deleteRecursively() }, readonly = false)
    }

    fun gc(context: Context, bytes: Long, capacity: Int) {
        val dir: File = dir(context) ?: return
        doWithLock(things = {
            val map: File = Maps.get(dir)
            val lines: List<String> = map.readLines()
            //第一步：删除不在bucket.map中的文件
            val fs: List<File> = dir.listFiles().filterNot {
                val e: String? = lines.find { line: String -> line.contains(it.name) }
                return@filterNot !e.isNullOrEmpty()
            }.filterNot { it.name == Maps.MAP_FILE }
            fs.forEach(File::deleteRecursively)
            //第二步:通过计算使用率排名，将超出文件大小及项目数的文件删除
            val now: Long = System.currentTimeMillis()
            val origins: List<Entity> = lines.mapNotNull { line: String -> Entity.parse(line) }
            //使用率降序排序
            val es: List<Entity> = origins.sortedByDescending { e: Entity ->
                val duration: Double = (now - e.stamp) / 1.0
                return@sortedByDescending e.count / duration
            }
            val delKeys: MutableList<String> = LinkedList()
            //对排名落后，超出Bucket文件尺寸限制或 bucket.map 中有记录但文件不存在的进行清理
            var totalSize: Long = 0
            val cLines: List<Entity> = es.filter { e: Entity ->
                val file = File(dir, withExt(e.key))
                if (!file.exists() || !file.isFile || totalSize > bytes) {
                    delKeys.add(e.key)
                    return@filter false
                }
                totalSize += file.length()
                return@filter true
            }
            //超出文件项目数的进行清理
            if (cLines.size > capacity) {
                delKeys.addAll(cLines.subList(capacity, cLines.size).map(Entity::key))
            }
            //删除文件
            deleteByKey(dir, delKeys)
        }, finally = { Maps.recover(dir) }, readonly = false)
    }

    /**
     * 记录实体
     */
    class Entity constructor(
        val key: String,
        val meta: String,
        val count: Int,
        val stamp: Long
    ) {
        companion object {
            fun parse(line: String): Entity? {
                val parts: List<String> = line.split(SPLIT_FLAG)
                if (parts.size != 4) return null
                val key: String = parts[0]
                val name: String = parts[1]
                val count: Int = parts[2].toIntOrNull() ?: 0
                val stamp: Long = parts[3].toLongOrNull() ?: 0
                return Entity(key, name, count, stamp)
            }
        }

        fun string(increase: Boolean = false): String {
            val count: Int = if (increase) count + 1 else count
            return arrayOf(key, meta, count, stamp).joinToString(separator = SPLIT_FLAG)
        }
    }

    /**
     * 映射相关接口
     */
    private object Maps {
        const val MAP_FILE = "bucket.map"
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
            file.printWriter().use { lines.forEach(it::println) }
            if (!file.exists() || !file.isFile || file.length() <= 0) {
                if (bak.renameTo(file)) bak.delete()
                return false
            }
            bak.delete()
            return true
        }
    }
}