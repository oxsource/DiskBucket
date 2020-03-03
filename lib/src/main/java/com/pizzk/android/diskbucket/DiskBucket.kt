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

    fun ls(context: Context): List<String> {
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
            val lines: List<String> = Files.readLines(maps)
            val line: String = lines.find { it.startsWith(key) } ?: return null
            val entity: Entity = Entity.parse(line) ?: return null
            val file = File(dir, entity.name)
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

    fun put(context: Context, key: String, name: String, ins: InputStream): File? {
        if (key.isEmpty() || name.isEmpty()) return null
        val dir: File = dir(context) ?: return null
        return doWithLock<File>(things = {
            val map: File = Maps.backup(dir) ?: return null
            if (name == map.name) throw Exception("bucket map file name conflict.")
            if (key.contains(SPLIT_FLAG)) throw Exception("object key contains '${SPLIT_FLAG}'.")
            if (name.contains(SPLIT_FLAG)) throw Exception("object name contains '${SPLIT_FLAG}'.")
            val sFile = File(dir, name)
            //文件存储
            Files.save(sFile, ins)
            if (!sFile.exists() || !sFile.isFile) return null
            val stamp: Long = System.currentTimeMillis()
            val line: String = Entity(key, name, 0, stamp).string(increase = false)
            //更新映射文件
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

    private fun deleteByKey(dir: File, keys: List<String>): Boolean {
        if (keys.isEmpty()) return false
        val map: File = Maps.backup(dir) ?: return false
        val lines: List<String> = Files.readLines(map)
        val hits: List<String> = keys.map { key: String ->
            val line: String = lines.find { it.startsWith(key) } ?: return@map null
            val entity: Entity = Entity.parse(line) ?: return@map null
            val file = File(dir, entity.name)
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
        doWithLock(things = { Files.deletes(dir) }, readonly = false)
    }

    fun gc(context: Context, bytes: Long, capacity: Int) {
        val dir: File = dir(context) ?: return
        doWithLock(things = {
            val map: File = Maps.get(dir)
            val lines: List<String> = Files.readLines(map)
            //第一步：删除不在bucket.map中的文件
            val fs: List<File> = dir.listFiles().filterNot {
                val e: String? = lines.find { line: String -> line.contains(it.name) }
                return@filterNot !e.isNullOrEmpty()
            }.filterNot { it.name == Maps.MAP_FILE }
            fs.forEach(Files::deletes)
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
                val file = File(dir, e.name)
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
        val name: String,
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
            return arrayOf(key, name, count, stamp).joinToString(separator = SPLIT_FLAG)
        }
    }

    /**
     * 映射相关接口
     */
    object Maps {
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