import dev.salavatov.sffs.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.*
import java.nio.file.Paths
import kotlin.io.path.deleteIfExists
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SFFSTest {
    val tmpfile = Paths.get("./test.db")

    lateinit var fs: SingleFileFS

    @BeforeEach
    fun `make tmp db`() = runBlocking {
        fs = SingleFileFS(tmpfile)
    }

    @AfterEach
    fun `rm tmp db`() {
        tmpfile.deleteIfExists()
    }

    suspend inline fun <reified T> assertThrowsBlocking(block: () -> Unit): Unit {
        try {
            block()
        } catch (e: Throwable) {
            assert(e is T)
            return
        }
        assert(false) { "no exception of type ${T::class.java.canonicalName} caught" }
    }

    val sampleData = "sample data".toByteArray()

    @Nested
    @DisplayName("Basic tests")
    inner class BasicTests {
        @Test
        fun `empty fs`() = runBlocking {
            assert(fs.root.listFolder().isEmpty())
            assertEquals(fs.root.name, "")
            assertContentEquals(fs.root.absolutePath, emptyList())
        }

        @Test
        fun `write read one file`() = runBlocking {
            val s = fs.root.createFile("sample")
            assertContentEquals(s.read(), ByteArray(0))
            s.write(sampleData)
            assertContentEquals(s.read(), sampleData)
        }

        @Test
        fun `multiple rewrite of one file`() = runBlocking {
            val s = fs.root.createFile("sample")
            for (i in 10..20) {
                val data = ByteArray(i) { it.toByte() }
                s.write(data)
                assertContentEquals(s.read(), data)
                assertEquals(s.getSize(), i.toLong())
            }
        }

        @Test
        fun `multiple create delete`() = runBlocking {
            for (i in 1..10) {
                val s = fs.root.createFile("sample")
                val data = ByteArray(i) { it.toByte() }
                s.write(data)
                assertContentEquals(s.read(), data)
                s.remove()
            }
            assertThrowsBlocking<SingleFileFSFileNotFoundException>() { fs.root % "sample" }
        }

        @Test
        fun `file in a subfolder`() = runBlocking {
            val rootfile = fs.root.createFile("rootfile")

            val subf = fs.root.createFolder("subfolder")
            val subsubf = subf.createFolder("subsubfolder")
            val subsubfile = subsubf.createFile("subsubfile")
            val subfile = subf.createFile("subfile")
            val aboba = subf.createFolder("aboba")
            val abobafile = aboba.createFile("abobafile")

            /*
            root - subfolder - subsubfolder - subsubfile
               |        |------ aboba - abobafile
               |        |--- subfile
               |--- rootfile
            */

            for (i in 0..5) {
                val add = ByteArray(i) { it.toByte() }
                subsubfile.write(sampleData.copyOf(sampleData.size - i))
                abobafile.write(sampleData + sampleData + add)
                subfile.write(sampleData + sampleData + sampleData + add)
                rootfile.write(sampleData + "omegalul".toByteArray() + add)
            }

            assertContentEquals(fs.root.listFolder().map { it.name }, listOf("rootfile", "subfolder"))
            assertContentEquals(subf.listFolder().map { it.name }, listOf("subsubfolder", "subfile", "aboba"))
            assertContentEquals(subsubf.listFolder().map { it.name }, listOf("subsubfile"))
            assertContentEquals(aboba.listFolder().map { it.name }, listOf("abobafile"))

            val finalAdd = ByteArray(5) { it.toByte() }
            assertContentEquals(subsubfile.read(), sampleData.copyOf(sampleData.size - 5))
            assertContentEquals(abobafile.read(), sampleData + sampleData + finalAdd)
            assertContentEquals(subfile.read(), sampleData + sampleData + sampleData + finalAdd)
            assertContentEquals(rootfile.read(), sampleData + "omegalul".toByteArray() + finalAdd)
        }

        @Test
        fun `copy works`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val file = folder.createFile("file")
            file.write(sampleData)
            val copy = fs.copy(file, fs.root, "fff")
            assertContentEquals(copy.read(), sampleData)
            assertContentEquals(file.read(), sampleData)
        }

        @Test
        fun `copy throws if exists`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val file = folder.createFile("file")
            file.write(sampleData)
            fs.root.createFile("fff")
            assertThrowsBlocking<SingleFileFSFileExistsException> {
                fs.copy(file, fs.root, "fff")
            }
        }

        @Test
        fun `copy doesnt throw if exists and overwrite enabled`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val file = folder.createFile("file")
            file.write(sampleData)
            fs.root.createFile("fff")
            val copy = fs.copy(file, fs.root, "fff", true)
            assertContentEquals(copy.read(), sampleData)
            assertContentEquals(file.read(), sampleData)
        }

        @Test
        fun `move works`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val file = folder.createFile("file")
            file.write(sampleData)
            val move = fs.move(file, fs.root, "fff")
            assertContentEquals(move.read(), sampleData)
            assertThrowsBlocking<SingleFileFSFileNotFoundException> { file.read() }
        }

        @Test
        fun `move throws if exists`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val file = folder.createFile("file")
            file.write(sampleData)
            fs.root.createFile("fff")
            assertThrowsBlocking<SingleFileFSFileExistsException> {
                fs.move(file, fs.root, "fff")
            }
        }

        @Test
        fun `move doesnt throw if exists and overwrite enabled`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val file = folder.createFile("file")
            file.write(sampleData)
            fs.root.createFile("fff")
            val move = fs.move(file, fs.root, "fff", true)
            assertContentEquals(move.read(), sampleData)
            assertThrowsBlocking<SingleFileFSFileNotFoundException> { file.read() }
        }

        @Test
        fun `non-existent file lookup throws`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            folder.createFile("file")
            fs.root.createFile("rootfile")
            fs.root / "folder"
            fs.root / "folder" % "file"
            fs.root % "rootfile"
            assertThrowsBlocking<SingleFileFSFileNotFoundException> { fs.root % "smth" }
            assertThrowsBlocking<SingleFileFSFileNotFoundException> { fs.root / "folder" % "smth" }
            assertThrowsBlocking<SingleFileFSFolderNotFoundException> { fs.root / "smth" }
            assertThrowsBlocking<SingleFileFSFolderNotFoundException> { fs.root / "folder" / "subfolder" }
        }

        @Test
        fun `remove of non-empty folder throws`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val kek = folder.createFolder("kek")
            assertThrowsBlocking<SingleFileFSFolderNotEmptyException> { folder.remove() }
            kek.remove()
            folder.remove()
        }

        @Test
        fun `remove of non-empty folder doesnt throw if recursively is set`() = runBlocking {
            val folder = fs.root.createFolder("folder")
            val kek = folder.createFolder("kek")
            folder.remove(true)
            assertThrowsBlocking<SingleFileFSFolderNotFoundException> { kek.listFolder() }
            assertThrowsBlocking<SingleFileFSFolderNotFoundException> { folder.listFolder() }
        }
    }

    @Nested
    @DisplayName("Multithreaded test cases")
    inner class MultithreadedTests {
        @OptIn(DelicateCoroutinesApi::class)
        @Test
        fun `stress 1`() = runBlocking(newFixedThreadPoolContext(4, "aboba")) {
            val files = listOf("a", "b", "c", "d").map { fs.root.createFile(it).also { it.write(ByteArray(1) ) } }
            List(4) { idx ->
                launch {
                    var cntDiff = 0
                    val data = ByteArray(idx + 1) { idx.toByte() }
                    for (i in 0..3000) {
                        val file = files[Random.nextInt(0..3)]
                        val doWrite = Random.nextDouble(0.0, 1.0) < 0.2
                        if (doWrite)
                            file.write(data)
                        val res = file.read()
                        assertEquals(res.size, res[0] + 1)
                        assertContentEquals(res, ByteArray(res[0] + 1) { res[0] })
                        if (doWrite && res[0] != idx.toByte()) cntDiff++
                        // avg cntDiff is around ~65
                        // if nThreads = 0 => cntDiff = 0
                    }
                    println("$idx diff count: $cntDiff")
                }
            }.joinAll()
        }
    }
}