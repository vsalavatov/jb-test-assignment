package dev.salavatov.sffs

import dev.salavatov.multifs.vfs.AbsolutePath
import dev.salavatov.sync.RWMutex
import dev.salavatov.sync.withReadLock
import dev.salavatov.sync.withWriteLock
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.*

internal class NodeNotFoundException(message: String? = null, cause: Throwable? = null) : Exception(message, cause)

/**
 * FileFSAPI is responsible for maintaining thread-safe access to the file & performing some
 * high-level actions on the storage state.
 */
internal class FileFSAPI(val path: java.nio.file.Path) {
    private val STORAGE_MIN_EFFICIENCY_RATIO = 0.4
    private val rwMutex = RWMutex()
    private var initCheck = AtomicBoolean(false)

    init {
        if (path.notExists())
            Files.createFile(path)
    }

    suspend inline fun <T> withWriteLock(block: FileFSAPI.(FileController) -> T): T = rwMutex.withWriteLock {
        FileController(openFileForWrite()).use {
            if (!initCheck.get()) {
                if (it.size == 0L)
                    initializeFile(it)
                initCheck.set(true)
            }
            val result = block(it)
            defragment(it)
            result
        }
    }

    suspend inline fun <T> withReadLock(block: FileFSAPI.(FileController) -> T): T = rwMutex.withReadLock {
        FileController(openFileForRead()).use {
            block(it)
        }
    }

    private fun openFileForRead(): FileChannel = FileChannel.open(
        path,
        StandardOpenOption.READ,
//            ExtendedOpenOption.NOSHARE_WRITE, // not supported
//            ExtendedOpenOption.NOSHARE_DELETE,
    )

    private fun openFileForWrite(): FileChannel = FileChannel.open(
        path,
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
//            ExtendedOpenOption.NOSHARE_READ, // not supported
//            ExtendedOpenOption.NOSHARE_WRITE,
//            ExtendedOpenOption.NOSHARE_DELETE,
    )

    /**
     * Performs defragmentation of the storage file. Only triggers if the actual amount of stored data is less that
     * 40% of the storage file size. Current implementation needs linear to the number of nodes amount of memory to
     * store mappings.
     */
    fun defragment(fileController: FileController) {
        val root = fileController.getRootFragment()
        if (fileController.size * STORAGE_MIN_EFFICIENCY_RATIO <= root.totalSizeBytes)
            return

//        println("[DEFRAG] BEFORE: data size: ${root.totalSizeBytes}, storage size: ${fileController.size}")

        val defragFilePath = path.parent / (path.name + ".defrag")
        if (!defragFilePath.exists())
            defragFilePath.createFile()
        FileController(
            FileChannel.open(
                defragFilePath,
                StandardOpenOption.WRITE,
//                ExtendedOpenOption.NOSHARE_READ, // not supported
//                ExtendedOpenOption.NOSHARE_WRITE,
//                ExtendedOpenOption.NOSHARE_DELETE,
            )
        ).use { defragController ->
            val metadataPositionMapping = mutableMapOf<Long, Long>()
            val referencePositionMapping = mutableMapOf<Long, Long>()
            val queue = java.util.PriorityQueue<NodeFragment> { o1, o2 ->
                o1.reference.dataPosition.compareTo(o2.reference.dataPosition)
            }
            referencePositionMapping[0L] = 0L
            var currentPosition = NodeReference.SIZE_BYTES
            queue.add(root)
            while (queue.isNotEmpty()) {
                val node = queue.poll()
                metadataPositionMapping[node.reference.dataPosition] = currentPosition
                if (node is FolderFragment) {
                    val CHILD_REFS_OFFSET = Long.SIZE_BYTES + Int.SIZE_BYTES
                    node.meta.children.forEachIndexed { idx, ref ->
                        val childFrag = fileController.readFragment(ref, node)
                        referencePositionMapping[ref.position] =
                            currentPosition + CHILD_REFS_OFFSET + idx * NodeReference.SIZE_BYTES
                        queue.add(childFrag)
                    }
                }
                currentPosition += node.metaSizeBytes - NodeReference.SIZE_BYTES // reference size is accounted in metaSizeBytes
            }
            // this thing could be implemented smarter...

            defragController.putReference(FolderReference.MARK, NodeReference.SIZE_BYTES) as FolderReference
            queue.add(root)
            while (queue.isNotEmpty()) {
                val node = queue.poll()
                when (node) {
                    is FileFragment -> {
                        val newRef = FileReference(
                            referencePositionMapping[node.reference.position]!!,
                            metadataPositionMapping[node.reference.dataPosition]!!
                        )
                        val data = fileController.readFileContent(node)
                        defragController.putFileFragment(newRef, node.meta.name, data, node.parentFragment)
                    }
                    is FolderFragment -> {
                        val newRef = FolderReference(
                            referencePositionMapping[node.reference.position]!!,
                            metadataPositionMapping[node.reference.dataPosition]!!
                        )
                        defragController.putFolderFragment(
                            newRef,
                            node.meta.name,
                            node.meta.childrenUsedSpace,
                            node.meta.children.map {
                                when (it) {
                                    is FileReference -> {
                                        FileReference(
                                            referencePositionMapping[it.position]!!,
                                            metadataPositionMapping[it.dataPosition]!!
                                        )
                                    }
                                    is FolderReference -> {
                                        FolderReference(
                                            referencePositionMapping[it.position]!!,
                                            metadataPositionMapping[it.dataPosition]!!
                                        )
                                    }
                                }
                            },
                            node
                        )

                        node.meta.children.forEach { ref ->
                            val childFrag = fileController.readFragment(ref, node)
                            queue.add(childFrag)
                        }
                    }
                }
            }
//            println("[DEFRAG] AFTER: data size: ${root.totalSizeBytes}, storage size: ${defragController.size}")
        }

        fileController.close()
        defragFilePath.moveTo(path, true)
    }

    private fun initializeFile(fc: FileController) {
        val rootRef = fc.putReference(FolderReference.MARK, NodeReference.SIZE_BYTES) as FolderReference
        fc.putFolderFragment(rootRef, "", 0, emptyList(), null)
        fc.flush()
    }

    private fun NodeFragment.assertFolder(
        message: String? = "expected node fragment to represent a folder"
    ): FolderFragment =
        this as? FolderFragment ?: throw IllegalStateException(message)

    private fun FileController.getRootFragment(): FolderFragment =
        readFragment(0L, null)
            .assertFolder("expected root reference at the beginning of the file")

    /**
     * Retrieves a node fragment that corresponds to the given absolute path
     */
    fun navigate(fileController: FileController, path: AbsolutePath): NodeFragment {
        val rootFragment = fileController.getRootFragment()
        var currentFolderFragment: NodeFragment = rootFragment
        for (part in path) {
            val folderNode = currentFolderFragment.assertFolder()
            currentFolderFragment = folderNode.meta.children
                .map { fileController.readFragment(it, folderNode) }
                .find { it.meta.name == part }
                ?: throw NodeNotFoundException("$path wasn't found")
        }
        return currentFolderFragment
    }

    /**
     * Adds child fragment to its parent's child-list and updates space usage statistics.
     * @return updated folderFragment of the parent
     */
    fun addChildNode(
        fileController: FileController,
        folderFragment: FolderFragment,
        childFragment: NodeFragment
    ): FolderFragment {
        // rewrite reference
        fileController.position = folderFragment.reference.position
        val newRef = fileController.putReference(FolderReference.MARK, fileController.size) as FolderReference
        // write new metadata record
        val newMeta = folderFragment.meta.let {
            FolderMeta(
                it.name,
                it.childrenUsedSpace + childFragment.totalSizeBytes,
                it.children + childFragment.reference
            )
        }
        val newFrag = fileController.putFolderFragment(
            newRef,
            newMeta.name,
            newMeta.childrenUsedSpace,
            newMeta.children,
            folderFragment.parentFragment
        )
        // update parent stats
        newFrag.parentFragment?.let {
            val usedSpaceDelta = newFrag.totalSizeBytes - folderFragment.totalSizeBytes
            fileController.propagateUsedSpaceChange(it, usedSpaceDelta)
        }
        return newFrag
    }

    /**
     * Removes child fragment from its parent's child-list and updates space usage statistics.
     * @param childFragment must be a child of `folderFragment` before invocation
     * @return updated folderFragment of the parent
     */
    fun removeChildNode(
        fileController: FileController,
        folderFragment: FolderFragment,
        childFragment: NodeFragment
    ): FolderFragment {
        // no need to rewrite the reference, because new metadata record is certainly smaller than an old one
        // write new metadata record'
        val newMeta = folderFragment.meta.let { meta ->
            FolderMeta(
                meta.name,
                meta.childrenUsedSpace - childFragment.totalSizeBytes,
                meta.children.filter { it.dataPosition != childFragment.reference.dataPosition }
            )
        }
        assert(newMeta.children.size + 1 == folderFragment.meta.children.size)
        val newFrag = fileController.putFolderFragment(
            folderFragment.reference,
            newMeta.name,
            newMeta.childrenUsedSpace,
            newMeta.children,
            folderFragment.parentFragment
        )
        // update parent stats
        newFrag.parentFragment?.let {
            val usedSpaceDelta = newFrag.totalSizeBytes - folderFragment.totalSizeBytes
            fileController.propagateUsedSpaceChange(it, usedSpaceDelta)
        }
        return newFrag
    }
}