package dev.salavatov.sffs

import dev.salavatov.multifs.vfs.*
import dev.salavatov.sffs.SingleFileFS.Companion.checkNodeNotExists

/**
 * SingleFileFS is a VFS that stores everything in a single file of the local filesystem.
 * All operations are thread-safe.
 */
class SingleFileFS(val path: java.nio.file.Path) : VFS<SingleFileFSFile, SingleFileFSFolder> {
    internal val api = FileFSAPI(path)

    override suspend fun copy(file: File, newParent: Folder, newName: PathPart?, overwrite: Boolean): SingleFileFSFile {
        if (newParent !is SingleFileFSFolder)
            throw SingleFileFSException("can't operate on folders that don't belong to SingleFileFS")
        val targetName = newName ?: file.name
        if (newParent == file.parent && targetName == file.name) {
            // no op
            return file.fromGeneric()
        }
        return genericCopy(file, newParent, targetName, overwrite, onExistsThrow = {
            throw SingleFileFSFileExistsException("couldn't copy ${file.absolutePath} to ${it.absolutePath}: target file exists")
        })
    }

    override suspend fun move(file: File, newParent: Folder, newName: PathPart?, overwrite: Boolean): SingleFileFSFile {
        if (newParent !is SingleFileFSFolder)
            throw SingleFileFSException("can't operate on folders that don't belong to SingleFileFS")
        val targetName = newName ?: file.name
        if (newParent == file.parent && targetName == file.name) {
            // no op
            return file.fromGeneric()
        }
        return genericMove(file, newParent, targetName, overwrite, onExistsThrow = {
            throw SingleFileFSFileExistsException("couldn't move ${file.absolutePath} to ${it.absolutePath}: target file exists")
        })
    }

    override fun representPath(path: AbsolutePath): String = path.joinToString("/", "/")

    override val root: SingleFileFSFolder
        get() = SingleFileFSFolder(this, "", null)

    private fun File.fromGeneric(): SingleFileFSFile =
        this as? SingleFileFSFile
            ?: throw SingleFileFSException("expected the file to be part of the SingleFileFS (class: ${this.javaClass.kotlin.qualifiedName}")

    companion object {
        internal fun FileFSAPI.checkNodeNotExists(
            fileController: FileController,
            path: AbsolutePath,
            onExistsMessage: String = "$path exists"
        ) {
            try {
                navigate(fileController, path)
            } catch (e: NodeNotFoundException) {
                return
            }
            throw SingleFileFSNodeExistsException(onExistsMessage)
        }
    }
}

sealed class SingleFileFSNode(
    val fs: SingleFileFS,
    override val name: String,
    private val parent_: SingleFileFSFolder?
) : VFSNode {
    override val parent: SingleFileFSFolder
        get() = parent_ ?: this as SingleFileFSFolder

    override val absolutePath: AbsolutePath
        get() = if (this == parent) {
            emptyList()
        } else {
            parent.absolutePath + name
        }
}

open class SingleFileFSFolder(fs: SingleFileFS, name: String, parent: SingleFileFSFolder?) :
    SingleFileFSNode(fs, name, parent), Folder {

    override suspend fun createFile(name: PathPart): SingleFileFSFile =
        fs.api.withWriteLock { fc ->
            try {
                val folderFrag = navigate(fc, absolutePath) as? FolderFragment
                    ?: throw SingleFileFSFolderNotFoundException("$absolutePath is not a folder")
                checkNodeNotExists(fc, absolutePath + name, "$name already exists in $absolutePath")
                val fileFrag = fc.putFileFragment(
                    FileReference(NodeReference.INTANGIBLE, fc.size),
                    name,
                    ByteArray(0),
                    folderFrag
                )
                addChildNode(fc, folderFrag, fileFrag)
                SingleFileFSFile(fs, name, this@SingleFileFSFolder)
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFolderNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't create file $name at $absolutePath", e)
            }
        }

    override suspend fun createFolder(name: PathPart): SingleFileFSFolder =
        fs.api.withWriteLock { fc ->
            try {
                val folderFrag = navigate(fc, absolutePath) as? FolderFragment
                    ?: throw SingleFileFSFolderNotFoundException("$absolutePath is not a folder")
                checkNodeNotExists(fc, absolutePath + name, "$name already exists in $absolutePath")
                val subFolder = fc.putFolderFragment(
                    FolderReference(NodeReference.INTANGIBLE, fc.size),
                    name,
                    0L,
                    emptyList(),
                    folderFrag
                )
                addChildNode(fc, folderFrag, subFolder)
                SingleFileFSFolder(fs, name, this@SingleFileFSFolder)
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFolderNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't create folder $name at $absolutePath", e)
            }
        }

    override suspend fun listFolder(): List<SingleFileFSNode> =
        fs.api.withReadLock { fc ->
            try {
                val folderFrag = navigate(fc, absolutePath) as? FolderFragment
                    ?: throw SingleFileFSFolderNotFoundException("$absolutePath is not a folder")
                folderFrag.meta.children.map {
                    val childFrag = fc.readFragment(it, folderFrag)
                    when (childFrag) {
                        is FileFragment -> SingleFileFSFile(fs, childFrag.meta.name, this@SingleFileFSFolder)
                        is FolderFragment -> SingleFileFSFolder(fs, childFrag.meta.name, this@SingleFileFSFolder)
                    }
                }
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFolderNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't list folder $absolutePath", e)
            }
        }

    override suspend fun div(path: PathPart): SingleFileFSFolder =
        fs.api.withReadLock { fc ->
            try {
                val frag = navigate(fc, absolutePath + path) as? FolderFragment
                    ?: throw SingleFileFSFolderNotFoundException("${absolutePath + path} is not a folder")
                return SingleFileFSFolder(fs, frag.meta.name, this@SingleFileFSFolder)
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFolderNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't find child folder $path at $absolutePath", e)
            }
        }

    override suspend fun rem(path: PathPart): SingleFileFSFile =
        fs.api.withReadLock { fc ->
            try {
                val frag = navigate(fc, absolutePath + path) as? FileFragment
                    ?: throw SingleFileFSFileNotFoundException("${absolutePath + path} is not a file")
                return SingleFileFSFile(fs, frag.meta.name, this@SingleFileFSFolder)
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFileNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't find child file $path at $absolutePath", e)
            }
        }

    override suspend fun remove(recursively: Boolean) =
        fs.api.withWriteLock { fc ->
            try {
                val folderFrag = navigate(fc, absolutePath) as? FolderFragment
                    ?: throw SingleFileFSFolderNotFoundException("$absolutePath is not a folder")
                if (!recursively && folderFrag.meta.children.isNotEmpty())
                    throw SingleFileFSFolderNotEmptyException("folder $absolutePath is not empty")
                folderFrag.parentFragment?.let { parent ->
                    removeChildNode(fc, parent, folderFrag)
                }
                Unit
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFolderNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't remove folder $absolutePath", e)
            }
        }
}

class SingleFileFSFile(fs: SingleFileFS, name: String, parent: SingleFileFSFolder?) :
    SingleFileFSNode(fs, name, parent), File {
    override suspend fun getSize(): Long =
        fs.api.withReadLock { fc ->
            try {
                val frag = fs.api.navigate(fc, absolutePath) as? FileFragment
                    ?: throw SingleFileFSFileNotFoundException("$absolutePath is a folder")
                frag.meta.fileSize
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFileNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't get size of $absolutePath", e)
            }
        }

    override suspend fun read(): ByteArray =
        fs.api.withReadLock { fc ->
            try {
                val frag = fs.api.navigate(fc, absolutePath) as? FileFragment
                    ?: throw SingleFileFSFileNotFoundException("$absolutePath is not a file")
                fc.readFileContent(frag)
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFileNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't read file $absolutePath", e)
            }
        }

    override suspend fun remove() =
        fs.api.withWriteLock { fc ->
            try {
                val fileFrag = navigate(fc, absolutePath) as? FileFragment
                    ?: throw SingleFileFSFileNotFoundException("$absolutePath is not a file")
                removeChildNode(fc, fileFrag.parentFragment!!, fileFrag)
                Unit
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFileNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't remove file $absolutePath", e)
            }
        }

    override suspend fun write(data: ByteArray) =
        fs.api.withWriteLock { fc ->
            try {
                val fileFrag = navigate(fc, absolutePath) as? FileFragment
                    ?: throw SingleFileFSFileNotFoundException("$absolutePath is not a file")
                fc.updateFileContent(fileFrag.reference, data, fileFrag.parentFragment)
                Unit
            } catch (e: SingleFileFSException) {
                throw e
            } catch (e: NodeNotFoundException) {
                throw SingleFileFSFileNotFoundException("$absolutePath not found")
            } catch (e: Throwable) {
                throw SingleFileFSException("couldn't write to file $absolutePath", e)
            }
        }
}