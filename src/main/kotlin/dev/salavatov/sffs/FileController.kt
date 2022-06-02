package dev.salavatov.sffs

import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.channels.Channels
import java.nio.channels.FileChannel

/**
 * This class provides methods for low-level manipulation of the SingleFileFS state
 */
internal class FileController(private val fileChannel: FileChannel) : AutoCloseable {
    private val reader = DataInputStream(Channels.newInputStream(fileChannel))
    private val writer = DataOutputStream(Channels.newOutputStream(fileChannel))

    var position: Long
        get() = fileChannel.position()
        set(value) {
            fileChannel.position(value)
        }

    val size: Long
        get() = fileChannel.size()

    fun flush() {
        writer.flush()
    }

    fun readFragment(referencePosition: Long, parentFragment: FolderFragment?): NodeFragment {
        if (referencePosition == 0L && size == 0L) { // case for empty file & root query
            return FolderFragment(
                FolderReference(NodeReference.INTANGIBLE, NodeReference.INTANGIBLE),
                FolderMeta("", 0L, emptyList()),
                null,
                0L
            )
        }
        position = referencePosition
        return readFragment(readReference(), parentFragment)
    }

    fun readFragment(reference: NodeReference, parentFragment: FolderFragment?): NodeFragment {
        return when (reference) {
            is FileReference -> readFileFragment(reference, parentFragment)
            is FolderReference -> readFolderFragment(reference, parentFragment)
        }
    }

    // reference: [mark (folder/file), position of data]          [actual metadata]
    //                                      \----------------------/
    fun readReference(): NodeReference {
        val refPosition = position
        val mark = reader.readByte()
        val dataPosition = reader.readLong()
        return when (mark) {
            FileReference.MARK -> {
                FileReference(refPosition, dataPosition)
            }
            FolderReference.MARK -> {
                FolderReference(refPosition, dataPosition)
            }
            else -> throw IllegalStateException("invalid mark")
        }
    }

    fun putReference(mark: Byte, dataPosition: Long): NodeReference {
        val refPosition = position
        writer.writeByte(mark.toInt())
        writer.writeLong(dataPosition)
        return when (mark) {
            FileReference.MARK -> FileReference(refPosition, dataPosition)
            FolderReference.MARK -> FolderReference(refPosition, dataPosition)
            else -> throw IllegalStateException("invalid mark")
        }
    }

    // [name, fileSize, content]
    fun readFileFragment(
        reference: FileReference,
        parentFragment: FolderFragment?
    ): FileFragment {
        position = reference.dataPosition
        val name = reader.readUTF()
        val fileSize = reader.readLong()
        val metaSizeBytes = position - reference.dataPosition + fileSize + NodeReference.SIZE_BYTES
        return FileFragment(reference, FileMeta(name, fileSize), parentFragment, metaSizeBytes)
    }

    fun readFileContent(fileFragment: FileFragment): ByteArray {
        position = fileFragment.reference.dataPosition
        reader.readUTF() // skip name
        val fileSize = reader.readLong()
        val result = ByteArray(fileSize.toInt())
        if (reader.readNBytes(result, 0, fileSize.toInt()) != fileSize.toInt()) {
            throw IllegalStateException("couldn't read the whole file")
        }
        return result
    }

    fun putFileFragment(
        reference: FileReference,
        name: String,
        data: ByteArray,
        parentFragment: FolderFragment?
    ): FileFragment {
        position = reference.dataPosition
        writer.writeUTF(name)
        val fileSize = data.size.toLong()
        writer.writeLong(fileSize)
        writer.write(data)
        val metaSize = position - reference.dataPosition + NodeReference.SIZE_BYTES
        return FileFragment(reference, FileMeta(name, fileSize), parentFragment, metaSize)
    }

    fun updateFileContent(
        reference: FileReference,
        data: ByteArray,
        parentFragment: FolderFragment?
    ): FileFragment {
        position = reference.dataPosition
        val name = reader.readUTF()
        val fileSizePos = position
        val fileSizeBefore = reader.readLong()
        val fileSize = data.size.toLong()
        if (fileSize <= fileSizeBefore) { // rewrite existing content
            position = fileSizePos
            writer.writeLong(fileSize)
            writer.write(data)
            val metaSizeBytes = position - reference.dataPosition
            propagateUsedSpaceChange(parentFragment, fileSize - fileSizeBefore)
            return FileFragment(reference, FileMeta(name, fileSize), parentFragment, metaSizeBytes)
        }
        // new content is larger -- make new metadata record and update reference
        position = reference.position
        val newRef = putReference(FileReference.MARK, size) as FileReference
        // write new metadata record
        val newFrag = putFileFragment(newRef, name, data, parentFragment)
        propagateUsedSpaceChange(parentFragment, fileSize - fileSizeBefore)
        return newFrag
    }

    // [children used space (Long), children count (Int), child-reference..., UTF8 name]
    fun readFolderFragment(
        reference: FolderReference,
        parentFragment: FolderFragment?
    ): FolderFragment {
        position = reference.dataPosition
        val childrenUsedSpace = reader.readLong()
        val childrenCount = reader.readInt()
        val childrenRefs = List(childrenCount) { readReference() }
        val name = reader.readUTF()
        val metaSizeBytes = position - reference.dataPosition + NodeReference.SIZE_BYTES
        return FolderFragment(
            reference,
            FolderMeta(name, childrenUsedSpace, childrenRefs),
            parentFragment,
            metaSizeBytes
        )
    }

    fun putFolderFragment(
        reference: FolderReference,
        name: String,
        childrenUsedSpace: Long,
        children: List<NodeReference>,
        parentFragment: FolderFragment?
    ): FolderFragment {
        position = reference.dataPosition
        writer.writeLong(childrenUsedSpace)
        writer.writeInt(children.size)
        children.forEach { putReference(it.mark, it.dataPosition) }
        writer.writeUTF(name)
        val metaSizeBytes = position - reference.dataPosition + NodeReference.SIZE_BYTES
        return FolderFragment(reference, FolderMeta(name, childrenUsedSpace, children), parentFragment, metaSizeBytes)
    }

    fun propagateUsedSpaceChange(fragment: FolderFragment?, delta: Long) {
        var current = fragment
        while (current != null) {
            position = current.reference.dataPosition
            val childrenUsedSpace = reader.readLong()
            position = current.reference.dataPosition
            writer.writeLong(childrenUsedSpace + delta)
            current = current.parentFragment
        }
    }

    override fun close() {
        if (writer.size() > 0)
            writer.flush()
        fileChannel.close()
    }
}
