package dev.salavatov.sffs

/**
 * This class wraps information about a reference to a node
 * @param position actual position of a reference in a file. It isn't stored in a file in any form
 * (special value `INTANGIBLE` is used for references that aren't stored yet)
 * @param dataPosition actual position of node's metadata in a file
 */
sealed class NodeReference(val position: Long, val dataPosition: Long) {
    abstract val mark: Byte

    companion object {
        const val SIZE_BYTES: Long = (Byte.SIZE_BYTES + Long.SIZE_BYTES).toLong()
        const val INTANGIBLE = -239L // special value for a reference that isn't actually stored in a file
    }
}

class FolderReference(position: Long, dataPosition: Long) : NodeReference(position, dataPosition) {
    override val mark: Byte
        get() = MARK

    override fun toString(): String {
        return "FolderRef[$position->$dataPosition]"
    }

    companion object {
        const val MARK = 'F'.code.toByte()
    }
}

class FileReference(position: Long, dataPosition: Long) : NodeReference(position, dataPosition) {
    override val mark: Byte
        get() = MARK

    override fun toString(): String {
        return "FileRef[$position->$dataPosition]"
    }

    companion object {
        const val MARK = 'C'.code.toByte()
    }
}

/**
 * NodeMeta and its inheritors represent the metadata of the nodes
 */
sealed class NodeMeta(val name: String)

/**
 * @param childrenUsedSpace total number of bytes used to store children's data
 * @param children list of child node references
 */
class FolderMeta(name: String, val childrenUsedSpace: Long, val children: List<NodeReference>) : NodeMeta(name) {
    override fun toString(): String {
        return "Folder(\"$name\", children $childrenUsedSpace bytes, $children)"
    }
}

/**
 * @param fileSize the size of the content that is stored in this file
 */
class FileMeta(name: String, val fileSize: Long) : NodeMeta(name) {
    override fun toString(): String {
        return "File(\"$name\", content $fileSize bytes)"
    }
}

/**
 * NodeFragment comprises all information about how the node is stored in a file.
 * This includes the information about the reference to this node, metadata record, and link to the parent fragment.
 * @param metaSizeBytes size of metadata record in bytes (includes size of the reference & size of content for the file)
 * @property totalSizeBytes total size on disk that is used to store the node and its descendants
 */
sealed class NodeFragment(
    open val reference: NodeReference,
    open val meta: NodeMeta,
    val parentFragment: FolderFragment?,
    val metaSizeBytes: Long
) {
    abstract val totalSizeBytes: Long
}

class FolderFragment(
    override val reference: FolderReference,
    override val meta: FolderMeta,
    parentFragment: FolderFragment?,
    metaSizeBytes: Long
) : NodeFragment(reference, meta, parentFragment, metaSizeBytes) {
    // we must subtract here because children's node references are counted twice (in metaSizeBytes and in childrenUsedSpace)
    override val totalSizeBytes: Long = metaSizeBytes + meta.childrenUsedSpace - meta.children.size * NodeReference.SIZE_BYTES

    override fun toString(): String {
        return "$parentFragment -> FolderFragment{${reference}, ${meta}, record $metaSizeBytes bytes, total $totalSizeBytes bytes}"
    }
}

class FileFragment(
    override val reference: FileReference,
    override val meta: FileMeta,
    parentFragment: FolderFragment?,
    metaSizeBytes: Long
) : NodeFragment(reference, meta, parentFragment, metaSizeBytes) {
    override val totalSizeBytes: Long = metaSizeBytes

    override fun toString(): String {
        return "$parentFragment -> FileFragment{$reference, $meta, record $metaSizeBytes bytes}"
    }
}