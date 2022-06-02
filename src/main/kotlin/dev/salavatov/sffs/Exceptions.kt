package dev.salavatov.sffs

import dev.salavatov.multifs.vfs.*

open class SingleFileFSException(message: String? = null, cause: Throwable? = null) : VFSException(message, cause)

open class SingleFileFSFolderNotEmptyException(message: String? = null, cause: Throwable? = null) :
    SingleFileFSException(message, cause), VFSFolderNotEmptyException

open class SingleFileFSNodeNotFoundException(message: String? = null, cause: Throwable? = null) :
    SingleFileFSException(message, cause), VFSNodeNotFoundException

open class SingleFileFSFolderNotFoundException(message: String? = null, cause: Throwable? = null) :
    SingleFileFSNodeNotFoundException(message, cause), VFSFolderNotFoundException

open class SingleFileFSFileNotFoundException(message: String? = null, cause: Throwable? = null) :
    SingleFileFSNodeNotFoundException(message, cause), VFSFileNotFoundException

open class SingleFileFSNodeExistsException(message: String? = null, cause: Throwable? = null) :
    SingleFileFSException(message, cause), VFSNodeExistsException

open class SingleFileFSFileExistsException(message: String? = null, cause: Throwable? = null) :
    SingleFileFSNodeExistsException(message, cause), VFSFileExistsException