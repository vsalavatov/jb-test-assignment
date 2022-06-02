package dev.salavatov.sync

import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/**
 * Read/Write mutex with writers prioritization
 * Based on https://github.com/Kotlin/kotlinx.coroutines/issues/94
 * https://greenteapress.com/semaphores/LittleBookOfSemaphores.pdf @ 4.2.6
 */
class RWMutex {
    private val noReaders = Semaphore(1)
    private val noWriters = Semaphore(1)
    private var readSwitch = Lightswitch(noWriters)
    private var writeSwitch = Lightswitch(noReaders)

    suspend fun readLock() = noReaders.withPermit {
        readSwitch.lock()
    }

    suspend fun readUnlock() = readSwitch.unlock()

    suspend fun writeLock() {
        writeSwitch.lock()
        noWriters.acquire()
    }

    suspend fun writeUnlock() {
        noWriters.release()
        writeSwitch.unlock()
    }
}

@OptIn(ExperimentalContracts::class)
suspend inline fun <T> RWMutex.withReadLock(action: () -> T): T {
    contract {
        callsInPlace(action, InvocationKind.EXACTLY_ONCE)
    }

    readLock()
    try {
        return action()
    } finally {
        readUnlock()
    }
}

@OptIn(ExperimentalContracts::class)
suspend inline fun <T> RWMutex.withWriteLock(action: () -> T): T {
    contract {
        callsInPlace(action, InvocationKind.EXACTLY_ONCE)
    }

    writeLock()
    try {
        return action()
    } finally {
        writeUnlock()
    }
}