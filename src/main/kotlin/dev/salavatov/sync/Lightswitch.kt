package dev.salavatov.sync

import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.atomic.AtomicInteger

internal class Lightswitch(val semaphore: Semaphore) {
    private val counter = AtomicInteger()
    private val mutex = Semaphore(1)

    suspend fun lock() = mutex.withPermit {
        if (counter.incrementAndGet() == 1) {
            semaphore.acquire()
        }
    }

    suspend fun unlock() = mutex.withPermit {
        if (counter.decrementAndGet() == 0) {
            semaphore.release()
        }
    }
}