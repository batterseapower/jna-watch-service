package watchservice

import java.io.{File, IOException}
import java.nio.file._
import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent._
import java.util.function.{Consumer, BiFunction}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.sun.jna._
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.mutable
import scala.collection.JavaConverters._

object CLibrary {
    val INSTANCE = Native.loadLibrary("c", classOf[CLibrary]).asInstanceOf[CLibrary]

    val NAME_MAX = 255

    // http://kernelhistory.sourcentral.org/linux-0.99.1/S/35.html
    val ENOENT  = 2
    val EINTR   = 4
    val ENOTDIR = 20
    val EINVAL  = 22

    // Constants from http://lxr.free-electrons.com/source/include/uapi/linux/inotify.h#L40
    val IN_CLOSE_WRITE = 0x00000008
    val IN_ATTRIB      = 0x00000004
    val IN_MOVED_TO    = 0x00000080
    val IN_CREATE      = 0x00000100
    val IN_MOVE_SELF   = 0x00000800
    val IN_ONLYDIR     = 0x01000000

    val IN_UNMOUNT     = 0x00002000
    val IN_Q_OVERFLOW  = 0x00004000
    val IN_IGNORED     = 0x00008000
}

trait CLibrary extends Library {
    @throws[LastErrorException]
    def inotify_init() : Int
    @throws[LastErrorException]
    def inotify_add_watch(fd : Int, pathname : String, mask : Int) : Int
    @throws[LastErrorException]
    def inotify_rm_watch(fd : Int, wd : Int) : Int
    @throws[LastErrorException]
    def read(fd : Int, buf : Pointer, size : Int) : Int
    @throws[LastErrorException]
    def close(fd : Int) : Int
}

// The JVM's LinuxWatchService has a bug where it randomly reports events on the wrong directory (at least as
// of JDK8u60). So we get to reimplement it. Joy.
//
// (Yes, I've been a good citizen and reported the bug to Oracle)
trait WorkingWatchService {
    def poll() : Option[WatchKey]
    def poll(duration : Int, unit : TimeUnit) : Option[WatchKey]
    def register(path : Path) : WatchKey
    def close() : Unit
}

class WorkingJDKWatchService(jdkService : java.nio.file.WatchService) extends WorkingWatchService {
    def poll() = Option(jdkService.poll())
    def poll(duration : Int, unit : TimeUnit) = Option(jdkService.poll(duration, unit))
    def register(path : Path) = path.register(jdkService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW)
    def close() = jdkService.close()
}

class WorkingLinuxWatchKey(val path : Path, service : WorkingLinuxWatchService, var wd : Int) extends WatchKey {
    private val events = new mutable.ArrayBuffer[WatchEvent[Path]]() // INVARIANT: if number of events > 0, this key is either enqueued in the service, OR invalid, OR not ready
    private var ready = true

    def setInvalid() = synchronized {
        val theWD = wd
        wd = -1
        theWD
    }
    def addEvent(event : WatchEvent[Path]) = synchronized {
        events += event
        if (ready && events.size == 1 && isValid) service.enqueue(this)
    }

    override def cancel() = synchronized { if (isValid) service.cancel(setInvalid(), this) }

    override def isValid : Boolean = synchronized { wd != -1 }

    override def pollEvents() : java.util.List[WatchEvent[_]] = synchronized {
        val result = new util.ArrayList[WatchEvent[_]]()
        events.foreach(result.add)
        events.clear()
        result
    }

    override def reset() : Boolean = synchronized {
        val valid = isValid
        if (!ready && valid) {
            ready = true
            if (events.nonEmpty) service.enqueue(this)
        }
        valid
    }

    override def watchable() = path
}

object WorkingLinuxWatchService {
    private val THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WorkingWatchService-%d").build()
}

class WorkingLinuxWatchService extends WorkingWatchService with StrictLogging { service =>
    private val wdToKey = new ConcurrentHashMap[Int, WorkingLinuxWatchKey]()
    private val keyQueue = new LinkedBlockingQueue[WatchKey]()

    private val ifd = CLibrary.INSTANCE.inotify_init()
    if (ifd == -1) throw new LastErrorException(Native.getLastError) // This shouldn't really happen: JNA should have thrown for us already

    private val thread = Executors.newSingleThreadExecutor(WorkingLinuxWatchService.THREAD_FACTORY)
    private val threadFuture = thread.submit(new Runnable {
        override def run() = try {
            // The read() of the inotify FD is guaranteed to never return a partial event.
            // What's more, it's guaranteed that a buffer of size "sizeof(struct inotify_event) + NAME_MAX + 1" will be sufficient to read at least one event
            val bufferSize = Math.max(8192, (4 + 4 + 4 + 4) + CLibrary.NAME_MAX + 1)
            val buffer = new Memory(bufferSize)
            while (true) {
                val bytesRead = try {
                    val bytesRead = CLibrary.INSTANCE.read(ifd, buffer, bufferSize)
                    if (bytesRead == -1) throw new LastErrorException(Native.getLastError) // This shouldn't really happen: JNA should have thrown for us already
                    bytesRead
                } catch {
                    case (e : LastErrorException) if e.getErrorCode == CLibrary.EINTR => throw new InterruptedException()
                }

                var offset = 0
                while (offset < bytesRead) {
                    val wd     = buffer.getInt(offset +  0)
                    val mask   = buffer.getInt(offset +  4) // Actually uint32_t
                    val cookie = buffer.getInt(offset +  8) // Actually uint32_t
                    val len    = buffer.getInt(offset + 12) // Actually uint32_t
                    val name   = if (len <= 0) null else buffer.getString(offset + 16) // Assume whatever the default string encoding is. A bit dodgy, but the best we can do given we can't instantiate UnixPath ourselves..

                    if ((mask & CLibrary.IN_Q_OVERFLOW) != 0) {
                        // wd should be -1 in this case
                        wdToKey.values().asScala.foreach { key =>
                            key.addEvent(new WatchEvent[Path] {
                                override def count() = 1
                                override def kind() = StandardWatchEventKinds.OVERFLOW.asInstanceOf[WatchEvent.Kind[Path]]
                                override def context() = null
                            })
                        }
                    }

                    Option(wdToKey.get(wd)).foreach { key =>
                        if (name != null) {
                            // name == null indicates an event for the directory itself ==> ignore it
                            for ((theKind, kindMask) <- Seq(
                                StandardWatchEventKinds.ENTRY_CREATE -> (CLibrary.IN_CREATE | CLibrary.IN_MOVED_TO),
                                StandardWatchEventKinds.ENTRY_DELETE -> 0,
                                StandardWatchEventKinds.ENTRY_MODIFY -> (CLibrary.IN_ATTRIB | CLibrary.IN_CLOSE_WRITE)
                            ) if (kindMask & mask) != 0) {
                                key.addEvent(new WatchEvent[Path] {
                                    override def count() = 1
                                    override def kind() = theKind
                                    override def context() = new File(name).toPath
                                })
                            }
                        }

                        // IN_MOVE_SELF is maybe a bit surprising. We need to listen for this event because IN_IGNORED
                        // will not be generated in this case (unlike the IN_DELETE_SELF case), but we want to make sure
                        // our WatchKey for the watch descriptor refers to the *new* name rather than the *old* name.
                        // A convenient way to do this is to just cancel the old watch and wait for our user to reregister
                        // on the new name (if it is even within a path of interest).
                        if ((mask & (CLibrary.IN_IGNORED | CLibrary.IN_UNMOUNT | CLibrary.IN_MOVE_SELF)) != 0) {
                            // File was deleted
                            key.cancel()
                        }
                    }

                    offset += 16 + len
                }
            }
        } catch {
            case (_ : InterruptedException) => Thread.currentThread().interrupt() // We are being shut down gracefully
        }
    })

    private def checkForThreadDeath() : Unit = {
        if (threadFuture.isDone) threadFuture.get()
    }

    def poll() : Option[WatchKey] = { checkForThreadDeath(); Option(keyQueue.poll()) }
    def poll(duration : Int, unit : TimeUnit) : Option[WatchKey] = { checkForThreadDeath(); Option(keyQueue.poll(duration, unit)) }

    def enqueue(key : WatchKey) = keyQueue.put(key)
    def cancel(wd : Int, key : WorkingLinuxWatchKey) = synchronized {
        if (wdToKey.remove(wd, key)) {
            try {
                if (CLibrary.INSTANCE.inotify_rm_watch(ifd, wd) == -1) throw new LastErrorException(Native.getLastError) // This shouldn't really happen: JNA should have thrown for us already
            } catch {
                case (e : LastErrorException) if e.getErrorCode == CLibrary.EINVAL => () // This happens when the wd is already closed because e.g. the watched file has been deleted
                case (e : LastErrorException) => logger.warn(s"Failed to close inotify watch for path ${key.path}", e)
            }
        }
    }

    // We actually take advantage of the fact we're using the inotify interface directly and wait for IN_CLOSE_WRITE rather than IN_MODIFY. We also specify IN_ONLYDIR -- which
    // Java doesn't, which actually leads to an (unimportant) race between the isDirectory check it does and opening the watch.
    def register(path : Path) : WatchKey = {
        var oldKey = Option.empty[WorkingLinuxWatchKey]
        val key = synchronized {
            val wd = try {
                val wd = CLibrary.INSTANCE.inotify_add_watch(ifd, path.toString, CLibrary.IN_CLOSE_WRITE | CLibrary.IN_ATTRIB | CLibrary.IN_CREATE | CLibrary.IN_MOVED_TO | CLibrary.IN_MOVE_SELF | CLibrary.IN_ONLYDIR)
                if (wd < 0) throw new LastErrorException(Native.getLastError) // This shouldn't really happen: JNA should have thrown for us already
                wd
            } catch {
                case (e : LastErrorException) if e.getErrorCode == CLibrary.ENOTDIR => throw new NotDirectoryException(path.toString)
                case (e : LastErrorException) if e.getErrorCode == CLibrary.ENOENT  => throw new NoSuchFileException(path.toString)
                case (e : LastErrorException) => throw new IOException("inotify_add_watch failed", e)
            }

            wdToKey.compute(wd, new BiFunction[Int, WorkingLinuxWatchKey, WorkingLinuxWatchKey] {
                def apply(wd : Int, key : WorkingLinuxWatchKey) = {
                    // NB: do *not* use Files.isSameFile to test paths for equality, because we exactly *do* want to replace key
                    // when the paths are textually different but point to the same directory inode!
                    if (key != null && key.path.toAbsolutePath == path.toAbsolutePath) key else {
                        if (key != null) {
                            oldKey = Some(key)
                        }
                        new WorkingLinuxWatchKey(path, service, wd)
                    }
                }
            })
        }

        // Make the old key invalid outside the wdToKey update, because it might be that someone else has oldKey
        // locked and is waiting to update wdToKey themselves (e.g. if they are calling WorkingLinuxWatchKey.cancel)
        // and in that case we'll get a deadlock!
        oldKey.foreach { oldKey =>
            // If key.path != path then we are in the rather unusual case where the directory being watched has
            // been moved and our current notion of the correct path is out of date.
            oldKey.setInvalid()
        }

        key
    }

    def close() = {
        threadFuture.cancel(true)
        wdToKey.values().asScala.toBuffer[WorkingLinuxWatchKey].foreach(_.cancel())
        try {
            if (CLibrary.INSTANCE.close(ifd) == -1) throw new LastErrorException(Native.getLastError) // This shouldn't really happen: JNA should have thrown for us already
        } catch {
            case (e : LastErrorException) => logger.warn(s"Failed to close inotify", e)
        }
    }
}
