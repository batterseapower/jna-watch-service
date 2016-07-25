This might be useful if you need to work around the fact that WatchService is broken on Linux before JDK8: http://blog.omega-prime.co.uk/?p=161, https://bugs.openjdk.java.net/browse/JDK-8162378

WatchService was fixed by this Java 8 backport: https://bugs.openjdk.java.net/browse/JDK-8162378

Even if you are using a WatchService implementation that is unaffected by the bug, you might want to use this implementation if you want your watch keys to be invalidated when the directory you are watching gets moved.
