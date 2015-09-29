package com.meltmedia.dropwizard.etcd.json;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import com.meltmedia.dropwizard.etcd.json.WatchService.RunnableWithException;

public class FunctionalLock {
  ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  Lock read = stateLock.readLock();
  Lock write = stateLock.writeLock();

  protected <T> T read(Callable<T> callable) throws Exception {
    read.lock();
    try {
      return callable.call();
    } finally {
      read.unlock();
    }
  }

  protected <E extends Exception> void read(RunnableWithException<E> r, Class<E> e) throws E {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected void read(Runnable r) {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected <E extends Exception> void write(RunnableWithException<E> r, Class<E> e) throws E {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected void write(Runnable r) {
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
  }

  protected <T> T write(Callable<T> callable) throws Exception {
    read.lock();
    try {
      return callable.call();
    } finally {
      read.unlock();
    }
  }
  
  protected <T> T write(Supplier<T> supplier) {
    write.lock();
    try {
      return supplier.get();
    } finally {
      write.unlock();
    }
  }

}
