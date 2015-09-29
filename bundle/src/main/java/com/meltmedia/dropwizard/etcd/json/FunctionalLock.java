package com.meltmedia.dropwizard.etcd.json;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class FunctionalLock {
  ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  Lock read = stateLock.readLock();
  Lock write = stateLock.writeLock();
  
  public <T> Callable<T> readCallable(Callable<T> callable) {
    return (Callable<T>)() -> {
      read.lock();
      try {
        return callable.call();
      } finally {
        read.unlock();
      }      
    };
  }

  public <E extends Exception> RunnableWithException<E> readRunnable(RunnableWithException<E> r, Class<E> e) {
    return () -> {
      read.lock();
      try {
        r.run();
      } finally {
        read.unlock();
      }
    };
  }

  public Runnable readRunnable(Runnable r) {
    return ()->{
    read.lock();
    try {
      r.run();
    } finally {
      read.unlock();
    }
    };
  }

  public <E extends Exception> RunnableWithException<E> writeRunnable(RunnableWithException<E> r, Class<E> e) {
    return () -> {
    write.lock();
    try {
      r.run();
    } finally {
      write.unlock();
    }
    };
  }

  public Runnable writeRunnable(Runnable r) {
    return ()->{
    write.lock();
    try {
      r.run();
    } finally {
      write.unlock();
    }
    };
  }

  protected <T> Callable<T> writeCallable(Callable<T> callable) {
    return ()->{
    write.lock();
    try {
      return callable.call();
    } finally {
      write.unlock();
    }
    };
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
