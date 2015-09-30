package com.meltmedia.dropwizard.etcd.json;

public interface RunnableWithException<E extends Exception> {
  public void run() throws E;
}