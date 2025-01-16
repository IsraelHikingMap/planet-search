package com.example;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class MinWayIdFinder {
  List<Long> ids = new CopyOnWriteArrayList<Long>();
    long minId;
    public MinWayIdFinder() {
      this.minId = Integer.MAX_VALUE;
    }

    public void addWayId(long id) {
      ids.add(id);
      if (id < minId) {
        minId = id;
      }
    }
}