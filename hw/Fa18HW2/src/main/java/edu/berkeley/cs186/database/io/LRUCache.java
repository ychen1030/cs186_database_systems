package edu.berkeley.cs186.database.io;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A least-recently used cache used for buffer management. Extends the LinkedHashMap class for
 * simplicity of implementation. The key type is the virtual page number and the value type is
 * Page.
 *
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS PACKAGE.
 */
public class LRUCache<k extends Long, v extends Page> extends LinkedHashMap<k, v> {
  private int cacheSize;

  public LRUCache(int cacheSize) {
    super(16, 0.75f, true);
    this.cacheSize = cacheSize;
  }

  protected boolean removeEldestEntry(Map.Entry<k, v> eldest) {
    if (size() > cacheSize) {
      eldest.getValue().flush();
      return true;
    }
    return false;
  }
}
