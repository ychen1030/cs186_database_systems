package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.PageAllocator;

/** Metadata about a B+ tree. */
class BPlusTreeMetadata {
  // The page allocator used to persist the B+ tree. Every node of the B+ tree
  // is stored on a single page allocated by this allocator.
  private final PageAllocator allocator;

  // B+ trees map keys (of some type) to record ids. This is the type of the
  // keys.
  private final Type keySchema;

  // The order of the tree. Given a tree of order d, its inner nodes store
  // between d and 2d keys and between d+1 and 2d+1 children pointers. Leaf
  // nodes store between d and 2d (key, record id) pairs. Notable exceptions
  // include The root node and leaf nodes that have been deleted from; these
  // may contain fewer than d entries.
  private final int order;

  public BPlusTreeMetadata(PageAllocator allocator, Type keySchema, int order) {
    this.allocator = allocator;
    this.keySchema = keySchema;
    this.order = order;
  }

  public PageAllocator getAllocator() {
    return allocator;
  }

  public Type getKeySchema() {
    return keySchema;
  }

  public int getOrder() {
    return order;
  }
}
