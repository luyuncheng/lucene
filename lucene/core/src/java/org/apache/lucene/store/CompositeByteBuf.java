/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;

/** Like Netty CompositeByteBuf to reduce ByteBuffer copy */
public class CompositeByteBuf {

  private int componentCount;
  private Component[] components; // resized when needed

  public CompositeByteBuf() {
    this.componentCount = 0;
    components = new Component[0];
  }

  public CompositeByteBuf(int initSize) {
    this.componentCount = 0;
    components = new Component[initSize];
  }
  /**
   * Add the given {@link ByteBuffer}.
   *
   * <p>{@link ByteBuffer} ownership of {@code buffer} is not transferred to this {@link
   * CompositeByteBuf}.
   *
   * @param buffer the {@link ByteBuffer} to add. {@link CompositeByteBuf}.
   */
  public CompositeByteBuf addComponent(ByteBuffer buffer) {
    checkNotNull(buffer, "buffer");
    assert buffer != null;
    final int cIndex = componentCount;

    checkComponentIndex(cIndex);
    Component c = newComponent(buffer, 0);
    int readableBytes = c.length();
    shiftComps(cIndex, 1);
    components[cIndex] = c;
    if (cIndex >= 1) {
      c.reposition(components[cIndex - 1].endOffset);
    }
    return this;
  }

  /** Return the index for the given offset */
  public int toComponentIndex(int offset) {
    int size = componentCount;
    if (offset == 0) { // fast-path zero offset
      for (int i = 0; i < size; i++) {
        if (components[i].endOffset > offset) {
          return i;
        }
      }
    }
    if (size <= 2) { // fast-path for 1 and 2 component count
      return size == 1 || offset < components[0].endOffset ? 0 : 1;
    }
    for (int low = 0, high = size; low <= high; ) {
      int mid = low + high >>> 1;
      Component c = components[mid];
      if (offset >= c.endOffset) {
        low = mid + 1;
      } else if (offset < c.offset) {
        high = mid - 1;
      } else {
        return mid;
      }
    }

    throw new Error("should not reach here");
  }

  /**
   * Transfers bytes from componets at offset into the given destination array.
   *
   * @param offset the offset within the compositeBytebuf of the first byte to be written
   * @param dest the destination array
   * @param destOffset The offset within the array of the first byte to be written
   * @param length The number of bytes to be written to the given array
   * @return this CompositeByteBuf
   */
  public CompositeByteBuf copyBytes(int offset, byte[] dest, int destOffset, int length) {
    int componentId = toComponentIndex(offset);

    while (length > 0 && componentId <= componentCount) {
      Component c = components[componentId];
      int localLength = Math.min(length, c.endOffset - offset);
      c.getBuf().get(c.idx(offset), dest, destOffset, localLength);
      offset += localLength;
      destOffset += localLength;
      length -= localLength;
      componentId++;
    }
    return this;
  }
    public ArrayList<Component> rangeOffset(int offset, int length) {
        int componentId = toComponentIndex(offset);
        ArrayList<Component> result = new ArrayList<>(1);

        while (length > 0 && componentId <= componentCount) {
            Component c = components[componentId];
            result.add(c);
            int localLength = Math.min(length, c.endOffset - offset);
            offset += localLength;
            length -= localLength;
            componentId++;
        }
        return result;
    }
  /** Return the current number of {@link ByteBuffer}'s that are composed in this instance */
  public int numComponents() {
    return componentCount;
  }
  /** Return the endOffset of this CompositeByteBuf */
  public int capacity() {
    int size = componentCount;
    return size > 0 ? components[size - 1].endOffset : 0;
  }

  private Component newComponent(final ByteBuffer buf, final int offset) {
    final int srcIndex = buf.position();
    final int len = buf.remaining();
    return new Component(buf.order(ByteOrder.LITTLE_ENDIAN), srcIndex, offset, len);
  }

  public static final class Component {
    final ByteBuffer buf;

    int adjustment; // index of the start of this CompositeByteBuf relative to srcBuf
    int offset; // offset of this component within this CompositeByteBuf
    int endOffset; // end offset of this component within this CompositeByteBuf

    Component(ByteBuffer buf, int srcOffset, int offset, int len) {
      this.buf = buf;
      this.adjustment = srcOffset - offset;
      this.offset = offset;
      this.endOffset = offset + len;
    }

    public ByteBuffer getBuf() {
      return this.buf;
    }

    public int length() {
      return endOffset - offset;
    }

    public int getOffset() {
       return this.offset;
    }
    public int getEndOffset() {
      return this.endOffset;
    }
    public int getAdjustment() {
      return this.adjustment;
    }
    public int idx(int index) {
      return index + adjustment;
    }
    void reposition(int newOffset) {
      int move = newOffset - offset;
      endOffset += move;
      adjustment -= move;
      offset = newOffset;
    }
  }

  private void shiftComps(int i, int count) {
    final int size = componentCount, newSize = size + count;
    assert i >= 0 && i <= size && count > 0;
    if (newSize > components.length) {
      // grow the array
      int newArrSize = Math.max(size + (size >> 1), newSize);
      Component[] newArr;
      if (i == size) {
        newArr = Arrays.copyOf(components, newArrSize, Component[].class);
      } else {
        newArr = new Component[newArrSize];
        if (i > 0) {
          System.arraycopy(components, 0, newArr, 0, i);
        }
        if (i < size) {
          System.arraycopy(components, i, newArr, i + count, size - i);
        }
      }
      components = newArr;
    } else if (i < size) {
      System.arraycopy(components, i, components, i + count, size - i);
    }
    componentCount = newSize;
  }

  /**
   * Checks that the given argument is not null. If it is, throws {@link NullPointerException}.
   * Otherwise, returns the argument.
   */
  public static <T> T checkNotNull(T arg, String text) {
    if (arg == null) {
      throw new NullPointerException(text);
    }
    return arg;
  }
  /**
   * Checks that the given index is not out of bound. If it is, throws {@link
   * IndexOutOfBoundsException}.
   */
  private void checkComponentIndex(int cIndex) {

    if (cIndex < 0 || cIndex > componentCount) {
      throw new IndexOutOfBoundsException(
          String.format(
              "cIndex: %d (expected: >= 0 && <= numComponents(%d))", cIndex, componentCount));
    }
  }
}
