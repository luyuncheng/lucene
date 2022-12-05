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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.compress.LZ4;

/**
 * A compression mode that compromises on the compression ratio to provide fast compression and
 * decompression.
 *
 * @lucene.internal
 */
public final class LZ4WithPresetDictCompressionMode extends CompressionMode {

  // Shoot for 10 sub blocks
  private static final int NUM_SUB_BLOCKS = 10;
  // And a dictionary whose size is about 2x smaller than sub blocks
  private static final int DICT_SIZE_FACTOR = 2;

  /** Sole constructor. */
  public LZ4WithPresetDictCompressionMode() {}

  @Override
  public Compressor newCompressor() {
    return new LZ4WithPresetDictCompressor();
  }

  @Override
  public Decompressor newDecompressor() {
    return new LZ4WithPresetDictDecompressor();
  }

  @Override
  public String toString() {
    return "BEST_SPEED";
  }

  private static final class LZ4WithPresetDictDecompressor extends Decompressor {

    private int[] compressedLengths;

    LZ4WithPresetDictDecompressor() {
      compressedLengths = new int[0];
    }

    private int readCompressedLengths(
        DataInput in, int originalLength, int dictLength, int blockLength) throws IOException {
      in.readVInt(); // compressed length of the dictionary, unused
      int totalLength = dictLength;
      int i = 0;
      while (totalLength < originalLength) {

        compressedLengths[i++] = in.readVInt();
        totalLength += blockLength;
      }
      return i;
    }

    /**
     * Decompress bytes that were stored between offsets <code>offset</code> and <code>offset+length
     * </code> in the original stream from the compressed stream <code>in</code> to <code>bytes
     * </code> . After returning, the length of <code>bytes</code> (<code>bytes.length</code>) must
     * be equal to <code>length</code>. Implementations of this method are free to resize <code>
     * bytes</code> depending on their needs. Note: First dictLength + blockLength bytes in
     * bytes.bytes are decompressed buffer
     *
     * @param in the input that stores the compressed stream
     * @param originalLength the length of the original data (before compression)
     * @param offset bytes before this offset do not need to be decompressed
     * @param length bytes after <code>offset+length</code> do not need to be decompressed
     * @param bytes a {@link BytesRef} where to store the decompressed data
     */
    @Override
    public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes)
        throws IOException {
      assert offset + length <= originalLength;

      if (length == 0) {
        bytes.length = 0;
        return;
      }

      final int dictLength = in.readVInt();
      final int blockLength = in.readVInt();
      bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, dictLength + blockLength);
      bytes.offset = dictLength + blockLength;
      bytes.length = dictLength + blockLength;
      compressedLengths = new int[originalLength / blockLength + 1];
      final int numBlocks = readCompressedLengths(in, originalLength, dictLength, blockLength);

      // Read the dictionary
      if (LZ4.decompress(in, dictLength, bytes.bytes, 0) != dictLength) {
        throw new CorruptIndexException("Illegal dict length", in);
      }

      int offsetInBlock = dictLength;
      int offsetInBytesRef = offset;
      if (offset >= dictLength) {
        offsetInBytesRef -= dictLength;

        // Skip unneeded blocks
        int numBytesToSkip = 0;
        for (int i = 0; i < numBlocks && offsetInBlock + blockLength < offset; ++i) {
          int compressedBlockLength = compressedLengths[i];
          numBytesToSkip += compressedBlockLength;
          offsetInBlock += blockLength;
          offsetInBytesRef -= blockLength;
        }
        in.skipBytes(numBytesToSkip);
      } else {
        // The dictionary contains some bytes we need, copy its content to the BytesRef
        bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + dictLength);
        System.arraycopy(bytes.bytes, 0, bytes.bytes, bytes.offset, dictLength);
        bytes.length = bytes.length + dictLength;
      }

      // Read blocks that intersect with the interval we need
      while (offsetInBlock < offset + length) {
        final int bytesToDecompress = Math.min(blockLength, offset + length - offsetInBlock);
        LZ4.decompress(in, bytesToDecompress, bytes.bytes, dictLength);
        bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + bytesToDecompress);
        System.arraycopy(bytes.bytes, dictLength, bytes.bytes, bytes.length, bytesToDecompress);
        bytes.length += bytesToDecompress;
        offsetInBlock += blockLength;
      }

      bytes.offset += offsetInBytesRef;
      bytes.length = length;

      compressedLengths = null;
      assert bytes.isValid();
    }

    @Override
    public Decompressor clone() {
      return new LZ4WithPresetDictDecompressor();
    }
  }

  private static class LZ4WithPresetDictCompressor extends Compressor {

    final ByteBuffersDataOutput compressed;
    final LZ4.FastCompressionHashTable hashTable;
    byte[] buffer;

    LZ4WithPresetDictCompressor() {
      compressed = ByteBuffersDataOutput.newResettableInstance();
      hashTable = new LZ4.FastCompressionHashTable();
      buffer = BytesRef.EMPTY_BYTES;
    }

    private void doCompress(byte[] bytes, int dictLen, int len, DataOutput out) throws IOException {
      long prevCompressedSize = compressed.size();
      LZ4.compressWithDictionary(bytes, 0, dictLen, len, compressed, hashTable);
      // Write the number of compressed bytes
      out.writeVInt(Math.toIntExact(compressed.size() - prevCompressedSize));
    }

    @Override
    public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
      final int len = (int) (buffersInput.size() - buffersInput.position());
      final int dictLength = Math.min(LZ4.MAX_DISTANCE, len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR));
      final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
      buffer = ArrayUtil.growNoCopy(buffer, dictLength + blockLength);
      out.writeVInt(dictLength);
      out.writeVInt(blockLength);

      compressed.reset();
      // Compress the dictionary first
      buffersInput.readBytes(buffer, 0, dictLength);
      doCompress(buffer, 0, dictLength, out);

      // And then sub blocks
      for (int start = dictLength; start < len; start += blockLength) {
        int l = Math.min(blockLength, len - start);
        buffersInput.readBytes(buffer, dictLength, l);
        doCompress(buffer, dictLength, l, out);
      }

      // We only wrote lengths so far, now write compressed data
      compressed.copyTo(out);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
