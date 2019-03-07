/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.video.videodemo;

import io.pravega.client.byteStream.ByteStreamReader;
import org.jcodec.common.io.SeekableByteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A SeekableByteChannel that JCodec can use to read video from a Pravega stream.
 */
public class PravegaSeekableByteChannelAdapter implements SeekableByteChannel {
    private ByteStreamReader reader;
    private long size;
    private long offset;

    /**
     * @param reader The Pravega stream. It should be positioned at the beginning of the video.
     * @param size The size of the video as reported to JCodec.
     */
    public PravegaSeekableByteChannelAdapter(ByteStreamReader reader, long size) {
        this.reader = reader;
        this.size = size;
        this.offset = reader.getOffset();
    }

    @Override
    public long position() throws IOException {
        return reader.getOffset() - offset;
    }

    @Override
    public SeekableByteChannel setPosition(long newPosition) throws IOException {
        reader.seekToOffset(newPosition + offset);
        return this;
    }

    @Override
    public long size() throws IOException {
        return size;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return reader.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        return reader.isOpen();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
