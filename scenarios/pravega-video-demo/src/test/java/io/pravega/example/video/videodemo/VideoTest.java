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

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Cleanup;
import org.jcodec.api.FrameGrab;
import org.jcodec.api.MediaInfo;
import org.jcodec.common.model.Picture;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VideoTest {
    private static final Logger log = LoggerFactory.getLogger(VideoTest.class);

    /**
    This writes a video of any length to a Pravega stream.
    First it writes a long representing the length of the video file contents.
    Then it writes the video file contents.
    This function can be run multiple times to append additional video files.
    */
    @Ignore()
    @Test
    public void testWriteVideoToPravega() throws Exception {
        URI controllerURI = URI.create("tcp://localhost:9090");
        StreamManager streamManager = StreamManager.create(controllerURI);
        String scope = "video9";
        streamManager.createScope(scope);
        String streamName = "stream1";
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        ByteStreamClient byteStreamClient = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = byteStreamClient.createByteStreamWriter(streamName);
        @Cleanup
        DataOutputStream dos = new DataOutputStream(writer);
        log.info("tailOffset={}", writer.fetchTailOffset());

        List<String> fileNames = new ArrayList<>();
        for (int i = 0 ; i < 20 ; i++) {
            fileNames.add("../small.mp4");          // http://techslides.com/demos/sample-videos/small.mp4
            fileNames.add("../Wildlife.mp4");       // https://archive.org/download/WildlifeSampleVideo/Wildlife.mp4
            fileNames.add("../bike.mp4");           // 221 MB 2K video
        }

        for (String fileName : fileNames) {
            File file = new File(fileName);
            long fileLength = file.length();
            log.info("Writing {} bytes from {}", fileLength, file);
            dos.writeLong(fileLength);
            long bytesWritten = Files.copy(file.toPath(), dos);
            assertEquals(bytesWritten, fileLength);
            dos.flush();
            log.info("Finished writing {} bytes from {}", bytesWritten, file);
            log.info("tailOffset={}", writer.fetchTailOffset());
        }
    }

    /**
     This continually reads and decodes video files from a Pravega stream.
     It must be in the format written by testWriteVideoToPravega.
     It will block when it reaches the end of the stream.
     */
    @Ignore()
    @Test
    public void testReadVideoFromPravega() throws Exception {
        URI controllerURI = URI.create("tcp://localhost:9090");
        StreamManager streamManager = StreamManager.create(controllerURI);
        String scope = "video9";
        streamManager.createScope(scope);
        String streamName = "stream1";
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        ByteStreamClient byteStreamClient = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamReader reader = byteStreamClient.createByteStreamReader(streamName);
        @Cleanup
        DataInputStream dis = new DataInputStream(reader);

        for (;;) {
            // This will block until the length of the next chunk has been written to Pravega.
            long dataLength = dis.readLong();
            long dataOffset = reader.getOffset();
            log.info("dataOffset={}, dataLength={}", dataOffset, dataLength);
            PravegaSeekableByteChannelAdapter adapter = new PravegaSeekableByteChannelAdapter(reader, dataLength);
            FrameGrab grab = FrameGrab.createFrameGrab(adapter);
            Picture picture;
            long frameNumber = 0;
            while (null != (picture = grab.getNativeFrame())) {
                frameNumber++;
                log.info("frame {} {}x{} {}", frameNumber, picture.getWidth(), picture.getHeight(), picture.getColor());
                // Process the video frame here
            }
            log.info("Finished reading chunk");
            // We need to reposition the Pravega reader because FrameGrab may not finish at the end of the chunk.
            reader.seekToOffset(dataOffset + dataLength);
        }
    }

    /**
     * This demonstrates the ability to read only the header of each video to quickly summarize the contents of a stream.
     */
    @Ignore()
    @Test
    public void testSummarizeVideo() throws Exception {
        URI controllerURI = URI.create("tcp://localhost:9090");
        StreamManager streamManager = StreamManager.create(controllerURI);
        String scope = "video9";
        streamManager.createScope(scope);
        String streamName = "stream1";
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        ByteStreamClient byteStreamClient = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamReader reader = byteStreamClient.createByteStreamReader(streamName);
        @Cleanup
        DataInputStream dis = new DataInputStream(reader);

        long tailOffset = reader.fetchTailOffset();
        for (int chunkIndex = 0 ; ; chunkIndex++) {
            if (reader.getOffset() >= tailOffset) {
                log.info("Reached the end of the stream.");
                break;
            }
            long dataLength = dis.readLong();
            long dataOffset = reader.getOffset();
            log.info("dataOffset={}, dataLength={}, chunkIndex={}", dataOffset, dataLength, chunkIndex);
            if (true) {
                PravegaSeekableByteChannelAdapter adapter = new PravegaSeekableByteChannelAdapter(reader, dataLength);
                FrameGrab grab = FrameGrab.createFrameGrab(adapter);
                final MediaInfo mediaInfo = grab.getMediaInfo();
                log.info("mediaInfo={}x{}", mediaInfo.getDim().getWidth(), mediaInfo.getDim().getHeight());
            }
            reader.seekToOffset(dataOffset + dataLength);
        }
    }

}
