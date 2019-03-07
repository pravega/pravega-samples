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
import org.apache.commons.cli.*;
import org.jcodec.api.FrameGrab;
import org.jcodec.common.model.Picture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;

/**
 * This demonstrates how to use the Pravega byte stream API to write and read videos of any length.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public final String scope;
    public final String streamName;
    public final URI controllerURI;
    public final String mode;
    private final List<String> fileNames;

    public Main(String scope, String streamName, URI controllerURI, String mode, List<String> fileNames) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        this.mode = mode;
        this.fileNames = fileNames;
    }

    public void run() throws Exception {
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI)) {
            ByteStreamClient byteStreamClient = clientFactory.createByteStreamClient();

            if (mode.equals("write")) {
                // This writes videos of any length to a Pravega stream.
                // First it writes a long representing the length of the video file contents.
                // Then it writes the video file contents.
                log.info("fileNames={}", fileNames);
                try (ByteStreamWriter writer = byteStreamClient.createByteStreamWriter(streamName);
                     DataOutputStream dos = new DataOutputStream(writer)) {
                    log.info("tailOffset={}", writer.fetchTailOffset());
                    for (String fileName : fileNames) {
                        File file = new File(fileName);
                        long fileLength = file.length();
                        log.info("Writing {} bytes from {}", fileLength, file);
                        dos.writeLong(fileLength);
                        long bytesWritten = Files.copy(file.toPath(), dos);
                        if (bytesWritten != fileLength) {
                            throw new Exception("Unexpected number of bytes copied from file");
                        }
                        dos.flush();
                        log.info("Finished writing {} bytes from {}", bytesWritten, file);
                        log.info("tailOffset={}", writer.fetchTailOffset());
                    }
                }

            } else if (mode.equals("read")) {
                // This continually reads and decodes video files from a Pravega stream.
                // It must be in the format written by testWriteVideoToPravega.
                // It will block when it reaches the end of the stream.
                // Note that the library JCodec library that is used in this demo only supports videos in MP4 format.
                // By using other video libraries, various video formats such as MPEG-TS and MKV can be used.
                try (ByteStreamReader reader = byteStreamClient.createByteStreamReader(streamName);
                     DataInputStream dis = new DataInputStream(reader)) {
                    for (; ; ) {
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
                            // TODO: process the video frame here
                        }
                        log.info("Finished reading chunk");
                        // We need to reposition the Pravega reader because FrameGrab may not finish at the end of the chunk.
                        reader.seekToOffset(dataOffset + dataLength);
                    }
                }

            } else {
                throw new Exception("Invalid mode");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Main", options);
            System.exit(1);
        }

        final String scope = cmd.getOptionValue("scope") == null ? Constants.DEFAULT_SCOPE : cmd.getOptionValue("scope");
        final String streamName = cmd.getOptionValue("name") == null ? Constants.DEFAULT_STREAM_NAME : cmd.getOptionValue("name");
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);
        final String mode = cmd.getOptionValue("mode");

        Main app = new Main(scope, streamName, controllerURI, mode, cmd.getArgList());
        app.run();
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream.");
        options.addOption("n", "name", true, "The name of the stream.");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        options.addRequiredOption("m", "mode", true, "'write' or 'read'");
        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }
}
