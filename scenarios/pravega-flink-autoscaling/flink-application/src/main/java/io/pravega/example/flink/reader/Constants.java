/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.example.flink.reader;

/**
 * Defines a handful of constants used by the Flink reader application.
 */
public class Constants {
    protected static final String HOST_PARAM = "host";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final String PORT_PARAM = "port";
    protected static final String DEFAULT_PORT = "9999";
    protected static final String STREAM_PARAM = "stream";
    protected static final String DEFAULT_SCOPE = "test";
    protected static final String DEFAULT_STREAM = "stream";
    protected static final String WORD_SEPARATOR = " ";
}
