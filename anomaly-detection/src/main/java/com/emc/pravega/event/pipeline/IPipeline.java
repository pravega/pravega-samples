/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.event.pipeline;

import com.emc.pravega.event.AppConfiguration;

public interface IPipeline {
	void run(AppConfiguration appConfiguration) throws Exception;
}
