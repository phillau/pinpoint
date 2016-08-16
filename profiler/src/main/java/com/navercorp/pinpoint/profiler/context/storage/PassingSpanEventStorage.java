/*
 * Copyright 2016 NAVER Corp.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanEvent;

/**
 * @author Taejin Koo
 */
public class PassingSpanEventStorage implements Storage {

    private final Storage delegator;

    public PassingSpanEventStorage(Storage storage) {
        if (storage == null) {
            throw new NullPointerException("storage may not be null");
        }
        this.delegator = storage;
    }

    @Override
    public void store(SpanEvent spanEvent) {
        delegator.store(spanEvent);
    }

    @Override
    public void store(Span span) {
        // do nothing
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

}
