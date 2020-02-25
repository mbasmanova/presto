/*
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
 */
package com.facebook.presto.spi.block;

public interface LazyBlockLoader<T extends Block>
{
    void load(T block);

    default void load(ValueConsumer consumer, boolean includeNulls)
    {
        throw new UnsupportedOperationException();
    }

    abstract class ValueConsumer
    {
        public void acceptLong(int position, long value)
        {
            throw new UnsupportedOperationException();
        }

        public void acceptDouble(int position, double value)
        {
            throw new UnsupportedOperationException();
        }

        public void acceptFloat(int position, float value)
        {
            throw new UnsupportedOperationException();
        }

        public void acceptNull(int position)
        {
            throw new UnsupportedOperationException();
        }
    }
}
