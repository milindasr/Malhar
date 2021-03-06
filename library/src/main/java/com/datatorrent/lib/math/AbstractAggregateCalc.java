/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;

import java.util.Collection;

/**
 *
 * Invokes two abstract functions aggregateLongs(Collection<T> collection), and
 * aggregateDoubles(Collection<T> collection) on input tuple and emits the
 * result on four ports, namely \"doubleResult\", \"floatResult\",
 * \"integerResult\", \"longResult\". Input tuple object has to be an
 * implementation of the interface Collection&lt;T&gt;. Tuples are emitted on
 * the output ports only if they are connected. This is done to avoid the cost
 * of calling the functions when some ports are not connected. integerResult and
 * floatResult get rounded results respectively.
 * <p>
 * This is a pass through operator<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Collection&lt;T extends Number&gt;<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>longResult</b>: emits Long<br>
 * <br>
 *
 * @since 0.3.3
 */
public abstract class AbstractAggregateCalc<T extends Number> extends
		AbstractOutput
{
	/**
	 * Input port, accepts collection of values of type 'T'.
	 */
	public final transient DefaultInputPort<Collection<T>> input = new DefaultInputPort<Collection<T>>()
	{
		/**
		 * Aggregate calculation result is only emitted on output port if it is connected.
		 */
		@Override
		public void process(Collection<T> collection)
		{
			Double dResult = null;
			if (doubleResult.isConnected()) {
				doubleResult.emit(dResult = aggregateDoubles(collection));
			}

			if (floatResult.isConnected()) {
				floatResult
						.emit(dResult == null ? (float) (aggregateDoubles(collection))
								: dResult.floatValue());
			}

			Long lResult = null;
			if (longResult.isConnected()) {
				longResult.emit(lResult = aggregateLongs(collection));
			}

			if (integerResult.isConnected()) {
				integerResult.emit(lResult == null ? (int) aggregateLongs(collection)
						: lResult.intValue());
			}
		}

	};

	/**
	 * Abstract function to be implemented by sub class, custom calculation on input aggregate.
	 * @param collection Aggregate of values 
	 * @return calculated value.
	 */
	public abstract long aggregateLongs(Collection<T> collection);

	/**
	 * Abstract function to be implemented by sub class, custom calculation on input aggregate.
	 * @param collection Aggregate of values 
	 * @return calculated value.
	 */
	public abstract double aggregateDoubles(Collection<T> collection);
}
