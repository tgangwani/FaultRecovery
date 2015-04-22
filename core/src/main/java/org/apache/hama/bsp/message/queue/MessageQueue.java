/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.message.queue;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TaskAttemptID;

import java.util.List;

/**
 * Simple queue interface.
 */
public interface MessageQueue<M extends Writable> extends Iterable<M>,
    Configurable {

  public static final String PERSISTENT_QUEUE = "hama.queue.behaviour.persistent";

  /**
   * Used to initialize the queue.
   */
  public void init(Configuration conf, TaskAttemptID id);

  public void init(Configuration conf, TaskAttemptID id, BSPPeer peerRef);

  /**
   * Finally close the queue. Commonly used to free resources.
   */
  public void close();

    /**
     * Adds a whole Java Collection to the implementing queue.
     */
    public void addAll(Iterable<M> col);

    /**
     * Adds a whole Java Collection to the implementing queue.
     */
    public void addAllRecovery(Iterable<M> col);

  /**
   * Adds the other queue to this queue.
   */
  public void addAll(MessageQueue<M> otherqueue);

    /**
     * Adds the received bundle
     *
     * @param bundle
     */
    public void addBundle(BSPMessageBundle<M> bundle);


    /**
     * Adds the received bundle for recovery
     *
     * @param bundle
     */
    public void addBundleRecovery(BSPMessageBundle<M> bundle);

    /**
     * Adds a single item to the implementing queue.
     */
    public void add(M item);

  /**
   * Clears all entries in the given queue.
   */
  public void clear();

  /**
   * Polls for the next item in the queue (FIFO).
   * 
   * @return a new item or null if none are present.
   */
  public M poll();

  /**
   * @return how many items are in the queue.
   */
  public int size();

  public List<M> getStateHints();

  public List<M> getRelevantMessages(String peerName);
}
