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
package org.apache.hama.graph;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;


//import com.sun.tools.javac.util.Log;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.message.queue.SynchronizedQueue;

import org.apache.hama.bsp.HashPartitioner;


public class IncomingVertexMessageManager<M extends WritableComparable<M>>
    implements SynchronizedQueue<GraphJobMessage> {

  private Configuration conf;
  protected BSPPeer<?, ?, ?, ?, M> peer;
  private List<GraphJobMessage> stateHints = new ArrayList<GraphJobMessage>();

  private static final Log LOG = LogFactory
            .getLog(IncomingVertexMessageManager.class);

  private final MessagePerVertex msgPerVertex = new MessagePerVertex();

  private final ConcurrentLinkedQueue<GraphJobMessage> mapMessages = new ConcurrentLinkedQueue<GraphJobMessage>();

  public List<GraphJobMessage> getStateHints() {
      return stateHints;
  }
  @Override
  public Iterator<GraphJobMessage> iterator() {
    return msgPerVertex.iterator();
  }

  public List<GraphJobMessage> getRelevantMessages(String peerName) {
      HashPartitioner<WritableComparable, IntWritable> partitioner = new HashPartitioner();
      List<GraphJobMessage> msgs = new ArrayList<GraphJobMessage>();
      ConcurrentNavigableMap<WritableComparable, List<WritableComparable>> offsetMap = msgPerVertex.getVertexIdOffsetMap();
      HashMap<WritableComparable, GraphJobMessage> aggregatorMap = msgPerVertex.getMessageAggregatorMap();
      if(offsetMap.size() != aggregatorMap.size()) {
        LOG.info("[ERROR IncomingVertexManager.java] Size mismatch!");
      }

      for (WritableComparable<?> dstVertexId : offsetMap.keySet()) {

          List<WritableComparable> list = offsetMap.get(dstVertexId);
          GraphJobMessage aggregatorGraphMsg = aggregatorMap.get(dstVertexId);
          byte[] valueArray = aggregatorGraphMsg.getValuesBytes();
          int messageSizeBytes = valueArray.length / aggregatorGraphMsg.getNumOfValues();

          Iterator<WritableComparable> it = list.iterator();
          Iterator<Writable> aggregateIt = aggregatorGraphMsg.getIterableMessages().iterator();
          while(it.hasNext()) {
              WritableComparable srcId = it.next();
              int partition = partitioner.getPartition(srcId , null, peer.getNumPeers());
              String srcPeer = peer.getAllPeerNames()[partition];
              if (srcPeer.equals(peerName)) {
                  GraphJobMessage newMsg = new GraphJobMessage(dstVertexId, aggregateIt.next(), srcId);
                  msgs.add(newMsg);
              } else {
                  aggregateIt.next();
              }
          }
      }
    return msgs;
  }

    @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

    @Override
    public void addBundle(BSPMessageBundle<GraphJobMessage> bundle) {
        addAll(bundle);
    }

    @Override
    public void addBundleRecovery(BSPMessageBundle<GraphJobMessage> bundle) {
        addAllRecovery(bundle);
    }

    @Override
    public void addAll(Iterable<GraphJobMessage> col) {
        for (GraphJobMessage m : col)
            add(m);
    }

    @Override
    public void addAllRecovery(Iterable<GraphJobMessage> col) {

        HashPartitioner<WritableComparable, IntWritable> partitioner = new HashPartitioner();
        for (GraphJobMessage m : col)
        {
           if (m.isVertexMessage()) {
             int partition = partitioner.getPartition(m.getSrcVertexId(), null, peer.getNumPeers());
             String srcPeer = peer.getAllPeerNames()[partition];
             if (peer.getPeerName().equals(srcPeer)) {
                   stateHints.add(m);
             }
             else {
               add(m);
             }
           }
           else {
            add(m);
           }
        }
    }

  @Override
  public void addAll(MessageQueue<GraphJobMessage> otherqueue) {
    GraphJobMessage poll = null;
    while ((poll = otherqueue.poll()) != null) {
      add(poll);
    }
  }

    @Override
    public void add(GraphJobMessage item) {
        if (item.isVertexMessage()) {
            msgPerVertex.add(item.getVertexId(), item);
        } else if (item.isMapMessage() || item.isVerticesSizeMessage()) {
            mapMessages.add(item);
        }
    }

  @Override
  public void clear() {
    msgPerVertex.clear();
  }

  @Override
  public GraphJobMessage poll() {
    if (mapMessages.size() > 0) {
      return mapMessages.poll();
    } else {
      return msgPerVertex.pollFirstEntry();
    }
  }

  @Override
  public int size() {
    return msgPerVertex.size();
  }

  // empty, not needed to implement
  @Override
  public void init(Configuration conf, TaskAttemptID id) {
  }

  @Override
  public void init(Configuration conf, TaskAttemptID id, BSPPeer peerRef) {
      this.peer = peerRef;
  }

  @Override
  public void close() {
    this.clear();
  }

  @Override
  public MessageQueue<GraphJobMessage> getMessageQueue() {
    return this;
  }

  @Override
  public void save() {
    // this function is called when localQ is made to refer to localQ-for-next-iteration
    // i.e after enterBarrier. This means all the messages from peers have been
    // received in msgPerVertex.storage. Hence we can save storage into a shadow
    // map
    msgPerVertex.saveShadow();
  }
}
