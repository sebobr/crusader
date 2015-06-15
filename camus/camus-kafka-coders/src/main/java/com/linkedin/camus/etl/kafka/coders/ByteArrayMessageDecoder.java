package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;

import java.util.Properties;


/** Original code taken from Yash Ranadive at http://etl.svbtle.com/setting-up-camus-linkedins-kafka-to-hdfs-pipeline
 * MessageDecoder class that will convert the payload into a ByteArray object,
 * System.currentTimeMillis() will be used to set CamusWrapper's
 * timestamp property

 * This MessageDecoder returns a CamusWrapper that works with ByteArray payloads,
 */
public class ByteArrayMessageDecoder extends MessageDecoder<byte[], byte[]> {
  private static final Logger log = Logger.getLogger(ByteArrayMessageDecoder.class);

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<byte[]> decode(byte[] payload) {
    //Push the raw payload and add the current time
    return new CamusWrapper<byte[]>(payload, System.currentTimeMillis());
  }
}
