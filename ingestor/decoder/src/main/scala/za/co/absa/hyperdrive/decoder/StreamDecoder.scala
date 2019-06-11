/*
 *  Copyright 2019 ABSA Group Limited
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package za.co.absa.hyperdrive.decoder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.DataStreamReader
/**
  * Base class for all StreamDecoders.
  *
  * The process for adding a new StreamDecoder implementation is:
  * <ul>
  * <li>1. Create the implementation package (e.g. za.co.absa.hyperdrive.decoder.impl.protobuffer).</li>
  * <li>2. Add the extension of this class (e.g. za.co.absa.hyperdrive.decoder.impl.protobuffer.ProtobufferStreamDecoder).</li>
  * <li>3. Create the factory package (e.g. za.co.absa.hyperdrive.decoder.factories.protobuffer).</li>
  * <li>4. Add the factory as an implementation of [[za.co.absa.hyperdrive.decoder.StreamDecoderFactory]].</li>
  * <li>5. Add the factory to the abstract stream decoder factory at [[za.co.absa.hyperdrive.decoder.factories.StreamDecoderAbstractFactory]] as described in the class documentation.</li>
  * <li>6. ITEM 5 WILL CHANGE SOON IN FAVOR OF A MAVEN ARCHETYPE TO BE MADE AVAILABLE TO CLASSPATH.
  * </ul>
  */
abstract class StreamDecoder {

  /**
    * Decodes the stream into the correct format, depending on the implementation.
    */
  def decode(streamReader: DataStreamReader): DataFrame
}
