/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.metaverse.impl.model.kettle.json;

import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.trans.TransMeta;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class KettleObjectMapperTest {

  private static final String TRANS_JSON = "{\n" +
    "  \"@class\" : \"org.pentaho.di.trans.TransMeta\",\n" +
    "  \"name\" : \"mongo_input\",\n" +
    "  \"description\" : null,\n" +
    "  \"created\" : 1422284845742,\n" +
    "  \"lastmodified\" : 1422284845742,\n" +
    "  \"createdby\" : \"-\",\n" +
    "  \"lastmodifiedby\" : \"-\",\n" +
    "  \"path\" : \"src/test/resources/mongo_input.ktr\",\n" +
    "  \"parameters\" : [ ],\n" +
    "  \"variables\" : [ ],\n" +
    "  \"steps\" : [ ],\n" +
    "  \"connections\" : [  ],\n" +
    "  \"hops\" : [  ]\n" +
    "}";

  KettleObjectMapper mapper;

  List<StdSerializer> serializers;

  List<StdDeserializer> deserializers;

  @Before
  public void setUp() throws Exception {
    serializers = new ArrayList<StdSerializer>();
    deserializers = new ArrayList<StdDeserializer>();
  }

  @Test
  public void testConstructor() {
    mapper = new KettleObjectMapper( null, null );
    mapper = new KettleObjectMapper( serializers, null );
    mapper = new KettleObjectMapper( serializers, deserializers );
  }

  @Test
  public void testReadValue() throws Exception {
    StdDeserializer deserializer = new TransMetaJsonDeserializer( TransMeta.class, null );
    deserializers.add( deserializer );
    mapper = new KettleObjectMapper( null, deserializers );
    assertNotNull( mapper );
    mapper.readValue( TRANS_JSON, TransMeta.class );
  }

  @Test
  public void testWriteValueAsString() throws Exception {
    StdSerializer serializer = new TransMetaJsonSerializer( TransMeta.class );
    serializers.add( serializer );
    mapper = new KettleObjectMapper( serializers, null );
    mapper.writeValueAsString( new TransMeta() );
  }
}
