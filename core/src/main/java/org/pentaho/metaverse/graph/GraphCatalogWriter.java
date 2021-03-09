/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.metaverse.graph;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.graph.catalog.CatalogLineageClient;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The GraphMLWriter class contains methods for writing a metaverse graph model in GraphML format
 * 
 */
public class GraphCatalogWriter extends BaseGraphWriter {

  private static final Logger log = LogManager.getLogger( GraphCatalogWriter.class );

  private CatalogLineageClient lineageClient;

  public GraphCatalogWriter( String catalogUrl,
                             String catalogUsername,
                             String catalogPassword,
                             String catalogTokenUrl,
                             String catalogClientId,
                             String catalogClientSecret ) {
    super();
    lineageClient = new CatalogLineageClient( catalogUrl,
            catalogUsername,
            catalogPassword,
            catalogTokenUrl,
            catalogClientId,
            catalogClientSecret );
  }

  @Override
  public void outputGraphImpl( Graph graph, OutputStream out ) throws IOException {

    log.info( "Stating lineage processing." );

    ArrayList<String> inputSources = new ArrayList<>();
    ArrayList<String> outputTargets = new ArrayList<>();

    GremlinPipeline<Graph, Vertex> inputNodesPipe =
            new GremlinPipeline<Graph, Vertex>( graph )
                    //.V( DictionaryConst.PROPERTY_NAME, "baltimore_art.csv" )
                    .V()
                    .has( DictionaryConst.PROPERTY_TYPE, DictionaryConst.NODE_TYPE_TRANS_STEP )
                    .in( DictionaryConst.LINK_READBY )
                    .cast( Vertex.class );
    List<Vertex> inputVertexes = inputNodesPipe.toList();
    inputVertexes.forEach( vertex -> {
      String sourceName = vertex.getProperty( DictionaryConst.PROPERTY_PATH );
      if ( sourceName != null && !sourceName.equals( "" ) ) {
        inputSources.add( getSourceName( sourceName ) );
      }
    } );

    GremlinPipeline<Graph, Vertex> outputNodesPipe =
            new GremlinPipeline<Graph, Vertex>( graph )
                    //.V( DictionaryConst.PROPERTY_NAME, "csvToTextOut.txt" )
                    .V()
                    .has( DictionaryConst.PROPERTY_TYPE, DictionaryConst.NODE_TYPE_TRANS_STEP )
                    .out( DictionaryConst.LINK_WRITESTO )
                    .cast( Vertex.class );
    List<Vertex> outputVertexes = outputNodesPipe.toList();
    outputVertexes.forEach( vertex -> {
      String sourceName = vertex.getProperty( DictionaryConst.PROPERTY_PATH );
      if ( sourceName != null && !sourceName.equals( "" ) ) {
        outputTargets.add( getSourceName( sourceName ) );
      }
    } );

    try {
      lineageClient.processLineage( inputSources, outputTargets );
    } catch ( Exception e ) {
      log.error( e.getMessage(), e );
    }

    log.info( "Lineage processing done." );
  }

  private String getSourceName( String fullName ) {
    String sourceName = null;
    if ( fullName.contains( "/" ) ) {
      sourceName = fullName.substring( fullName.lastIndexOf( "/" ) + 1 );
    } else {
      sourceName = fullName;
    }
    return sourceName;
  }
}
