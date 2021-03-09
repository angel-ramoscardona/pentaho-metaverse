package org.pentaho.metaverse.graph.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.CatalogClientException;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.search.*;
import com.pentaho.di.plugins.catalog.common.CatalogClientBuilderUtil;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import com.pentaho.di.plugins.catalog.read.ReadPayload;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static com.pentaho.di.plugins.catalog.api.entities.search.SearchCriteria.CATALOG_DEFAULT_LIMIT;

public class CatalogLineageClient {

  private static final Logger log = LogManager.getLogger( CatalogLineageClient.class );

  private String catalogUrl;
  private String catalogUsername;
  private String catalogPassword;
  private String catalogTokenUrl;
  private String catalogClientId;
  private String catalogClientSecret;

  public CatalogLineageClient( String catalogUrl,
                                  String catalogUsername,
                                  String catalogPassword,
                                  String catalogTokenUrl,
                                  String catalogClientId,
                                  String catalogClientSecret ) {
    this.catalogUrl = catalogUrl;
    this.catalogUsername = catalogUsername;
    this.catalogPassword = catalogPassword;
    this.catalogTokenUrl = catalogTokenUrl;
    this.catalogClientId = catalogClientId;
    this.catalogClientSecret = catalogClientSecret;
  }

  public void processLineage( ArrayList<String> inputSources, ArrayList<String> outputTargets ) throws Exception {

    ArrayList<String> sourcesIDs = getExistingResourceIDs( inputSources );
    ArrayList<String> targetIDs = getExistingResourceIDs( outputTargets );

    if ( sourcesIDs.isEmpty() || targetIDs.isEmpty() ) {
      log.info( "Either targets or sources IDs could not be found. Can't create lineage." );
      return;
    }

    targetIDs.forEach( targetID -> {
      deleteExistingLineageRelations( targetID );
    } );

    targetIDs.forEach( targetID -> {
      sourcesIDs.forEach( sourceID -> {
        log.info( "Adding lineage relation  between " + sourceID + " and " + targetID + "." );
        addParentLineage( sourceID, targetID );
        log.info( "Lineage relation created between " + sourceID + " and " + targetID + "." );
      } );
    } );
  }

  private ArrayList<String> getExistingResourceIDs( ArrayList<String> sourceNames ) {
    ArrayList<String> resourceIDs = new ArrayList<>();
    sourceNames.forEach( name -> {
      String id = searchResourceByName( name );
      if ( id != null ) {
        resourceIDs.add( id );
      }
    } );
    return resourceIDs;
  }

  private String searchResourceByName( String resourceName ) {

    String resourceId = null;

    try {

      SearchCriteria.SearchCriteriaBuilder searchCriteriaBuilder = new SearchCriteria.SearchCriteriaBuilder();
      searchCriteriaBuilder.searchPhrase( resourceName );
      searchCriteriaBuilder.addFacet( Facet.RESOURCE_TAGS, "" );
      searchCriteriaBuilder.addFacet( Facet.VIRTUAL_FOLDERS, "" );
      searchCriteriaBuilder.addFacet( Facet.DATA_SOURCES, "" );
      searchCriteriaBuilder.addFacet( Facet.RESOURCE_TYPE, "" );
      searchCriteriaBuilder.addFacet( Facet.FILE_SIZE, "" );
      searchCriteriaBuilder.addFacet( Facet.FILE_FORMAT, "" );
      searchCriteriaBuilder.pagingCriteria( new PagingCriteria( 0, StringUtils.isNumeric( "1000" ) ? Integer.valueOf( "1000" ) : CATALOG_DEFAULT_LIMIT ) )
              .sortBySpecs( Collections.singletonList( new SortBySpecs( ReadPayload.SCORE, false ) ) )
              .entityScope( Collections.singletonList( ReadPayload.DATA_RESOURCE ) ).searchType( ReadPayload.ADVANCED )
              .preformedQuery( false ).showCollectionMembers( true ).build();

      CatalogClient catalogClient = getCatalogClient();
      SearchResult result = catalogClient.getSearch().doNew( searchCriteriaBuilder.build() );
      for ( DataResource dataResource : result.getEntities() ) {
        if ( dataResource.getResourcePath().endsWith( "/" + resourceName ) ) {
          resourceId = dataResource.getKey();
        }
      }

    } catch ( CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }

    return resourceId;
  }

  private void addParentLineage( String parentResourceID, String targetResourceID ) {

    try {
      String addParentURL = "/api/v2/lineage/addparent/" + targetResourceID;
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode childNode = objectMapper.createObjectNode();
      ((ObjectNode) childNode).put( "relatedKey", parentResourceID );
      String body = objectMapper.writeValueAsString( childNode );
      StringEntity entity = new StringEntity( body );
      CatalogClient catalogClient = getCatalogClient();
      catalogClient.doPost( addParentURL, entity );
    } catch ( IOException | CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }
  }

  private void deleteExistingLineageRelations( String resourceID ) {
    try {
      String getLineageInfo = "/api/v2/lineage/multihop/" + resourceID + "?fieldLineage=false&field=&upstream=1&downstream=0&lineageType=ACCEPTED%2CSUGGESTED";
      CatalogClient catalogClient = getCatalogClient();
      CloseableHttpResponse httpResponse = catalogClient.doGet( getLineageInfo );
      String json_string = EntityUtils.toString( httpResponse.getEntity() );
      JSONObject jsonResponse = (JSONObject) new JSONParser().parse( json_string );
      ((JSONArray) jsonResponse.get( "edges" )).forEach( node -> {
        JSONObject jsonObject = (JSONObject) node;
        String type = (String) jsonObject.get( "type" );
        if ( type != null && type.equals( "edge" ) ) {
          String lineageRelationID = (String) jsonObject.get( "target" );
          lineageRelationID = lineageRelationID.substring( 0, lineageRelationID.lastIndexOf( "_" ) );
          deleteExistingLineageRelation( lineageRelationID );
        }
      } );
    } catch ( IOException | CatalogClientException | ParseException e ) {
      log.error( e.getMessage(), e );
    }
  }

  private void deleteExistingLineageRelation( String lineageRelationID ) {
    try {
      String deleteLineageRelation = "/api/v2/lineage/" + lineageRelationID;
      CatalogClient catalogClient = getCatalogClient();
      catalogClient.doDelete( deleteLineageRelation );
      log.info( "Deleted old lineage relation " + lineageRelationID );
    } catch ( IOException | CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }
  }

  private CatalogClient getCatalogClient() {
    CatalogDetails catalogDetails = new CatalogDetails();
    catalogDetails.setAuthType( "1" );
    catalogDetails.setUrl( catalogUrl );
    catalogDetails.setUsername( catalogUsername );
    catalogDetails.setPassword( catalogPassword );
    catalogDetails.setTokenUrl( catalogTokenUrl );
    catalogDetails.setClientId( catalogClientId );
    catalogDetails.setClientSecret( catalogClientSecret );

    CatalogClient catalogClient;
    try {
      CatalogClientBuilderUtil catalogClientBuilderUtil = new CatalogClientBuilderUtil( catalogDetails );
      catalogClient = catalogClientBuilderUtil.getCatalogClient();
      catalogClient.login();
    } catch ( CatalogClientException cce ) {
      throw new RuntimeException( cce );
    }
    return catalogClient;
  }
}
