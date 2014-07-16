/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package com.pentaho.metaverse.analyzer.kettle;

import com.pentaho.metaverse.util.MetaverseUtil;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.platform.api.metaverse.IAnalyzer;
import org.pentaho.platform.api.metaverse.IMetaverseBuilder;
import org.pentaho.platform.api.metaverse.IMetaverseNode;
import org.pentaho.platform.api.metaverse.IMetaverseObjectFactory;

/**
 * DatabaseConnectionAnalyzer collects metadata about a PDI database connection
 */
public class DatabaseConnectionAnalyzer implements IAnalyzer<DatabaseMeta> {

  /** A reference to the metaverse builder. */
  protected IMetaverseBuilder metaverseBuilder;

  /**
   * Analyzes a database connection for metadata.
   * 
   * @param object
   *          the object
   * @see org.pentaho.platform.api.metaverse.IAnalyzer#analyze(java.lang.Object)
   */
  @Override
  public IMetaverseNode analyze( DatabaseMeta object ) {

    if ( object == null ) {
      return null;
    }

    // TODO get unique ID and set it on the node
    IMetaverseNode node = getMetaverseObjectFactory().createNodeObject( "TODO" );

    // TODO Is this appropriate? What are our name conventions?
    node.setName( object.getName() );

    // TODO What are our valid list of types?
    node.setType( "datasource" );

    int accessType = object.getAccessType();
    node.setProperty( "accessType", accessType );

    String accessTypeDesc = object.getAccessTypeDesc();
    node.setProperty( "accessTypeDesc", accessTypeDesc );

    String databaseName = object.getDatabaseName();
    node.setProperty( "databaseName", databaseName );

    String port = object.getDatabasePortNumberString();
    node.setProperty( "port", port );

    String host = object.getHostname();
    node.setProperty( "hostName", host );

    String user = object.getUsername();
    node.setProperty( "userName", user );

    String pass = object.getPassword();
    node.setProperty( "password", pass );

    boolean shared = object.isShared();
    node.setProperty( "shared", shared );

    // TODO If these attributes are important, we will need to
    // TODO account for the same attributes in partitions in clusters

    metaverseBuilder.addNode( node );

    return node;

  }

  /**
   * package-protected for unit testing
   * @return factory for object creation
   */
  IMetaverseObjectFactory getMetaverseObjectFactory() {
    return MetaverseUtil.getMetaverseObjectFactory();
  }

  /**
   * @see org.pentaho.platform.api.metaverse.IAnalyzer#
   *      setMetaverseBuilder(org.pentaho.platform.api.metaverse.IMetaverseBuilder)
   */
  @Override
  public void setMetaverseBuilder( IMetaverseBuilder builder ) {
    this.metaverseBuilder = builder;
  }

}