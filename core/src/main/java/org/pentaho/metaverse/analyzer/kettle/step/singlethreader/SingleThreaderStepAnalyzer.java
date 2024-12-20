/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.metaverse.analyzer.kettle.step.singlethreader;

import com.tinkerpop.blueprints.Vertex;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.singlethreader.SingleThreaderMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.StepField;
import org.pentaho.metaverse.api.analyzer.kettle.KettleAnalyzerUtil;
import org.pentaho.metaverse.api.analyzer.kettle.step.IClonableStepAnalyzer;
import org.pentaho.metaverse.api.analyzer.kettle.step.StepAnalyzer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SingleThreaderStepAnalyzer extends StepAnalyzer<SingleThreaderMeta> {

  @Override protected Set<StepField> getUsedFields( SingleThreaderMeta meta ) {
    return null;
  }

  @Override protected void customAnalyze( SingleThreaderMeta meta, IMetaverseNode rootNode ) throws
    MetaverseAnalyzerException {

    // Add the injector/retrieval step info to the root node properties as well as the batch size
    rootNode.setProperty( "injectorStep", parentTransMeta.environmentSubstitute( meta.getInjectStep() ) );
    rootNode.setProperty( "retrieveStep", parentTransMeta.environmentSubstitute( meta.getRetrieveStep() ) );
    rootNode.setProperty( "batchSize", parentTransMeta.environmentSubstitute( meta.getBatchSize() ) );
    rootNode.setProperty( "batchTime", parentTransMeta.environmentSubstitute( meta.getBatchTime() ) );

    KettleAnalyzerUtil.analyze( this, parentTransMeta, meta, rootNode );
  }

  @Override public void postAnalyze( final SingleThreaderMeta meta ) throws MetaverseAnalyzerException {

    final String injectStepName = parentTransMeta.environmentSubstitute( meta.getInjectStep() );
    final String retrieveStepName = parentTransMeta.environmentSubstitute( meta.getRetrieveStep() );

    final String transformationPath = parentTransMeta.environmentSubstitute( meta.getFileName() );
    final TransMeta subTransMeta = KettleAnalyzerUtil.getSubTransMeta( meta );
    subTransMeta.setFilename( transformationPath );

    final List<Vertex> singleThreaderOutputFields = findFieldVertices( parentTransMeta, parentStepMeta.getName() );

    // traverse the output fields of this single threader step; for each output field add a "derives" link to an
    // output step of the injectorStepVertex with the same field name get the vertex corresponding to this step
    final List<Vertex> injectorOutputFields = findFieldVertices( subTransMeta, injectStepName );
    for ( final Vertex outputField : singleThreaderOutputFields ) {
      for ( final Vertex derivedField : injectorOutputFields ) {
        if ( outputField.getProperty( DictionaryConst.PROPERTY_NAME ).equals(
          derivedField.getProperty( DictionaryConst.PROPERTY_NAME ) ) ) {
          getMetaverseBuilder().addLink( outputField, DictionaryConst.LINK_DERIVES, derivedField );
          break;
        }
      }
    }

    // traverse the output fields of the retriever step; for each output field add a "derives" link to each single
    // threader output stpe with the same name as the retriever output field;
    final List<Vertex> retrieverOutputFields = findFieldVertices( subTransMeta, retrieveStepName );
    for ( final Vertex outputField : retrieverOutputFields ) {

      for ( final Vertex derivedField : singleThreaderOutputFields ) {
        if ( outputField.getProperty( DictionaryConst.PROPERTY_NAME ).equals(
          derivedField.getProperty( DictionaryConst.PROPERTY_NAME ) ) ) {
          getMetaverseBuilder().addLink( outputField, DictionaryConst.LINK_DERIVES, derivedField );
          break;
        }
      }
    }
  }

  @Override public Set<Class<? extends BaseStepMeta>> getSupportedSteps() {
    return new HashSet<Class<? extends BaseStepMeta>>() {
      {
        add( SingleThreaderMeta.class );
      }
    };
  }

  @Override public IClonableStepAnalyzer newInstance() {
    return new SingleThreaderStepAnalyzer();
  }
}
