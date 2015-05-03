/*
 * Copyright (c) 2006-2011 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 */
package org.nuxeo.ecm.core.opencmis.impl;

import org.nuxeo.ecm.core.io.impl.plugins.NuxeoArchiveWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStream;

import org.dom4j.DocumentException;
import org.nuxeo.ecm.core.api.ClientException;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Random;

import com.google.inject.Inject;

import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.Properties;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.data.PropertyString;
import org.apache.chemistry.opencmis.commons.spi.BindingsObjectFactory;
import org.apache.chemistry.opencmis.commons.spi.Holder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.io.DocumentPipe;
import org.nuxeo.ecm.core.io.DocumentReader;
import org.nuxeo.ecm.core.io.DocumentWriter;
import org.nuxeo.ecm.core.io.impl.DocumentPipeImpl;
import org.nuxeo.ecm.core.io.impl.plugins.DocumentTreeReader;
import org.nuxeo.ecm.core.io.impl.plugins.XMLDocumentWriter;
import org.nuxeo.ecm.core.io.impl.plugins.SingleDocumentReader;
import org.nuxeo.ecm.core.lifecycle.LifeCycle;
import org.nuxeo.ecm.automation.core.util.DateTimeFormat;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.ecm.core.opencmis.impl.server.NuxeoTypeHelper;
import org.nuxeo.ecm.core.schema.utils.DateParser;
import org.nuxeo.ecm.core.storage.sql.ra.PoolingRepositoryFactory;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;

@RunWith(FeaturesRunner.class)
@Features({ CmisFeature.class, CmisFeatureConfiguration.class })
@Deploy({ "org.nuxeo.ecm.webengine.core", //
        "org.nuxeo.ecm.automation.core" //
})
@LocalDeploy("org.nuxeo.ecm.core.opencmis.tests.tests:OSGI-INF/types-contrib.xml")
@RepositoryConfig(cleanup = Granularity.METHOD, repositoryFactoryClass = PoolingRepositoryFactory.class)
public class TestComplex extends TestCmisBindingBase {

    @Inject
    protected CoreSession coreSession;

    @Before
    public void setUp() throws Exception {
        setUpBinding(coreSession);
        setUpData(coreSession);
    }

    @After
    public void tearDown() {
        tearDownBinding();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetComplexListProperty() throws Exception {
    	
    	
        // Enable complex properties
        Framework.getProperties().setProperty(NuxeoTypeHelper.ENABLE_COMPLEX_PROPERTIES, "true");

        int RECORD_COUNT = 500;
        
        // Create a complex property to encode
        List<Map<String, Object>> propList = createComplexPropertyList(RECORD_COUNT);

        CoreSession session = coreSession;
        DocumentModel rootDocument = session.getRootDocument();
        DocumentModel workspace = session.createDocumentModel(rootDocument.getPathAsString(), RECORD_COUNT+"", "Workspace");
        workspace.setProperty("dublincore", "title", RECORD_COUNT+"");
        workspace = session.createDocument(workspace);
        
        DocumentModel doc = session.createDocumentModel(workspace.getPathAsString(), null, "ComplexFileRon");
        doc.setPropertyValue("complexTestRon:listItem", (Serializable) propList);
        doc.setProperty("dublincore", "title", RECORD_COUNT+"");

        doc = session.createDocument(doc);
        session.save();
        doc.refresh();

        try {
            DocumentReader reader = new DocumentTreeReader(session, doc);
            File archiveFile = new File(new File(System.getProperty("user.home")),RECORD_COUNT+".zip");
            DocumentWriter writer = new NuxeoArchiveWriter(archiveFile);
            DocumentPipe pipe = new DocumentPipeImpl();
            pipe.setReader(reader);
            pipe.setWriter(writer);
            pipe.run();
            writer.close();
            reader.close();
        } catch (IOException e) {
        }
    }

    private List<Map<String, Object>> createComplexPropertyList(int listSize) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 1; i <= listSize; i++) {
            HashMap<String, Object> map = new HashMap<String, Object>();
            list.add(map);

            Random r = new Random();

            //stringProp should be populated with a UUID.  
            map.put("stringProp", UUID.randomUUID().toString());
            List<String> arrayProp = new ArrayList<String>();
            //arrayProp should be populated with 1-3 UUIDs.
            map.put("arrayProp", arrayProp);
            int l = r.nextInt(3)+1;
            for (int j = 1; j <= l; j++) {
                arrayProp.add(UUID.randomUUID().toString());
            }
            // arrayProp2 & arrayProp3 should be populated with 1-3, 4-6 character values.
            List<String> arrayProp2 = new ArrayList<String>();
            map.put("arrayProp2", arrayProp2);
            l = r.nextInt(3)+1;
            for (int j = 1; j <= l; j++) {
                arrayProp2.add(Long.toHexString(r.nextLong()).substring(0, 6-r.nextInt(3)));
            }
            List<String> arrayProp3 = new ArrayList<String>();
            map.put("arrayProp3", arrayProp3);
            l = r.nextInt(3)+1;
            for (int j = 1; j <= l; j++) {
                arrayProp3.add(Long.toHexString(r.nextLong()).substring(0, 6-r.nextInt(3)));
            }
            map.put("intProp", Integer.valueOf(i));

        }
        return list;
    }


}
