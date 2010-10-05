/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.opencmis.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.AclCapabilities;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderContainer;
import org.apache.chemistry.opencmis.commons.data.ObjectList;
import org.apache.chemistry.opencmis.commons.data.ObjectParentData;
import org.apache.chemistry.opencmis.commons.data.Properties;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.data.PropertyString;
import org.apache.chemistry.opencmis.commons.data.RepositoryCapabilities;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.PropertyDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionContainer;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionList;
import org.apache.chemistry.opencmis.commons.enums.AclPropagation;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.CapabilityAcl;
import org.apache.chemistry.opencmis.commons.enums.CapabilityChanges;
import org.apache.chemistry.opencmis.commons.enums.CapabilityContentStreamUpdates;
import org.apache.chemistry.opencmis.commons.enums.CapabilityJoin;
import org.apache.chemistry.opencmis.commons.enums.CapabilityQuery;
import org.apache.chemistry.opencmis.commons.enums.CapabilityRenditions;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.enums.SupportedPermissions;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.exceptions.CmisConstraintException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisInvalidArgumentException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.chemistry.opencmis.commons.spi.BindingsObjectFactory;
import org.apache.chemistry.opencmis.commons.spi.DiscoveryService;
import org.apache.chemistry.opencmis.commons.spi.Holder;
import org.apache.chemistry.opencmis.commons.spi.MultiFilingService;
import org.apache.chemistry.opencmis.commons.spi.NavigationService;
import org.apache.chemistry.opencmis.commons.spi.ObjectService;
import org.apache.chemistry.opencmis.commons.spi.RepositoryService;
import org.apache.chemistry.opencmis.server.support.query.CalendarHelper;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nuxeo.ecm.core.api.security.SecurityConstants;
import org.nuxeo.ecm.core.opencmis.tests.Helper;

/**
 * Tests that hit directly the server APIs.
 */
public class TestNuxeoBinding extends NuxeoBindingTestCase {

    public static final String NUXEO_ROOT_TYPE = "Root"; // from Nuxeo

    public static final String NUXEO_ROOT_NAME = ""; // NuxeoPropertyDataName;

    // stream content with non-ASCII characters
    public static final String STREAM_CONTENT = "Caf\u00e9 Diem\none\0two";

    protected RepositoryService repoService;

    protected ObjectService objService;

    protected NavigationService navService;

    protected MultiFilingService filingService;

    protected DiscoveryService discService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        nuxeotc.openSession();
        Helper.makeNuxeoRepository(nuxeotc.getSession());
        nuxeotc.closeSession();
        repoService = binding.getRepositoryService();
        objService = binding.getObjectService();
        navService = binding.getNavigationService();
        filingService = binding.getMultiFilingService();
        discService = binding.getDiscoveryService();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected String createDocument(String name, String folderId, String typeId) {
        return objService.createDocument(repositoryId,
                createBaseDocumentProperties(name, typeId), folderId, null,
                null, null, null, null, null);
    }

    protected String createFolder(String name, String folderId, String typeId) {
        return objService.createFolder(repositoryId,
                createBaseDocumentProperties(name, typeId), folderId, null,
                null, null, null);
    }

    protected Properties createBaseDocumentProperties(String name, String typeId) {
        BindingsObjectFactory factory = binding.getObjectFactory();
        List<PropertyData<?>> props = new ArrayList<PropertyData<?>>();
        props.add(factory.createPropertyIdData(PropertyIds.NAME, name));
        props.add(factory.createPropertyIdData(PropertyIds.OBJECT_TYPE_ID,
                typeId));
        return factory.createPropertiesData(props);
    }

    protected Properties createProperties(String key, String value) {
        BindingsObjectFactory factory = binding.getObjectFactory();
        PropertyString prop = factory.createPropertyStringData(key, value);
        return factory.createPropertiesData(Collections.<PropertyData<?>> singletonList(prop));
    }

    protected ObjectData getObject(String id) {
        return objService.getObject(repositoryId, id, null, Boolean.FALSE,
                IncludeRelationships.NONE, null, Boolean.FALSE, Boolean.FALSE,
                null);
    }

    protected ObjectData getObjectByPath(String path) {
        return objService.getObjectByPath(repositoryId, path, null, null, null,
                null, null, null, null);
    }

    protected ObjectList query(String statement) {
        return discService.query(repositoryId, statement, null, null, null,
                null, null, null, null);
    }

    protected static Object getValue(ObjectData data, String key) {
        return data.getProperties().getProperties().get(key).getFirstValue();
    }

    protected static Object getValues(ObjectData data, String key) {
        return data.getProperties().getProperties().get(key).getValues();
    }

    protected static String getString(ObjectData data, String key) {
        return (String) getValue(data, key);
    }

    protected static Object getQueryValue(ObjectData data, String queryName) {
        Properties properties = data.getProperties();
        for (PropertyData<?> pd : properties.getPropertyList()) {
            if (queryName.equals(pd.getQueryName())) {
                return pd.getFirstValue();
            }
        }
        return null;
    }

    @Test
    public void testGetRepositoryInfos() {
        List<RepositoryInfo> infos = repoService.getRepositoryInfos(null);
        assertEquals(1, infos.size());
        checkInfo(infos.get(0));
    }

    @Test
    public void testGetRepositoryInfo() {
        RepositoryInfo info = repoService.getRepositoryInfo(repositoryId, null);
        checkInfo(info);
    }

    public void checkInfo(RepositoryInfo info) {
        assertEquals(repositoryId, info.getId());
        assertEquals("Nuxeo Repository " + repositoryId, info.getName());
        assertEquals("Nuxeo Repository " + repositoryId, info.getDescription());
        assertEquals("Nuxeo", info.getVendorName());
        assertEquals("Nuxeo OpenCMIS Connector", info.getProductName());
        assertEquals("5.4.0-SNAPSHOT", info.getProductVersion());
        assertEquals(rootFolderId, info.getRootFolderId());
        assertEquals("Guest", info.getPrincipalIdAnonymous());
        assertNull(info.getLatestChangeLogToken());
        assertEquals("1.0", info.getCmisVersionSupported());
        // TODO assertEquals("...", info.getThinClientUri());
        assertEquals(Boolean.TRUE, info.getChangesIncomplete());
        assertEquals(
                Arrays.asList(BaseTypeId.CMIS_DOCUMENT, BaseTypeId.CMIS_FOLDER),
                info.getChangesOnType());
        assertEquals(SecurityConstants.EVERYONE, info.getPrincipalIdAnyone());
        RepositoryCapabilities caps = info.getCapabilities();
        assertEquals(CapabilityAcl.NONE, caps.getAclCapability());
        assertEquals(CapabilityChanges.PROPERTIES, caps.getChangesCapability());
        assertEquals(CapabilityContentStreamUpdates.PWCONLY,
                caps.getContentStreamUpdatesCapability());
        assertEquals(CapabilityJoin.INNERANDOUTER, caps.getJoinCapability());
        assertEquals(CapabilityQuery.BOTHCOMBINED, caps.getQueryCapability());
        assertEquals(CapabilityRenditions.NONE, caps.getRenditionsCapability());
        AclCapabilities aclCaps = info.getAclCapabilities();
        assertEquals(AclPropagation.REPOSITORYDETERMINED,
                aclCaps.getAclPropagation());
        assertEquals(Collections.emptyMap(), aclCaps.getPermissionMapping());
        assertEquals(Collections.emptyList(), aclCaps.getPermissions());
        assertEquals(SupportedPermissions.BASIC,
                aclCaps.getSupportedPermissions());
    }

    @Test
    public void testGetTypeDefinition() {
        TypeDefinition type;

        type = repoService.getTypeDefinition(repositoryId, "cmis:folder", null);
        assertEquals(Boolean.FALSE, type.isCreatable());
        assertNull(type.getParentTypeId());
        assertEquals("cmis:folder", type.getLocalName());
        assertTrue(type.getPropertyDefinitions().containsKey("dc:title"));

        type = repoService.getTypeDefinition(repositoryId, "Folder", null);
        assertEquals(Boolean.TRUE, type.isCreatable());
        assertEquals("cmis:folder", type.getParentTypeId());
        assertEquals("Folder", type.getLocalName());

        type = repoService.getTypeDefinition(repositoryId, "cmis:document",
                null);
        assertEquals(Boolean.FALSE, type.isCreatable());
        assertNull(type.getParentTypeId());
        assertEquals("cmis:document", type.getLocalName());
        assertTrue(type.getPropertyDefinitions().containsKey("dc:title"));
        assertTrue(type.getPropertyDefinitions().containsKey(
                "cmis:contentStreamFileName"));

        try {
            // nosuchtype, Document is mapped to cmis:document
            repoService.getTypeDefinition(repositoryId, "Document", null);
            fail();
        } catch (CmisInvalidArgumentException e) {
            // ok
        }

        type = repoService.getTypeDefinition(repositoryId, "Note", null);
        assertEquals(Boolean.TRUE, type.isCreatable());
        assertEquals("cmis:document", type.getParentTypeId());
        assertEquals("Note", type.getLocalName());
        assertTrue(type.getPropertyDefinitions().containsKey("note"));
    }

    @Test
    public void testGetTypeChildren() {
        TypeDefinitionList types = repoService.getTypeChildren(repositoryId,
                "cmis:folder", Boolean.FALSE, null, null, null);
        List<String> ids = new LinkedList<String>();
        for (TypeDefinition type : types.getList()) {
            assertNull(type.getPropertyDefinitions());
            ids.add(type.getId());
        }
        assertTrue(ids.contains("Folder"));
        assertTrue(ids.contains("Root"));
        assertTrue(ids.contains("Domain"));
        assertTrue(ids.contains("OrderedFolder"));
        assertTrue(ids.contains("Workspace"));

        // check property definition inclusion
        types = repoService.getTypeChildren(repositoryId,
                BaseTypeId.CMIS_FOLDER.value(), Boolean.TRUE, null, null, null);
        for (TypeDefinition type : types.getList()) {
            Map<String, PropertyDefinition<?>> pd = type.getPropertyDefinitions();
            assertNotNull(pd);
            // dublincore in all types
            assertTrue(pd.keySet().contains("dc:title"));
        }

        types = repoService.getTypeChildren(repositoryId,
                BaseTypeId.CMIS_DOCUMENT.value(), Boolean.TRUE, null, null,
                null);
        ids = new LinkedList<String>();
        for (TypeDefinition type : types.getList()) {
            Map<String, PropertyDefinition<?>> pd = type.getPropertyDefinitions();
            assertNotNull(pd);
            ids.add(type.getId());
            // dublincore in all types
            assertTrue(pd.keySet().contains("dc:title"));
        }
        assertTrue(ids.contains("File"));
        assertTrue(ids.contains("Note"));
        assertTrue(ids.contains("MyDocType"));

        // nonexistent type
        try {
            repoService.getTypeChildren(repositoryId, "nosuchtype",
                    Boolean.TRUE, null, null, null);
            fail();
        } catch (CmisInvalidArgumentException e) {
            // ok
        }
    }

    @Test
    public void testGetTypeDescendants() {
        List<TypeDefinitionContainer> desc = repoService.getTypeDescendants(
                repositoryId, "cmis:folder", null, Boolean.FALSE, null);
        assertTrue(desc.size() > 2);
        TypeDefinition t = null;
        for (TypeDefinitionContainer tc : desc) {
            TypeDefinition type = tc.getTypeDefinition();
            if (type.getId().equals("OrderedFolder")) {
                t = type;
            }
        }
        assertNotNull(t);

        // nonexistent type
        try {
            repoService.getTypeDescendants(repositoryId, "nosuchtype", null,
                    Boolean.FALSE, null);
            fail();
        } catch (CmisInvalidArgumentException e) {
            // ok
        }
    }

    @Test
    public void testRoot() {
        ObjectData root = getObject(rootFolderId);
        assertNotNull(root.getId());
        assertEquals(NUXEO_ROOT_TYPE,
                getString(root, PropertyIds.OBJECT_TYPE_ID));
        assertEquals(NUXEO_ROOT_NAME, getString(root, PropertyIds.NAME));
        assertEquals("/", getString(root, PropertyIds.PATH));

        // root parent
        ObjectData parent = navService.getFolderParent(repositoryId,
                rootFolderId, null, null);
        assertNull(parent);
        List<ObjectParentData> parents = navService.getObjectParents(
                repositoryId, rootFolderId, null, null, null, null, null, null);
        assertEquals(0, parents.size());
    }

    @Test
    public void testGetObjectByPath() {
        ObjectData ob;

        ob = getObjectByPath("/testfolder1/testfile1");
        assertEquals("testfile1_Title", getString(ob, "dc:title"));

        // works by cmis:name too, needed for Adobe Drive 2
        ob = getObjectByPath("/testfolder1_Title/testfile1_Title");
        assertEquals("testfile1_Title", getString(ob, "dc:title"));

        // cannot mix both
        try {
            getObjectByPath("/testfolder1/testfile1_Title");
            fail();
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testCreateDocument() {
        String id = createDocument("doc1", rootFolderId, "File");
        assertNotNull(id);
        ObjectData data = getObject(id);
        assertEquals(id, data.getId());
        assertEquals("doc1", getString(data, PropertyIds.NAME));
    }

    protected String createDocumentMyDocType() {
        BindingsObjectFactory factory = binding.getObjectFactory();
        List<PropertyData<?>> props = new ArrayList<PropertyData<?>>();
        props.add(factory.createPropertyIdData(PropertyIds.NAME, "mydoc"));
        props.add(factory.createPropertyIdData(PropertyIds.OBJECT_TYPE_ID,
                "MyDocType"));
        props.add(factory.createPropertyStringData("my:string", "abc"));
        props.add(factory.createPropertyBooleanData("my:boolean", Boolean.TRUE));
        props.add(factory.createPropertyIntegerData("my:integer",
                BigInteger.valueOf(123)));
        props.add(factory.createPropertyIntegerData("my:long",
                BigInteger.valueOf(123)));
        props.add(factory.createPropertyDecimalData("my:double",
                BigDecimal.valueOf(123.456)));
        GregorianCalendar expectedDate = Helper.getCalendar(2010, 9, 30, 16, 4,
                55);
        props.add(factory.createPropertyDateTimeData("my:date", expectedDate));
        Properties properties = factory.createPropertiesData(props);
        String id = objService.createDocument(repositoryId, properties,
                rootFolderId, null, null, null, null, null, null);
        assertNotNull(id);
        return id;
    }

    @Test
    public void testCreateDocumentMyDocType() {
        String id = createDocumentMyDocType();
        ObjectData data = getObject(id);
        assertEquals(id, data.getId());
        assertEquals("mydoc", getString(data, PropertyIds.NAME));
        assertEquals("MyDocType", getString(data, PropertyIds.OBJECT_TYPE_ID));
        assertEquals("abc", getString(data, "my:string"));
        assertEquals(Boolean.TRUE, getValue(data, "my:boolean"));
        assertEquals(BigInteger.valueOf(123), getValue(data, "my:integer"));
        assertEquals(BigInteger.valueOf(123), getValue(data, "my:long"));
        assertEquals(BigDecimal.valueOf(123.456), getValue(data, "my:double"));
        GregorianCalendar date = (GregorianCalendar) getValue(data, "my:date");
        GregorianCalendar expectedDate = Helper.getCalendar(2010, 9, 30, 16, 4,
                55);
        if (!CalendarHelper.toString(expectedDate).equals(
                CalendarHelper.toString(date))) {
            // there may be a timezone difference if the database
            // doesn't store timezones -> try with local timezone
            TimeZone tz = TimeZone.getDefault();
            GregorianCalendar localDate = Helper.getCalendar(2010, 9, 30, 16,
                    4, 55, tz);
            assertEquals(CalendarHelper.toString(localDate),
                    CalendarHelper.toString(date));
        }
    }

    @Test
    public void testCreateDocumentWithContentStream() throws Exception {
        // null filename passed on purpose, size ignored by Nuxeo
        ContentStream cs = new ContentStreamImpl(null, "text/plain",
                Helper.FILE1_CONTENT);
        String id = objService.createDocument(repositoryId,
                createBaseDocumentProperties("doc1.txt", "File"), rootFolderId,
                cs, VersioningState.NONE, null, null, null, null);
        assertNotNull(id);
        ObjectData data = getObject(id);
        assertEquals(id, data.getId());
        assertEquals("doc1.txt", getString(data, PropertyIds.NAME));
        cs = objService.getContentStream(repositoryId, id, null, null, null,
                null);
        assertNotNull(cs);
        assertEquals("text/plain", cs.getMimeType());
        assertEquals("doc1.txt", cs.getFileName());
        assertEquals(Helper.FILE1_CONTENT.length(), cs.getLength());
        assertEquals(Helper.FILE1_CONTENT, Helper.read(cs.getStream(), "UTF-8"));
    }

    @Test
    public void testUpdateProperties() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1/testfile1");
        assertEquals("testfile1_Title", getString(ob, "dc:title"));

        Properties props = createProperties("dc:title", "new title");
        Holder<String> objectIdHolder = new Holder<String>(ob.getId());
        objService.updateProperties(repositoryId, objectIdHolder, null, props,
                null);
        assertEquals(ob.getId(), objectIdHolder.getValue());

        ob = getObject(ob.getId());
        assertEquals("new title", getString(ob, "dc:title"));
    }

    @Test
    public void testContentStream() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1/testfile1");

        // get stream
        ContentStream cs = objService.getContentStream(repositoryId,
                ob.getId(), null, null, null, null);
        assertNotNull(cs);
        assertEquals("text/plain", cs.getMimeType());
        assertEquals("testfile.txt", cs.getFileName());
        assertEquals(Helper.FILE1_CONTENT.length(), cs.getLength());
        assertEquals(Helper.FILE1_CONTENT, Helper.read(cs.getStream(), "UTF-8"));

        // set stream

        cs = new ContentStreamImpl("foo.txt", "text/plain; charset=UTF-8",
                STREAM_CONTENT);
        Holder<String> objectIdHolder = new Holder<String>(ob.getId());
        objService.setContentStream(repositoryId, objectIdHolder, Boolean.TRUE,
                null, cs, null);
        assertEquals(ob.getId(), objectIdHolder.getValue());

        // refetch
        cs = objService.getContentStream(repositoryId, ob.getId(), null, null,
                null, null);
        assertNotNull(cs);
        assertEquals("text/plain; charset=UTF-8", cs.getMimeType());
        assertEquals("foo.txt", cs.getFileName());
        assertEquals(STREAM_CONTENT.getBytes("UTF-8").length, cs.getLength());
        assertEquals(STREAM_CONTENT, Helper.read(cs.getStream(), "UTF-8"));

        // delete
        objService.deleteContentStream(repositoryId, objectIdHolder, null, null);

        // refetch
        try {
            cs = objService.getContentStream(repositoryId, ob.getId(), null,
                    null, null, null);
            fail("Should have no content stream");
        } catch (CmisConstraintException e) {
            // ok
        }
    }

    // flatten and order children
    protected static List<String> flatTree(List<ObjectInFolderContainer> tree)
            throws Exception {
        if (tree == null) {
            return null;
        }
        List<String> r = new LinkedList<String>();
        for (Iterator<ObjectInFolderContainer> it = tree.iterator(); it.hasNext();) {
            ObjectInFolderContainer child = it.next();
            String name = getString(child.getObject().getObject(),
                    PropertyIds.NAME);
            String elem = name;
            List<String> sub = flatTree(child.getChildren());
            if (sub != null) {
                elem += "[" + StringUtils.join(sub, ", ") + "]";
            }
            r.add(elem);
        }
        Collections.sort(r);
        return r;
    }

    protected static String flat(List<ObjectInFolderContainer> tree)
            throws Exception {
        return StringUtils.join(flatTree(tree), ", ");
    }

    @Test
    public void testGetDescendants() throws Exception {
        List<ObjectInFolderContainer> tree;

        try {
            navService.getDescendants(repositoryId, rootFolderId,
                    BigInteger.valueOf(0), null, null, null, null, null, null);
            fail("Depth 0 should be forbidden");
        } catch (CmisInvalidArgumentException e) {
            // ok
        }

        tree = navService.getDescendants(repositoryId, rootFolderId,
                BigInteger.valueOf(1), null, null, null, null, null, null);
        assertEquals("testfolder1_Title, " //
                + "testfolder2_Title", flat(tree));

        tree = navService.getDescendants(repositoryId, rootFolderId,
                BigInteger.valueOf(2), null, null, null, null, null, null);
        assertEquals("testfolder1_Title[" //
                + /* */"testfile1_Title, " //
                + /* */"testfile2_Title, " //
                + /* */"testfile3_Title], " //
                + "testfolder2_Title[" //
                + /* */"testfolder3_Title, " //
                + /* */"testfolder4_Title]", //
                flat(tree));

        tree = navService.getDescendants(repositoryId, rootFolderId,
                BigInteger.valueOf(3), null, null, null, null, null, null);
        assertEquals("testfolder1_Title[" //
                + /* */"testfile1_Title[], " //
                + /* */"testfile2_Title[], " //
                + /* */"testfile3_Title[]], " //
                + "testfolder2_Title[" //
                + /* */"testfolder3_Title[testfile4_Title], " //
                + /* */"testfolder4_Title[]]", //
                flat(tree));

        tree = navService.getDescendants(repositoryId, rootFolderId,
                BigInteger.valueOf(4), null, null, null, null, null, null);
        assertEquals("testfolder1_Title[" //
                + /* */"testfile1_Title[], " //
                + /* */"testfile2_Title[], " //
                + /* */"testfile3_Title[]], " //
                + "testfolder2_Title[" //
                + /* */"testfolder3_Title[testfile4_Title[]], " //
                + /* */"testfolder4_Title[]]", //
                flat(tree));

        tree = navService.getDescendants(repositoryId, rootFolderId,
                BigInteger.valueOf(-1), null, null, null, null, null, null);
        assertEquals("testfolder1_Title[testfile1_Title[], "
                + /* */"testfile2_Title[], " //
                + /* */"testfile3_Title[]], " //
                + "testfolder2_Title[" //
                + /* */"testfolder3_Title[testfile4_Title[]], " //
                + /* */"testfolder4_Title[]]", //
                flat(tree));

        ObjectData ob = getObjectByPath("/testfolder2");
        String folder2Id = ob.getId();

        tree = navService.getDescendants(repositoryId, folder2Id,
                BigInteger.valueOf(1), null, null, null, null, null, null);
        assertEquals("testfolder3_Title, testfolder4_Title", flat(tree));

        tree = navService.getDescendants(repositoryId, folder2Id,
                BigInteger.valueOf(2), null, null, null, null, null, null);
        assertEquals("testfolder3_Title[testfile4_Title], testfolder4_Title[]",
                flat(tree));

        tree = navService.getDescendants(repositoryId, folder2Id,
                BigInteger.valueOf(3), null, null, null, null, null, null);
        assertEquals(
                "testfolder3_Title[testfile4_Title[]], testfolder4_Title[]",
                flat(tree));

        tree = navService.getDescendants(repositoryId, folder2Id,
                BigInteger.valueOf(-1), null, null, null, null, null, null);
        assertEquals(
                "testfolder3_Title[testfile4_Title[]], testfolder4_Title[]",
                flat(tree));
    }

    @Test
    public void testCreateDocumentFromSource() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1/testfile1");
        String key = "dc:title";
        String value = "new title";
        Properties props = createProperties(key, value);
        String id = objService.createDocumentFromSource(repositoryId,
                ob.getId(), props, rootFolderId, null, null, null, null, null);
        assertNotNull(id);
        assertNotSame(id, ob.getId());
        // fetch
        ObjectData copy = getObjectByPath("/testfile1");
        assertNotNull(copy);
        assertEquals(value, getString(copy, key));
    }

    @Test
    public void testDeleteObject() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1/testfile1");
        objService.deleteObject(repositoryId, ob.getId(), Boolean.TRUE, null);
        try {
            ob = getObjectByPath("/testfolder1/testfile1");
            fail("Document should be deleted");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }

        ob = getObjectByPath("/testfolder2");
        try {
            objService.deleteObject(repositoryId, ob.getId(), Boolean.TRUE,
                    null);
            fail("Should not be able to delete non-empty folder");
        } catch (CmisConstraintException e) {
            // ok to fail, still has children
        }
        ob = getObjectByPath("/testfolder2");
        assertNotNull(ob);

        try {
            objService.deleteObject(repositoryId, "nosuchid", Boolean.TRUE,
                    null);
            fail("Should not be able to delete nonexistent object");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testRemoveObjectFromFolder1() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1/testfile1");
        filingService.removeObjectFromFolder(repositoryId, ob.getId(), null,
                null);
        try {
            ob = getObjectByPath("/testfolder1/testfile1");
            fail("Document should be deleted");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testRemoveObjectFromFolder2() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1/testfile1");
        ObjectData folder = getObjectByPath("/testfolder1");
        filingService.removeObjectFromFolder(repositoryId, ob.getId(),
                folder.getId(), null);
        try {
            ob = getObjectByPath("/testfolder1/testfile1");
            fail("Document should be deleted");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testDeleteTree() throws Exception {
        ObjectData ob = getObjectByPath("/testfolder1");
        objService.deleteTree(repositoryId, ob.getId(), null, null, null, null);
        try {
            getObjectByPath("/testfolder1");
            fail("Folder should be deleted");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
        try {
            getObjectByPath("/testfolder1/testfile1");
            fail("Folder should be deleted");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
        assertNotNull(getObjectByPath("/testfolder2"));
    }

    @Test
    public void testMoveObject() throws Exception {
        ObjectData fold = getObjectByPath("/testfolder1");
        ObjectData ob = getObjectByPath("/testfolder2/testfolder3/testfile4");
        Holder<String> objectIdHolder = new Holder<String>(ob.getId());
        objService.moveObject(repositoryId, objectIdHolder, fold.getId(), null,
                null);
        assertEquals(ob.getId(), objectIdHolder.getValue());
        try {
            getObjectByPath("/testfolder2/testfolder3/testfile4");
            fail("Object should be moved away");
        } catch (CmisObjectNotFoundException e) {
            // ok
        }
        ObjectData ob2 = getObjectByPath("/testfolder1/testfile4");
        assertEquals(ob.getId(), ob2.getId());
    }

    @Test
    public void testQueryBasic() throws Exception {
        String statement;
        ObjectList res;

        statement = "SELECT cmis:objectId, cmis:name" //
                + " FROM File"; // no WHERE clause
        res = query(statement);
        assertEquals(3, res.getNumItems().intValue());

        statement = "SELECT cmis:objectId, cmis:name" //
                + " FROM File" //
                + " WHERE cmis:name <> 'testfile1_Title'";
        res = query(statement);
        assertEquals(2, res.getNumItems().intValue());

        // spec says names are case-insensitive
        // statement = "SELECT CMIS:OBJECTid, DC:DESCRIPTion" //
        // + " FROM FILE" //
        // + " WHERE DC:TItle = 'testfile1_Title'";
        // res = query(statement);
        // assertEquals(1, res.getNumItems().intValue());

        // STAR
        statement = "SELECT * FROM cmis:document";
        res = query(statement);
        assertEquals(4, res.getNumItems().intValue());
        statement = "SELECT * FROM cmis:folder";
        res = query(statement);
        assertEquals(4, res.getNumItems().intValue());

        statement = "SELECT cmis:objectId, dc:description" //
                + " FROM File" //
                + " WHERE dc:title = 'testfile1_Title'";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());

        statement = "SELECT cmis:objectId, dc:description" //
                + " FROM File" //
                + " WHERE dc:title = 'testfile1_Title'"
                + " AND dc:description <> 'argh'"
                + " AND dc:coverage <> 'zzzzz'";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());

        // IN
        statement = "SELECT cmis:objectId" //
                + " FROM File" //
                + " WHERE dc:title IN ('testfile1_Title', 'xyz')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
    }

    protected String NOT_NULL = new String("__NOTNULL__");

    protected void checkWhereTerm(String type, String prop, String value) {
        if (value == NOT_NULL) {
            checkQueriedValue(type, prop + " IS NOT NULL");
        } else {
            checkQueriedValue(type, prop + " = " + value);
        }
    }

    @SuppressWarnings("boxing")
    protected void checkQueriedValue(String type, String term) {
        String statement = String.format(
                "SELECT cmis:objectId FROM %s WHERE %s", type, term);
        ObjectList res = query(statement);
        assertNotSame(0, res.getNumItems().intValue());
    }

    @Test
    public void testQueryWhereProperties() throws Exception {
        String statement;
        ObjectList res;

        createDocumentMyDocType();

        // STAR
        statement = "SELECT * FROM MyDocType";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());

        checkQueriedValue("MyDocType", "my:string = 'abc'");
        checkQueriedValue("MyDocType", "my:string <> 'def'");
        checkQueriedValue("MyDocType", "my:boolean = true");
        checkQueriedValue("MyDocType", "my:boolean <> FALSE");
        checkQueriedValue("MyDocType", "my:integer = 123");
        checkQueriedValue("MyDocType", "my:integer <> 456");
        checkQueriedValue("MyDocType", "my:double = 123.456");
        checkQueriedValue("MyDocType", "my:double <> 123");
        checkQueriedValue("MyDocType",
                "my:date = TIMESTAMP '2010-09-30T16:04:55-02:00'");
        checkQueriedValue("MyDocType",
                "my:date <> TIMESTAMP '1999-09-09T01:01:01Z'");
        try {
            statement = "SELECT cmis:objectId FROM MyDocType WHERE my:date <> TIMESTAMP 'foobar'";
            query(statement);
            fail("Should be invalid Timestamp");
        } catch (CmisRuntimeException e) {
            // ok
        }
    }

    @Test
    public void testQueryWhereSystemProperties() throws Exception {

        // ----- Object -----

        checkWhereTerm("File", PropertyIds.NAME, "'testfile1_Title'");
        checkWhereTerm("File", PropertyIds.OBJECT_ID, NOT_NULL);
        checkWhereTerm("File", PropertyIds.OBJECT_TYPE_ID, "'File'");
        // checkWhereTerm("File", PropertyIds.BASE_TYPE_ID,
        // "'cmis:document'");
        checkWhereTerm("File", PropertyIds.CREATED_BY, "'michael'");
        checkWhereTerm("File", PropertyIds.CREATION_DATE, NOT_NULL);
        // checkWhereTerm("File", PropertyIds.LAST_MODIFIED_BY, "'bob'");
        checkWhereTerm("File", PropertyIds.LAST_MODIFICATION_DATE, NOT_NULL);
        // checkWhereTerm("File", PropertyIds.CHANGE_TOKEN, null);

        // ----- Folder -----

        checkWhereTerm("Folder", PropertyIds.PARENT_ID, NOT_NULL);
        // checkWhereTerm("Folder", PropertyIds.PATH, NOT_NULL);
        // checkWhereTerm("Folder", PropertyIds.ALLOWED_CHILD_OBJECT_TYPE_IDS,
        // NOT_NULL);

        // ----- Document -----

        // checkWhereTerm("File", PropertyIds.IS_IMMUTABLE, "FALSE");
        // checkWhereTerm("File", PropertyIds.IS_LATEST_VERSION, "TRUE");
        // checkWhereTerm("File", PropertyIds.IS_MAJOR_VERSION, "TRUE");
        // checkWhereTerm("File", PropertyIds.IS_LATEST_MAJOR_VERSION, "FALSE");
        // checkWhereTerm("File", PropertyIds.VERSION_LABEL, NOT_NULL);
        // checkWhereTerm("File", PropertyIds.VERSION_SERIES_ID, NOT_NULL);
        // checkWhereTerm("File", PropertyIds.VERSION_SERIES_CHECKED_OUT_BY,
        // NOT_NULL);
        // checkWhereTerm("File", PropertyIds.VERSION_SERIES_CHECKED_OUT_ID,
        // NOT_NULL);
        // checkWhereTerm("File", PropertyIds.CHECKIN_COMMENT, "xyz");
        // checkWhereTerm("File", PropertyIds.CONTENT_STREAM_LENGTH, NOT_NULL);
        // checkWhereTerm("File", PropertyIds.CONTENT_STREAM_MIME_TYPE,
        // "text/plain");
        // checkWhereTerm("File", PropertyIds.CONTENT_STREAM_FILE_NAME,
        // "testfile.txt");
        // checkWhereTerm("File", PropertyIds.CONTENT_STREAM_ID, NOT_NULL);
    }

    protected void checkReturnedValue(String prop, Object expected) {
        checkReturnedValue(prop, expected, "File", "testfile1_Title");
    }

    protected void checkReturnedValue(String prop, Object expected,
            String type, String name) {
        String statement = String.format(
                "SELECT %s FROM %s WHERE cmis:name = '%s'", prop, type, name);
        ObjectList res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        ObjectData data = res.getObjects().get(0);
        Object value = expected instanceof List ? getValues(data, prop)
                : getValue(data, prop);
        if (expected == NOT_NULL) {
            assertNotNull(value);
        } else {
            assertEquals(expected, value);
        }
    }

    @Test
    public void testQueryReturnedProperties() throws Exception {
        checkReturnedValue("dc:title", "testfile1_Title");
        checkReturnedValue("dc:modified", NOT_NULL);
        // multi-valued
        checkReturnedValue("dc:subjects", Arrays.asList("foo", "gee/moo"));
        checkReturnedValue("dc:contributors", Arrays.asList("bob", "pete"),
                "File", "testfile2_Title");
    }

    @Test
    public void testQueryReturnedSystemProperties() throws Exception {

        // ----- Object -----

        checkReturnedValue(PropertyIds.NAME, "testfile1_Title");
        checkReturnedValue(PropertyIds.OBJECT_ID, NOT_NULL);
        checkReturnedValue(PropertyIds.OBJECT_TYPE_ID, "File");
        checkReturnedValue(PropertyIds.BASE_TYPE_ID, "cmis:document");
        checkReturnedValue(PropertyIds.CREATED_BY, "michael");
        checkReturnedValue(PropertyIds.CREATION_DATE, NOT_NULL);
        checkReturnedValue(PropertyIds.LAST_MODIFIED_BY, "bob", "File",
                "testfile2_Title");
        checkReturnedValue(PropertyIds.LAST_MODIFICATION_DATE, NOT_NULL);
        checkReturnedValue(PropertyIds.CHANGE_TOKEN, null);

        // ----- Folder -----

        checkReturnedValue(PropertyIds.PARENT_ID, rootFolderId, "Folder",
                "testfolder1_Title");
        checkReturnedValue(PropertyIds.PATH, "/testfolder1", "Folder",
                "testfolder1_Title");
        checkReturnedValue(PropertyIds.ALLOWED_CHILD_OBJECT_TYPE_IDS, null,
                "Folder", "testfolder1_Title");

        // ----- Document -----

        checkReturnedValue(PropertyIds.IS_IMMUTABLE, Boolean.FALSE);
        checkReturnedValue(PropertyIds.IS_LATEST_VERSION, Boolean.TRUE); // TODO
        checkReturnedValue(PropertyIds.IS_MAJOR_VERSION, Boolean.FALSE); // TODO
        checkReturnedValue(PropertyIds.IS_LATEST_MAJOR_VERSION, Boolean.FALSE); // TODO
        checkReturnedValue(PropertyIds.VERSION_LABEL, null);
        checkReturnedValue(PropertyIds.VERSION_SERIES_ID, NOT_NULL);
        checkReturnedValue(PropertyIds.VERSION_SERIES_CHECKED_OUT_BY, null);
        checkReturnedValue(PropertyIds.VERSION_SERIES_CHECKED_OUT_ID, null);
        checkReturnedValue(PropertyIds.CHECKIN_COMMENT, null);
        checkReturnedValue(
                PropertyIds.CONTENT_STREAM_LENGTH,
                new ContentStreamImpl(null, "text/plain", Helper.FILE1_CONTENT).getBigLength());
        checkReturnedValue(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain");
        checkReturnedValue(PropertyIds.CONTENT_STREAM_FILE_NAME, "testfile.txt");
        checkReturnedValue(PropertyIds.CONTENT_STREAM_ID, null);
    }

    @Test
    public void testQueryAny() throws Exception {
        String statement;
        ObjectList res;

        // ... = ANY ...
        statement = "SELECT cmis:name FROM File WHERE 'pete' = ANY dc:contributors";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        statement = "SELECT cmis:name FROM File WHERE 'bob' = ANY dc:contributors";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());

        // ANY ... IN ...
        statement = "SELECT cmis:name FROM File WHERE ANY dc:contributors IN ('pete')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        statement = "SELECT cmis:name FROM File WHERE ANY dc:contributors IN ('pete', 'bob')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());

        // ANY ... NOT IN ...
        statement = "SELECT cmis:name FROM File WHERE ANY dc:contributors NOT IN ('pete')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        statement = "SELECT cmis:name FROM File WHERE ANY dc:contributors NOT IN ('john')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        statement = "SELECT cmis:name FROM File WHERE ANY dc:contributors NOT IN ('pete', 'bob')";
        res = query(statement);
        assertEquals(0, res.getNumItems().intValue());
    }

    @Test
    public void testQueryOrderBy() throws Exception {
        String statement;
        ObjectList res;
        ObjectData data;

        statement = "SELECT cmis:objectId, cmis:name" //
                + " FROM File" //
                + " ORDER BY cmis:name";
        res = query(statement);
        assertEquals(3, res.getNumItems().intValue());
        data = res.getObjects().get(0);
        assertEquals("testfile1_Title", getString(data, PropertyIds.NAME));

        // now change order
        res = query(statement + " DESC");
        assertEquals(3, res.getNumItems().intValue());
        data = res.getObjects().get(0);
        assertEquals("testfile4_Title", getString(data, PropertyIds.NAME));
    }

    @Test
    public void testQueryInFolder() throws Exception {
        ObjectData f1 = getObjectByPath("/testfolder1");
        String statementPattern = "SELECT cmis:name FROM File" //
                + " WHERE IN_FOLDER('%s')" //
                + " ORDER BY cmis:name";
        String statement = String.format(statementPattern, f1.getId());
        ObjectList res = query(statement);
        assertEquals(2, res.getNumItems().intValue());
        assertEquals("testfile1_Title",
                getString(res.getObjects().get(0), PropertyIds.NAME));
        assertEquals("testfile2_Title",
                getString(res.getObjects().get(1), PropertyIds.NAME));

        // missing/illegal ID
        statement = String.format(statementPattern, "nosuchid");
        res = query(statement);
        assertEquals(0, res.getNumItems().intValue());
    }

    @Test
    public void testQueryInTree() throws Exception {
        ObjectList res;
        String statement;

        ObjectData f2 = getObjectByPath("/testfolder2");
        String statementPattern = "SELECT cmis:name FROM File" //
                + " WHERE IN_TREE('%s')";

        statement = String.format(statementPattern, f2.getId());
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        assertEquals("testfile4_Title",
                getString(res.getObjects().get(0), PropertyIds.NAME));

        // missing/illegal ID
        statement = String.format(statementPattern, "nosuchid");
        res = query(statement);
        assertEquals(0, res.getNumItems().intValue());
    }

    @Test
    public void testQueryContains() throws Exception {
        ObjectList res;
        String statement;

        statement = "SELECT cmis:name FROM File" //
                + " WHERE CONTAINS('testfile1_Title')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        assertEquals("testfile1_Title",
                getString(res.getObjects().get(0), PropertyIds.NAME));
    }

    @Test
    public void testQueryScore() throws Exception {
        ObjectList res;
        String statement;
        ObjectData data;

        statement = "SELECT cmis:name, SCORE() FROM File" //
                + " WHERE CONTAINS('testfile1_Title')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        data = res.getObjects().get(0);
        assertEquals("testfile1_Title", getString(data, PropertyIds.NAME));
        assertNotNull(getValue(data, "SEARCH_SCORE")); // name from spec

        // using an alias for the score
        statement = "SELECT cmis:name, SCORE() AS priority FROM File" //
                + " WHERE CONTAINS('testfile1_Title')";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        data = res.getObjects().get(0);
        assertEquals("testfile1_Title", getString(data, PropertyIds.NAME));
        assertNotNull(getValue(data, "priority"));

        // ORDER BY score
        statement = "SELECT cmis:name, SCORE() importance FROM File" //
                + " WHERE CONTAINS('testfile1_Title')" //
                + " ORDER BY importance DESC";
        res = query(statement);
        assertEquals(1, res.getNumItems().intValue());
        data = res.getObjects().get(0);
        assertEquals("testfile1_Title", getString(data, PropertyIds.NAME));
        assertNotNull(getValue(data, "importance"));
    }

    @Test
    public void testQueryJoin() throws Exception {
        String statement;
        ObjectList res;
        ObjectData data;

        String folder2id = getObjectByPath("/testfolder2").getId();
        String folder3id = getObjectByPath("/testfolder2/testfolder3").getId();
        String folder4id = getObjectByPath("/testfolder2/testfolder4").getId();

        statement = "SELECT A.cmis:objectId, A.dc:title, B.cmis:objectId, B.dc:title" //
                + " FROM cmis:folder A" //
                + " JOIN cmis:folder B ON A.cmis:objectId = B.cmis:parentId" //
                + " WHERE A.cmis:name = 'testfolder2_Title'" //
                + " ORDER BY B.dc:title";
        res = query(statement);
        assertEquals(2, res.getNumItems().intValue());

        data = res.getObjects().get(0);
        assertEquals(folder2id, getQueryValue(data, "A.cmis:objectId"));
        assertEquals("testfolder2_Title", getQueryValue(data, "A.dc:title"));
        assertEquals(folder3id, getQueryValue(data, "B.cmis:objectId"));
        assertEquals("testfolder3_Title", getQueryValue(data, "B.dc:title"));

        data = res.getObjects().get(1);
        assertEquals(folder2id, getQueryValue(data, "A.cmis:objectId"));
        assertEquals("testfolder2_Title", getQueryValue(data, "A.dc:title"));
        assertEquals(folder4id, getQueryValue(data, "B.cmis:objectId"));
        assertEquals("testfolder4_Title", getQueryValue(data, "B.dc:title"));
    }

}
