// SPDX-License-Identifier: Apache-2.0
package com.openexchange.core.infrastructure;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Money-schema evolution is append-only, which is what lets a member decode a frame at its header's version during a
 * rolling upgrade. Between the previous released schema (kept here as a test resource) and the current one: every
 * message keeps its id and name; every field and group keeps its position, id and type, so every offset stays put; new
 * root and group fields are appended after the old ones and carry {@code sinceVersion} equal to the new schema
 * version; no type is redefined; no enum or set value is removed or renumbered.
 *
 * <p>When the schema version moves, the baseline becomes the released schema of the version before it: copy the
 * released file into {@code src/test/resources/sbe/} and point {@link #BASELINE} at it. The copy is kept under a
 * {@code .baseline} name, not {@code .xml}: it carries schema id 2, and the schema id registry check counts every
 * {@code .xml} schema in the repo as a schema of its own.</p>
 */
public class MoneySchemaAppendOnlyTest {

    private static final Path CURRENT = Path.of("src", "main", "resources", "sbe", "money-schema.xml");
    private static final Path BASELINE = Path.of("src", "test", "resources", "sbe", "money-schema-v10.xml.baseline");

    // ---------------------------------------------------------------------------------------------- the rule

    /** The violations of the append-only rule from {@code previous} to {@code current}, empty when it holds. */
    static List<String> violations(final String previous, final String current) throws Exception {
        final Element p = parse(previous);
        final Element c = parse(current);
        final List<String> out = new ArrayList<>();
        final int pVersion = Integer.parseInt(p.getAttribute("version"));
        final int cVersion = Integer.parseInt(c.getAttribute("version"));
        if (!p.getAttribute("id").equals(c.getAttribute("id"))) {
            out.add("schema id " + c.getAttribute("id") + " differs from " + p.getAttribute("id"));
        }
        if (cVersion < pVersion) {
            out.add("schema version " + cVersion + " is below the previous " + pVersion);
        }
        // types: every previous type is unchanged
        final Map<String, Element> pTypes = types(p);
        final Map<String, Element> cTypes = types(c);
        for (final Map.Entry<String, Element> e : pTypes.entrySet()) {
            final Element now = cTypes.get(e.getKey());
            if (now == null) {
                out.add("type " + e.getKey() + " removed");
                continue;
            }
            if (!e.getValue().getTagName().equals(now.getTagName())
                    || !attrs(e.getValue(), "primitiveType", "length", "encodingType").equals(
                    attrs(now, "primitiveType", "length", "encodingType"))) {
                out.add("type " + e.getKey() + " redefined");
            }
            final Map<String, String> pValues = values(e.getValue());
            final Map<String, String> cValues = values(now);
            for (final Map.Entry<String, String> v : pValues.entrySet()) {
                final String value = cValues.get(v.getKey());
                if (value == null) {
                    out.add("value " + e.getKey() + "." + v.getKey() + " removed");
                } else if (!value.equals(v.getValue())) {
                    out.add("value " + e.getKey() + "." + v.getKey() + " renumbered from " + v.getValue() + " to " + value);
                }
            }
            if (!composite(e.getValue()).equals(composite(now))) {
                out.add("composite " + e.getKey() + " changed");
            }
        }
        // messages
        final Map<String, Element> pMessages = messages(p);
        final Map<String, Element> cMessages = messages(c);
        for (final Map.Entry<String, Element> e : pMessages.entrySet()) {
            final Element now = cMessages.get(e.getKey());
            if (now == null) {
                out.add("message id " + e.getKey() + " (" + e.getValue().getAttribute("name") + ") removed");
                continue;
            }
            if (!now.getAttribute("name").equals(e.getValue().getAttribute("name"))) {
                out.add("message id " + e.getKey() + " renamed from " + e.getValue().getAttribute("name") + " to "
                        + now.getAttribute("name"));
            }
            compareMembers(e.getValue().getAttribute("name"), e.getValue(), now, pVersion, cVersion, out);
        }
        // a new message in a new version carries that version
        for (final Map.Entry<String, Element> e : cMessages.entrySet()) {
            if (!pMessages.containsKey(e.getKey()) && cVersion > pVersion
                    && !String.valueOf(cVersion).equals(e.getValue().getAttribute("sinceVersion"))) {
                out.add("message " + e.getValue().getAttribute("name") + " added without sinceVersion=" + cVersion);
            }
        }
        return out;
    }

    /** Fields, then groups and data, of one message or group: the previous ones are a prefix of the current ones. */
    private static void compareMembers(final String where, final Element p, final Element c, final int pVersion,
                                       final int cVersion, final List<String> out) {
        final List<Element> pf = children(p, "field");
        final List<Element> cf = children(c, "field");
        for (int i = 0; i < pf.size(); i++) {
            final Element old = pf.get(i);
            final String name = old.getAttribute("name");
            if (i >= cf.size()) {
                out.add("field " + where + "." + name + " removed");
                continue;
            }
            final Element now = cf.get(i);
            if (!now.getAttribute("name").equals(name)) {
                out.add("field " + where + "." + name + " moved (position " + i + " is now " + now.getAttribute("name") + ")");
            } else if (!attrs(old, "id", "type", "offset", "sinceVersion", "presence").equals(
                    attrs(now, "id", "type", "offset", "sinceVersion", "presence"))) {
                out.add("field " + where + "." + name + " changed id, type, offset or sinceVersion");
            }
        }
        for (int i = pf.size(); i < cf.size(); i++) {
            final Element added = cf.get(i);
            if (cVersion == pVersion) {
                out.add("field " + where + "." + added.getAttribute("name") + " added without a version bump");
            } else if (!String.valueOf(cVersion).equals(added.getAttribute("sinceVersion"))) {
                out.add("field " + where + "." + added.getAttribute("name") + " added without sinceVersion=" + cVersion);
            }
        }
        final List<Element> pg = children(p, "group");
        final List<Element> cg = children(c, "group");
        for (int i = 0; i < pg.size(); i++) {
            final Element old = pg.get(i);
            if (i >= cg.size() || !cg.get(i).getAttribute("name").equals(old.getAttribute("name"))
                    || !cg.get(i).getAttribute("id").equals(old.getAttribute("id"))) {
                out.add("group " + where + "." + old.getAttribute("name") + " removed or moved");
                continue;
            }
            compareMembers(where + "." + old.getAttribute("name"), old, cg.get(i), pVersion, cVersion, out);
        }
        for (int i = pg.size(); i < cg.size(); i++) {
            final Element added = cg.get(i);
            if (cVersion == pVersion || !String.valueOf(cVersion).equals(added.getAttribute("sinceVersion"))) {
                out.add("group " + where + "." + added.getAttribute("name") + " added without sinceVersion=" + cVersion);
            }
        }
        if (children(p, "data").size() > children(c, "data").size()) {
            out.add("data of " + where + " removed");
        }
    }

    // ---------------------------------------------------------------------------------------------- XML helpers

    private static Element parse(final String xml) throws Exception {
        final DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setNamespaceAware(true);
        f.setExpandEntityReferences(false);
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        try (InputStream in = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))) {
            final Document d = f.newDocumentBuilder().parse(in);
            return d.getDocumentElement();
        }
    }

    private static List<Element> children(final Element parent, final String localName) {
        final List<Element> out = new ArrayList<>();
        final NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            final Node n = nodes.item(i);
            if (n instanceof Element e && localName.equals(e.getLocalName() == null ? e.getTagName() : e.getLocalName())) {
                out.add(e);
            }
        }
        return out;
    }

    private static Map<String, Element> types(final Element schema) {
        final Map<String, Element> out = new LinkedHashMap<>();
        for (final Element types : children(schema, "types")) {
            final NodeList nodes = types.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                if (nodes.item(i) instanceof Element e) {
                    out.put(e.getAttribute("name"), e);
                }
            }
        }
        return out;
    }

    private static Map<String, Element> messages(final Element schema) {
        final Map<String, Element> out = new LinkedHashMap<>();
        for (final Element m : children(schema, "message")) {
            out.put(m.getAttribute("id"), m);
        }
        return out;
    }

    private static Map<String, String> values(final Element type) {
        final Map<String, String> out = new LinkedHashMap<>();
        for (final String kind : new String[] {"validValue", "choice"}) {
            for (final Element v : children(type, kind)) {
                out.put(v.getAttribute("name"), v.getTextContent().trim());
            }
        }
        return out;
    }

    private static List<String> composite(final Element type) {
        final List<String> out = new ArrayList<>();
        for (final Element t : children(type, "type")) {
            out.add(t.getAttribute("name") + ":" + attrs(t, "primitiveType", "length", "offset"));
        }
        return out;
    }

    private static List<String> attrs(final Element e, final String... names) {
        final List<String> out = new ArrayList<>();
        for (final String n : names) {
            out.add(e.getAttribute(n));
        }
        return out;
    }

    private static String read(final Path p) throws IOException {
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private static String once(final String s, final String from, final String to) {
        assertEquals("the synthetic edit applies once: " + from, 1, s.split(java.util.regex.Pattern.quote(from), -1).length - 1);
        return s.replace(from, to);
    }

    // ---------------------------------------------------------------------------------------------- tests

    @Test
    public void theCurrentSchemaIsAnAppendOnlyEvolutionOfTheBaseline() throws Exception {
        assertEquals(List.of(), violations(read(BASELINE), read(CURRENT)));
    }

    @Test
    public void theBaselineIsTheCurrentVersionUntilTheNextBump() throws Exception {
        assertEquals("10", parse(read(BASELINE)).getAttribute("version"));
    }

    /** {@code base} with the Deposit message block passed through {@code edit}. */
    private static String inDeposit(final String base, final java.util.function.UnaryOperator<String> edit) {
        final int start = base.indexOf("<sbe:message name=\"Deposit\"");
        final int end = base.indexOf("</sbe:message>", start);
        return base.substring(0, start) + edit.apply(base.substring(start, end)) + base.substring(end);
    }

    @Test
    public void aMovedFieldIsNamed() throws Exception {
        final String base = read(BASELINE);
        final String userId = "<field name=\"userId\"        id=\"2\" type=\"int64\"/>";
        final String assetId = "<field name=\"assetId\"       id=\"3\" type=\"int32\"/>";
        final String moved = inDeposit(base, m -> once(once(m, userId, "@@"), assetId, userId).replace("@@", assetId));
        final List<String> v = violations(base, moved);
        assertTrue(v.toString(), v.contains("field Deposit.userId moved (position 1 is now assetId)"));
    }

    @Test
    public void aRemovedMessageIsNamed() throws Exception {
        final String base = read(BASELINE);
        final int start = base.indexOf("<sbe:message name=\"Deposit\"");
        final int end = base.indexOf("</sbe:message>", start) + "</sbe:message>".length();
        final List<String> v = violations(base, base.substring(0, start) + base.substring(end));
        assertTrue(v.toString(), v.contains("message id 1 (Deposit) removed"));
    }

    @Test
    public void aNewFieldWithoutSinceVersionIsNamed() throws Exception {
        final String base = read(BASELINE);
        final String bumped = once(base, "version=\"10\"", "version=\"11\"");
        final String next = inDeposit(bumped, m -> m + "    <field name=\"memo\" id=\"7\" type=\"int64\"/>\n    ");
        final List<String> v = violations(base, next);
        assertTrue(v.toString(), v.contains("field Deposit.memo added without sinceVersion=11"));
        // the same field with its sinceVersion is the append-only shape
        final String good = inDeposit(bumped,
                m -> m + "    <field name=\"memo\" id=\"7\" type=\"int64\" sinceVersion=\"11\"/>\n    ");
        assertEquals(List.of(), violations(base, good));
    }

    @Test
    public void aRemovedEnumValueIsNamed() throws Exception {
        final String base = read(BASELINE);
        final String removed = once(base, "<validValue name=\"REQUEST_ID_MISMATCH\">5</validValue>", "");
        final List<String> v = violations(base, removed);
        assertTrue(v.toString(), v.contains("value RejectReason.REQUEST_ID_MISMATCH removed"));
        final String renumbered = once(base, "<validValue name=\"REQUEST_ID_MISMATCH\">5</validValue>",
                "<validValue name=\"REQUEST_ID_MISMATCH\">6</validValue>");
        assertTrue(violations(base, renumbered).contains("value RejectReason.REQUEST_ID_MISMATCH renumbered from 5 to 6"));
    }
}
