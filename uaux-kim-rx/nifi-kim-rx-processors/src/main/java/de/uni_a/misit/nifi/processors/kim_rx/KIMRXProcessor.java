/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.uni_a.misit.nifi.processors.kim_rx;

import jakarta.mail.*;
import jakarta.mail.internet.MimeBodyPart;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

@Tags({"KIM", "Telematik", "KoPS"})
@CapabilityDescription("Polls a POP3 server mail and forwards the latest tagged mail as a flow file")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class KIMRXProcessor extends AbstractProcessor {
    // Regex from https://stackoverflow.com/a/201378
    // private static final Pattern EMAIL_ADDRESS_REGEX_PATTERN = Pattern.compile("(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])");
    // -> Cannot be used because praxis.test@kim.telematik is a valid mail address in KIM.

    // Simplified pattern to match non-RFC conform addresses
    private static final Pattern EMAIL_ADDRESS_REGEX_PATTERN = Pattern.compile("^\\S+@(\\S+\\.)+\\S+$");
    public static final PropertyDescriptor KIMRX_MAIL_FROM = new PropertyDescriptor
            .Builder().name("KIMRX_MAIL_FROM")
            .displayName("Only mail sender address")
            .description("Only read mails from address of a certain sender")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(
                EMAIL_ADDRESS_REGEX_PATTERN
            ))
            .build();

    public static final PropertyDescriptor KIMRX_TAG = new PropertyDescriptor
            .Builder().name("KIMRX_MAIL_TAG")
            .displayName("Mail subject text tag")
            .description("Only mails with provided tag will be considered, e.g. [STUDY_KIM1]")
            .required(true)
            .defaultValue("KIM TX [KIM_DEMO_TAG]")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor KIMRX_DELETE_MAILS = new PropertyDescriptor
            .Builder().name("KIMRX_DELETE_MAILS")
            .displayName("Delete matching mails after processing")
            .description("Delete all matching mails on the POP3 server after the mails have been read")
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor KIMRX_POP3_SERVER_HOST = new PropertyDescriptor
            .Builder().name("KIMRX_POP3_SERVER_HOST")
            .displayName("POP3 server")
            .description("POP3 server address (SSL/TLS)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .build();

    public static final PropertyDescriptor KIMRX_POP3_SERVER_PORT = new PropertyDescriptor
            .Builder().name("KIMRX_POP3_SERVER_PORT")
            .displayName("POP3 server port")
            .description("POP3 server port (SSL/TLS)")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("10995")
            .build();
    public static final PropertyDescriptor KIMRX_POP3_SERVER_AUTH_USERNAME = new PropertyDescriptor
            .Builder().name("KIMRX_POP3_SERVER_AUTH_USERNAME")
            .displayName("POP3 username")
            .description("POP3 username to log in to the server, e.g. praxis.test@kim.telematik#mail.kim.telematik:10465#Mandant1#ClientID1#Workplace1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor KIMRX_POP3_SERVER_AUTH_PASSWORD = new PropertyDescriptor
            .Builder().name("KIMRX_POP3_SERVER_AUTH_PASSWORD")
            .displayName("POP3 password")
            .description("POP3 password to log in to the server with a username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(true)
            .build();
    public static final PropertyDescriptor KIMRX_POP3_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT = new PropertyDescriptor
            .Builder().name("KIMRX_POP3_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT")
            .displayName("Allow insecure TLS connections")
            .description("Activate this point if you use self-signed or invalid certificates on the POP3 server")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor KIMRX_MAIL_ATTACH_FORCE_FILENAME = new PropertyDescriptor
            .Builder().name("KIMRX_MAIL_ATTACH_FORCE_FILENAME")
            .displayName("Forced file name")
            .description("If provided, the provided filename is set as filename attribute and not taken from the mail attachment.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    // Relationships
    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship
            .Builder()
            .name("success")
            .description("The mail could be sent successfully.")
            .build();
    public static final Relationship RELATIONSHIP_NO_NEW_MAIL = new Relationship
            .Builder()
            .name("no-new-mail")
            .description("No new mail with updated files were found.")
            .build();
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship
            .Builder()
            .name("failure")
            .description("Something went wrong during mail acquisition and parsing.")
            .build();
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(KIMRX_MAIL_FROM);
        descriptors.add(KIMRX_TAG);
        descriptors.add(KIMRX_DELETE_MAILS);
        descriptors.add(KIMRX_POP3_SERVER_HOST);
        descriptors.add(KIMRX_POP3_SERVER_PORT);
        descriptors.add(KIMRX_POP3_SERVER_AUTH_USERNAME);
        descriptors.add(KIMRX_POP3_SERVER_AUTH_PASSWORD);
        descriptors.add(KIMRX_POP3_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT);
        descriptors.add(KIMRX_MAIL_ATTACH_FORCE_FILENAME);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
        relationships.add(RELATIONSHIP_NO_NEW_MAIL);
        relationships.add(RELATIONSHIP_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        // Obtain descriptor settings
        String mail_from = context.getProperty(KIMRX_MAIL_FROM).getValue();
        String mail_tag = context.getProperty(KIMRX_TAG).getValue();
        boolean delete_mails = context.getProperty(KIMRX_DELETE_MAILS).asBoolean();
        String forced_attachment_filename = context.getProperty(KIMRX_MAIL_ATTACH_FORCE_FILENAME).getValue();

        String pop3_host = context.getProperty(KIMRX_POP3_SERVER_HOST).getValue();
        int pop3_port = context.getProperty(KIMRX_POP3_SERVER_PORT).asInteger();
        String auth_username = context.getProperty(KIMRX_POP3_SERVER_AUTH_USERNAME).getValue();
        String auth_password = context.getProperty(KIMRX_POP3_SERVER_AUTH_PASSWORD).getValue();
        boolean allow_insecure_tls = context.getProperty(KIMRX_POP3_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT).asBoolean();

        // init tag pattern for match
        Pattern tag_pattern = Pattern.compile(Pattern.quote(mail_tag));

        Properties prop = new Properties();
        prop.setProperty("mail.pop3.host", pop3_host);
        prop.setProperty("mail.pop3.port", String.valueOf(pop3_port));
        prop.setProperty("mail.pop3.ssl.enable", "true");

        // set additional flag
        if (allow_insecure_tls) {
            prop.setProperty("mail.pop3.ssl.trust", "*");
        }

        Folder inbox = null;
        Store store = null;
        try {
            // connect to the pop3 inbox
            Session pop3_session = Session.getDefaultInstance(prop, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(auth_username, auth_password);
                }
            });

            // connect
            store = pop3_session.getStore("pop3");
            store.connect();

            // pop3 does not support folders, (only uses 'INBOX')
            inbox = store.getFolder("INBOX");

            // open inbox with read/write if we want to delete mails
            inbox.open(delete_mails ? Folder.READ_WRITE : Folder.READ_ONLY);

            // get the list of inbox messages
            Message[] messages = inbox.getMessages();

            // collect all matching mails
            LinkedList<Message> matches = new LinkedList<>();
            for (int i = 0; i < messages.length; i++) {
                Message msg = messages[i];

                if (mail_from != null && !mail_from.isBlank() && (msg.getFrom().length == 0 || !msg.getFrom()[0].toString().equals(mail_from))) {
                    // mail sender address does not match
                    continue;
                }

                if (!tag_pattern.matcher(msg.getSubject()).find()) {
                    // tag is not part of the subject line
                    continue;
                }

                if (POP3MessageUtil.findFileAttachmentFromMimeBodyPart(msg.getContent()) == null) {
                    // the message lacks an attachment
                    continue;
                }

                matches.add(msg);
            }

            if (matches.isEmpty()) {
                // we signal a no-new-mails event
                session.transfer(flowFile, RELATIONSHIP_NO_NEW_MAIL);
            } else {
                matches.sort((m1, m2) -> {
                    try {
                        return - (m1.getSentDate().compareTo(m2.getSentDate()));
                    } catch (MessagingException e) {
                        return 0;
                    }
                });

                for (int i = 0; i<matches.size(); i++) {
                    Message msg = matches.get(i);
                    if (i == 0) {
                        // we have the most recent flow file
                        MimeBodyPart attachment_part = POP3MessageUtil.findFileAttachmentFromMimeBodyPart(msg.getContent());
                        String filename = attachment_part.getFileName();
                        if (filename == null && forced_attachment_filename != null) {
                            filename = forced_attachment_filename;
                        } else if (filename == null && forced_attachment_filename == null) {
                            filename = "data.bin";
                        }

                        // write to flow file
                        session.write(flowFile, outputStream -> {
                            try {
                                InputStream is = attachment_part.getInputStream();
                                byte[] buf = new byte[8192];
                                int length;
                                while ((length = is.read(buf)) != -1) {
                                    outputStream.write(buf, 0, length);
                                }
                            } catch (MessagingException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        // set filename
                        session.putAttribute(flowFile, "filename", filename);
                    }
                    // remove mails if desired
                    if (delete_mails) {
                        msg.setFlag(Flags.Flag.DELETED, true);
                    }
                }
                session.transfer(flowFile, RELATIONSHIP_SUCCESS);
            }

            if (inbox.isOpen()) inbox.close();
            if (store.isConnected()) store.close();

        } catch (MessagingException | IOException e) {
            session.penalize(flowFile);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        } finally {
            // close open sessions
            try { if (inbox != null && inbox.isOpen()) { inbox.close(true);} } catch (MessagingException ignored) {}
            try { if (store != null && store.isConnected()) { store.close(); } } catch (MessagingException ignored) {}
        }

        session.commit();
    }
}
