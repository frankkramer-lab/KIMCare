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
package de.uni_a.misit.nifi.processors.kim_tx;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static de.uni_a.misit.nifi.processors.kim_tx.SMTPMailUtil.sendMailMessage;

@Tags({"KIM", "Telematik", "KoPS"})
@CapabilityDescription("Sends an incoming datasource via SMTP to a KIM endpoint")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class KIMTXProcessor extends AbstractProcessor {
    // Regex from https://stackoverflow.com/a/201378
    // private static final Pattern EMAIL_ADDRESS_REGEX_PATTERN = Pattern.compile("(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])");
    // -> Cannot be used because praxis.test@kim.telematik is a valid mail address in KIM.

    // Simplified pattern to match non-RFC conform addresses
    private static final Pattern EMAIL_ADDRESS_REGEX_PATTERN = Pattern.compile("^\\S+@(\\S+\\.)+\\S+$");
    public static final PropertyDescriptor KIMTX_MAIL_TO_LIST = new PropertyDescriptor
            .Builder().name("KIMTX_MAIL_TO_LIST")
            .displayName("Mail receiver address (list)")
            .description("List of mail addresses to receive the message")
            .required(true)
            .addValidator(
                    StandardValidators.createListValidator(true, true, StandardValidators.createRegexMatchingValidator(
                            EMAIL_ADDRESS_REGEX_PATTERN
                    ))
            )
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor KIMTX_MAIL_FROM = new PropertyDescriptor
            .Builder().name("KIMTX_MAIL_FROM")
            .displayName("Mail sender address")
            .description("Mail address that is used by the sender of the message")
            .required(true)
            .addValidator(
                    StandardValidators.createListValidator(true, true, StandardValidators.createRegexMatchingValidator(
                            EMAIL_ADDRESS_REGEX_PATTERN
                    ))
            )
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KIMTX_MAIL_SUBJECT = new PropertyDescriptor
            .Builder().name("KIMTX_MAIL_SUBJECT")
            .displayName("Mail subject text")
            .description("Mail subject text line for message, including the tag for simple identification, e.g. [STUDY_KIM1]")
            .required(true)
            .defaultValue("KIM TX [KIM_DEMO_TAG]")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor KIMTX_MAIL_TEXT = new PropertyDescriptor
            .Builder().name("KIMTX_MAIL_TEXT")
            .displayName("Mail message text")
            .description("Text in mail message")
            .required(false)
            .defaultValue("Dies ist eine automatisch generierte E-Mail.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KIMTX_SMTP_SERVER_HOST = new PropertyDescriptor
            .Builder().name("KIMTX_SMTP_SERVER_HOST")
            .displayName("SMTP server")
            .description("SMTP server address (SSL/TLS)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .build();
    public static final PropertyDescriptor KIMTX_SMTP_SERVER_PORT = new PropertyDescriptor
            .Builder().name("KIMTX_SMTP_SERVER_PORT")
            .displayName("SMTP server port")
            .description("SMTP server port (SSL/TLS)")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("10465")
            .build();
    public static final PropertyDescriptor KIMTX_SMTP_SERVER_AUTH_USERNAME = new PropertyDescriptor
            .Builder().name("KIMTX_SMTP_SERVER_AUTH_USERNAME")
            .displayName("SMTP username")
            .description("SMTP username to log in to the server, e.g. praxis.test@kim.telematik#mail.kim.telematik:10465#Mandant1#ClientID1#Workplace1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor KIMTX_SMTP_SERVER_AUTH_PASSWORD = new PropertyDescriptor
            .Builder().name("KIMTX_SMTP_SERVER_AUTH_PASSWORD")
            .displayName("SMTP password")
            .description("SMTP password to log in to the server with a username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .required(true)
            .build();
    public static final PropertyDescriptor KIMTX_SMTP_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT = new PropertyDescriptor
            .Builder().name("KIMTX_SMTP_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT")
            .displayName("Allow insecure TLS connections")
            .description("Activate this point if you use self-signed or invalid certificates on the SMTP server")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor KIMTX_MAIL_ATTACH_FORCE_FILENAME = new PropertyDescriptor
            .Builder().name("KIMTX_MAIL_ATTACH_FORCE_FILENAME")
            .displayName("Forced file name")
            .description("If provided, the provided flow-file filename attribute is ignored")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    // Relationships
    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship
            .Builder()
            .name("success")
            .description("The mail could be sent successfully.")
            .build();
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship
            .Builder()
            .name("failure")
            .description("Something went wrong during the mail assembly and transmission.")
            .build();
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(KIMTX_MAIL_TO_LIST);
        descriptors.add(KIMTX_MAIL_FROM);
        descriptors.add(KIMTX_MAIL_SUBJECT);
        descriptors.add(KIMTX_MAIL_TEXT);
        descriptors.add(KIMTX_SMTP_SERVER_HOST);
        descriptors.add(KIMTX_SMTP_SERVER_PORT);
        descriptors.add(KIMTX_SMTP_SERVER_AUTH_USERNAME);
        descriptors.add(KIMTX_SMTP_SERVER_AUTH_PASSWORD);
        descriptors.add(KIMTX_SMTP_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT);
        descriptors.add(KIMTX_MAIL_ATTACH_FORCE_FILENAME);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
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
        if ( flowFile == null ) {
            return;
        }

        // Obtain descriptor settings
        List<String> mail_to_list = List.of(context.getProperty(KIMTX_MAIL_TO_LIST).getValue().split(","));
        String mail_from = context.getProperty(KIMTX_MAIL_FROM).getValue();
        String mail_subject = context.getProperty(KIMTX_MAIL_SUBJECT).getValue();
        String mail_text = context.getProperty(KIMTX_MAIL_TEXT).getValue();
        String smtp_host = context.getProperty(KIMTX_SMTP_SERVER_HOST).getValue();
        int smtp_port = context.getProperty(KIMTX_SMTP_SERVER_PORT).asInteger();
        String auth_username = context.getProperty(KIMTX_SMTP_SERVER_AUTH_USERNAME).getValue();
        String auth_password = context.getProperty(KIMTX_SMTP_SERVER_AUTH_PASSWORD).getValue();
        boolean allow_insecure_tls = context.getProperty(KIMTX_SMTP_SERVER_AUTH_ALLOW_INSECURE_TLS_CONTEXT).asBoolean();
        String forced_attachment_filename = context.getProperty(KIMTX_MAIL_ATTACH_FORCE_FILENAME).getValue();


        // Obtain filename for attachment
        String filename = flowFile.getAttribute("filename");
        if (forced_attachment_filename != null && !forced_attachment_filename.isBlank())
            filename = forced_attachment_filename;

        // Attach file descriptor for content
        try {
            InputStream data_stream = session.read(flowFile);
            try {
                sendMailMessage(
                        // server smtp settings
                        auth_username, auth_password, smtp_host, smtp_port, allow_insecure_tls,
                        // message data
                        mail_from, mail_to_list, mail_subject, mail_text, filename, data_stream
                );
            } finally {
                data_stream.close();
            }

            // Mail could be sent.
            session.transfer(flowFile, RELATIONSHIP_SUCCESS);
        } catch (RuntimeException ex) {
            // Something went wrong with sending the mail...
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        } catch (IOException ex) {
            // Something went wrong with sending the mail...
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        }

        session.commit();
    }
}
