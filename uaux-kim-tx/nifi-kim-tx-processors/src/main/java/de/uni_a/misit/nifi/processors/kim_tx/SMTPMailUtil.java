package de.uni_a.misit.nifi.processors.kim_tx;

import jakarta.activation.DataHandler;
import jakarta.mail.*;
import jakarta.mail.internet.*;
import jakarta.mail.util.ByteArrayDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class SMTPMailUtil {
    public static void sendMailMessage(
            String auth_username, String auth_password, String server_host, int server_port, boolean allow_insecure_tls,
            String from, List<String> tos, String subject, String text, String attachment_filename, InputStream attachment_content) {
        try {
            // create smtp session first
            Properties prop = new Properties();
            // setup smtp settings
            prop.setProperty("mail.smtp.auth", "true");
            prop.setProperty("mail.smtp.ssl.enable", "true");
            prop.setProperty("mail.smtp.host", server_host);
            prop.setProperty("mail.smtp.port", String.valueOf(server_port));

            // set additional flag
            if (allow_insecure_tls) {
                prop.setProperty("mail.smtp.ssl.trust", "*");
            }

            // create session with username / password
            Session session = Session.getInstance(prop, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(auth_username, auth_password);
                }
            });

            // build mail message
            MimeMessage msg = new MimeMessage(session);

            // parse receiver addresses
            List<InternetAddress> to_addresses = new LinkedList<InternetAddress>();
            for (String to: tos) {
                try {
                    to_addresses.add(new InternetAddress(to));
                } catch (AddressException ex) { }
            }
            InternetAddress[] addresses = to_addresses.toArray(new InternetAddress[0]);

            // set mail properties
            msg.setFrom(new InternetAddress(from));
            msg.setRecipients(Message.RecipientType.TO, addresses);
            msg.setSubject(subject);
            msg.setSentDate(new Date());

            // encode text line
            MimeBodyPart message = new MimeBodyPart();
            message.setText(text);

            // add attachment
            MimeBodyPart attachment = new MimeBodyPart();
            attachment.setDataHandler(new DataHandler(new ByteArrayDataSource(attachment_content, "application/octet-stream")));
            attachment.setFileName(MimeUtility.encodeText(attachment_filename));

            // assemble bodyparts
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(message);
            multipart.addBodyPart(attachment);
            msg.setContent(multipart);

            // finally send the message
            Transport.send(msg);
        } catch (AddressException e) {
            throw new RuntimeException(e);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
