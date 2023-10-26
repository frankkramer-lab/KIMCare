package de.uni_a.misit.nifi.processors.kim_rx;

import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.internet.MimeBodyPart;
import java.io.IOException;

public class POP3MessageUtil {

    public static MimeBodyPart findFileAttachmentFromMimeBodyPart(Object content)  {
        // Adopted from https://www.rgagnon.com/javadetails/java-receive-email-using-pop3.html
        try {
            if (content instanceof Multipart) {
                Multipart multi = ((Multipart)content);
                int parts = multi.getCount();
                for (int i=0; i < parts; ++i) {
                    MimeBodyPart part = (MimeBodyPart)multi.getBodyPart(i);
                    if (part.getContent() instanceof Multipart) {
                        // extract subpart
                        MimeBodyPart result = findFileAttachmentFromMimeBodyPart(part.getContent());
                        if (result != null) return result;
                    }
                    else {
                        if (part.isMimeType("application/octet-stream")) {
                            // we found the right part
                            return part;
                        }
                    }
                }
            }
            return null;
        } catch (IOException | MessagingException e) {
            throw new RuntimeException(e);
        }
    }
}
