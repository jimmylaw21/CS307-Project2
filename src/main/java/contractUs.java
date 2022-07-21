import org.apache.commons.mail.*;
import io.javalin.Javalin;
import static j2html.TagCreator.*;

public class contractUs {

    public static void main(String[] args) {

        Javalin app = Javalin.create().start(7000);

        app.get("/", ctx -> ctx.html(
                form().withAction("/contact-us").withMethod("post").with(
                        input().withName("subject").withPlaceholder("Subject"),
                        br(),
                        textarea().withName("message").withPlaceholder("Your message ..."),
                        br(),
                        button("Submit")
                ).render()
        ));

        app.post("/contact-us", ctx -> {
            Email email = new SimpleEmail();
            email.setHostName("smtp.googlemail.com");
            email.setSmtpPort(465);
            email.setAuthenticator(new DefaultAuthenticator("YOUR_EMAIL", "YOUR_PASSWORD"));
            email.setSSLOnConnect(true);
            email.setFrom("YOUR_EMAIL");
            email.setSubject(ctx.formParam("subject")); // subject from HTML-form
            email.setMsg(ctx.formParam("message")); // message from HTML-form
            email.addTo("RECEIVING_ADDRESS");
            email.send(); // will throw email-exception if something is wrong
            ctx.redirect("/contact-us/success");
        });

        app.get("/contact-us/success", ctx -> ctx.html("Your message was sent"));

    }

}
