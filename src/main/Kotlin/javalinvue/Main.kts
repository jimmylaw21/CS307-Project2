import io.javalin.Javalin

fun main() {
    Javalin.create { config ->
        config.enableWebjars()
    }.start(7000);
}