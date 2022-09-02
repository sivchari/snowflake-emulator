use server::server;

fn main() -> std::io::Result<()> {
    server::run("localhost", 8000)
}
