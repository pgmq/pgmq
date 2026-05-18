pub fn main() {
    println!(
        "cargo:rerun-if-changed={}/../pgmq-extension/sql/",
        env!("CARGO_MANIFEST_DIR")
    );
    println!(
        "cargo:rerun-if-changed={}/../pgmq-extension/pgmq.control",
        env!("CARGO_MANIFEST_DIR")
    );
}
