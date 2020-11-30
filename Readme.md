# Daqbuffer

Tools for the retrieval (work in progress):

* Channel scanner
* Channel config scanner
* Proxy for multi-backend requests

[Build instructions](#build)

# Build

Tested on RHEL 7 and 8, CentOS 7 and 8.

If not yet done, see [Setup Toolchain](#setup-toolchain) further below first.

Then:

```bash
cd ./daqbuffer
cargo build --release
```

That's it. Binary is now at: `./target/release/daqbuffer`

# Setup Toolchain

Install Rust toolchain.
Quoting from <https://www.rust-lang.org/tools/install> the official installation method:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

This specifically requires a verified TLS connection and then executes the installer.

Installation will, by default, be only for your user. No superuser privileges required.

That's it.

You should have the commands `rustc`, `rustup` and `cargo` now available in your terminal.

# License

GNU General Public License version 3 or later.
