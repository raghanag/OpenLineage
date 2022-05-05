ARG MANYLINUX_IMAGE=quay.io/pypa/manylinux2014_x86_64
FROM $MANYLINUX_IMAGE as manylinux
# Create virtual env for Docker
RUN /opt/python/cp37-cp37m/bin/python -m venv .env
# Maturin is build tool that we're using. It can build python wheels based on standard Rust Cargo.toml.
RUN . .env/bin/activate && python -m pip install maturin
# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
# Disable incremental compilation, since it causes issues.
ENV CARGO_INCREMENTAL=0
RUN . $HOME/.cargo/env
