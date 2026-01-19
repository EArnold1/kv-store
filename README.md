# kv_db

kv_db is a RoseDB-inspired log-structured key-value store written in Rust, focusing on durability and high-throughput writes via Write-Ahead Logging (WAL).

## Features

- Log-structured key-value storage
- Write-Ahead Logging (WAL) for durability
- High-throughput write operations
- Simple and extensible architecture

## Getting Started

### Prerequisites

- Rust (edition 2024 or later)

### Building

Clone the repository and build the project:

```sh
git clone https://github.com/EArnold1/kv-store.git
cd kv_store
cargo build --release
```

### Running

To run the key-value store:

```sh
cargo run --release
```

## Project Structure

- `src/main.rs` — Entry point for the application
- `src/lib.rs` — Library module
- `src/store.rs` — Core key-value store logic
- `src/wal.rs` — Write-Ahead Log implementation
- `src/record.rs` — Data record structures
- `src/helper.rs` — Utility functions
- `src/error.rs` — Error handling

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Author

Arnold Emmanuel (<arnoldemmanuel15@gmail.com>)

## Acknowledgements

- Inspired by [RoseDB](https://github.com/roseduan/rosedb)
